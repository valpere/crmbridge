package service_test

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/valpere/crmbridge/internal/binotel"
	"github.com/valpere/crmbridge/internal/fakenp"
	"github.com/valpere/crmbridge/internal/fakesalesdrive"
	"github.com/valpere/crmbridge/internal/novaposhta"
	"github.com/valpere/crmbridge/internal/salesdrive"
	"github.com/valpere/crmbridge/internal/service"
	"github.com/valpere/crmbridge/internal/store"
)

type clock struct {
	mu sync.Mutex
	t  time.Time
}

func (c *clock) Now() time.Time { c.mu.Lock(); defer c.mu.Unlock(); return c.t }
func (c *clock) Add(d time.Duration) {
	c.mu.Lock()
	c.t = c.t.Add(d)
	c.mu.Unlock()
}

type msg struct{ To, Text string }
type sink struct {
	mu  sync.Mutex
	got []msg
}

func (s *sink) Send(_ context.Context, to, text string) error {
	s.mu.Lock()
	s.got = append(s.got, msg{to, text})
	s.mu.Unlock()
	return nil
}
func (s *sink) all() []msg { s.mu.Lock(); defer s.mu.Unlock(); return append([]msg(nil), s.got...) }

type env struct {
	t   *testing.T
	svc *service.Service
	sd  *fakesalesdrive.Server
	np  *fakenp.Server
	clk *clock
	out *sink
	ctx context.Context
	st  *store.Store
}

func newEnv(t *testing.T, cfg service.Config, apiKey string) *env {
	t.Helper()
	clk := &clock{t: time.Date(2026, 9, 21, 12, 0, 0, 0, time.UTC)}
	sd := fakesalesdrive.New("key")
	sts := httptest.NewServer(sd.Handler())
	t.Cleanup(sts.Close)
	np := fakenp.New()
	nts := httptest.NewServer(np.Handler())
	t.Cleanup(nts.Close)
	if apiKey == "" {
		apiKey = "key"
	}
	st, err := store.Open(":memory:")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { st.Close() })
	out := &sink{}
	if cfg.StatusMap == nil {
		cfg.StatusMap = map[int]int{7: 21, 9: 22, 102: 23}
	}
	cfg.DefaultManager = "telegram:1"
	cfg.Managers = map[string]string{"101": "telegram:101", "102": "telegram:102"}
	svc := service.New(st, salesdrive.New(salesdrive.Config{BaseURL: sts.URL, APIKey: apiKey}),
		novaposhta.New(novaposhta.Config{BaseURL: nts.URL}), out, cfg, slog.New(slog.NewTextHandler(io.Discard, nil)))
	svc.SetClock(clk.Now)
	return &env{t: t, svc: svc, sd: sd, np: np, clk: clk, out: out, ctx: context.Background(), st: st}
}

func (e *env) drain() {
	e.t.Helper()
	for i := 0; i < 40; i++ {
		n, err := e.svc.ProcessOnce(e.ctx)
		if err != nil {
			e.t.Fatal(err)
		}
		e.clk.Add(10 * time.Second)
		if n == 0 {
			return
		}
	}
	e.t.Fatal("outbox did not settle")
}

func order(id string) service.SiteOrder {
	return service.SiteOrder{ID: id, Source: "site",
		Customer: service.Customer{FirstName: "Іра", LastName: "Коваль", Phone: "050 123 45 67", Telegram: "42"},
		Items:    []service.Item{{SKU: "COF", Name: "Кава 1 кг", Price: 12500, Qty: 2}},
		City:     "Київ", Warehouse: "12", PaymentMethod: "Оплата при отриманні"}
}

func call(rt, id, phone, disp string) binotel.Event {
	return binotel.Event{RequestType: rt, GeneralCallID: id, Incoming: true, ExternalNumber: phone, InternalNumber: "101", Disposition: disp}
}

func TestSiteOrderReachesCRMExactlyOnce(t *testing.T) {
	e := newEnv(t, service.Config{}, "")
	r, err := e.svc.Ingest(e.ctx, order("web-1"))
	if err != nil || r.Duplicate {
		t.Fatalf("%+v %v", r, err)
	}
	if r, _ := e.svc.Ingest(e.ctx, order("web-1")); !r.Duplicate { // the site retries
		t.Fatal("a repeated order id must be a no-op")
	}
	e.drain()
	os := e.sd.Orders()
	if len(os) != 1 {
		t.Fatalf("want 1 CRM order, got %d", len(os))
	}
	o := os[0]
	if o.ExternalID != "web-1" || o.Data.Phone != "380501234567" || len(o.Data.Products) != 1 ||
		o.Data.Products[0].CostPerItem != 125 || o.Data.Products[0].Amount != 2 || o.Data.NovaPoshta == nil || o.Data.NovaPoshta.WarehouseNumber != "12" {
		t.Fatalf("CRM order: %+v", o)
	}
	v, err := e.svc.GetOrder(e.ctx, "web-1")
	if err != nil || v.SDOrderID != o.ID {
		t.Fatalf("CRM id must be stored: %+v %v", v, err)
	}
}

func TestInvalidOrdersAreRejected(t *testing.T) {
	e := newEnv(t, service.Config{}, "")
	for name, mut := range map[string]func(*service.SiteOrder){
		"no id":      func(o *service.SiteOrder) { o.ID = "" },
		"bad phone":  func(o *service.SiteOrder) { o.Customer.Phone = "123" },
		"no items":   func(o *service.SiteOrder) { o.Items = nil },
		"zero price": func(o *service.SiteOrder) { o.Items[0].Price = 0 },
	} {
		o := order("x")
		mut(&o)
		if _, err := e.svc.Ingest(e.ctx, o); err == nil {
			t.Errorf("%s must be rejected", name)
		}
	}
}

func TestCRMOutageIsRetried(t *testing.T) {
	e := newEnv(t, service.Config{BackoffBase: time.Second}, "")
	e.svc.Ingest(e.ctx, order("web-1"))
	e.sd.FailNext(3)
	e.drain()
	if len(e.sd.Orders()) != 1 {
		t.Fatalf("want 1 order after retries, got %d", len(e.sd.Orders()))
	}
	js, _ := e.svc.Jobs(e.ctx, store.JobDone)
	if len(js) != 1 || js[0].Attempts != 3 {
		t.Fatalf("jobs: %+v", js)
	}
}

func TestLostAnswerDoesNotCreateSecondOrder(t *testing.T) {
	e := newEnv(t, service.Config{}, "")
	e.svc.Ingest(e.ctx, order("web-1"))
	e.sd.LoseNext(1) // the order is created, the answer is an HTTP 500
	e.drain()
	if n := len(e.sd.Orders()); n != 1 {
		t.Fatalf("a lost answer must not duplicate the order, got %d", n)
	}
	if e.sd.HandlerCalls() != 1 {
		t.Fatalf("CreateOrder must be called once, was %d", e.sd.HandlerCalls())
	}
	if v, _ := e.svc.GetOrder(e.ctx, "web-1"); v.SDOrderID == 0 {
		t.Fatal("the id must be found through the lookup")
	}
}

func TestPermanentFailureAndRequeue(t *testing.T) {
	e := newEnv(t, service.Config{MaxAttempts: 3, BackoffBase: time.Second}, "")
	e.svc.Ingest(e.ctx, order("web-1"))
	e.sd.FailNext(100)
	e.drain()
	js, _ := e.svc.Jobs(e.ctx, store.JobFailed)
	if len(js) != 1 || js[0].LastError == "" {
		t.Fatalf("job must fail visibly: %+v", js)
	}
	e.sd.FailNext(0)
	if err := e.svc.Requeue(e.ctx, js[0].ID); err != nil {
		t.Fatal(err)
	}
	e.drain()
	if len(e.sd.Orders()) != 1 {
		t.Fatalf("requeued job must create the order once, got %d", len(e.sd.Orders()))
	}
}

func TestWrongAPIKeyFailsImmediately(t *testing.T) {
	e := newEnv(t, service.Config{}, "wrong")
	e.svc.Ingest(e.ctx, order("web-1"))
	e.drain()
	js, _ := e.svc.Jobs(e.ctx, store.JobFailed)
	if len(js) != 1 || js[0].Attempts != 0 {
		t.Fatalf("a rejected key must not be retried: %+v", js)
	}
}

func TestMissedCalls(t *testing.T) {
	e := newEnv(t, service.Config{LeadDedupe: 30 * time.Minute}, "")
	ph := "0671112233"
	if r, _ := e.svc.OnCall(e.ctx, call("apiCallCompleted", "c1", ph, "NOANSWER")); r.Action != "lead" {
		t.Fatalf("first missed call: %+v", r)
	}
	if r, _ := e.svc.OnCall(e.ctx, call("apiCallCompleted", "c1", ph, "NOANSWER")); r.Action != "duplicate" {
		t.Fatalf("the same call event again: %+v", r)
	}
	e.clk.Add(20 * time.Minute)
	if r, _ := e.svc.OnCall(e.ctx, call("apiCallCompleted", "c2", ph, "BUSY")); r.Action != "note" {
		t.Fatalf("second missed call inside the window: %+v", r)
	}
	if r, _ := e.svc.OnCall(e.ctx, call("apiCallCompleted", "c3", "0509998877", "ANSWER")); r.Action != "answered" {
		t.Fatalf("answered call: %+v", r)
	}
	out := call("apiCallCompleted", "c4", ph, "NOANSWER")
	out.Incoming = false
	if r, _ := e.svc.OnCall(e.ctx, out); r.Action != "ignored" {
		t.Fatalf("outgoing call: %+v", r)
	}
	e.drain() // the note is queued behind the lead's creation and must wait for it
	os := e.sd.Orders()
	if len(os) != 1 || len(os[0].Notes) != 1 || !strings.Contains(os[0].Notes[0], "Ще один пропущений") {
		t.Fatalf("one lead with one note expected: %+v", os)
	}

	e.clk.Add(11 * time.Minute) // 31+ minutes after the lead was created
	if r, _ := e.svc.OnCall(e.ctx, call("apiCallCompleted", "c5", ph, "NOANSWER")); r.Action != "lead" {
		t.Fatalf("after the window a new lead is right: %+v", r)
	}
	e.drain()
	if len(e.sd.Orders()) != 2 {
		t.Fatalf("want 2 leads, got %d", len(e.sd.Orders()))
	}
}

func TestScreenPop(t *testing.T) {
	e := newEnv(t, service.Config{}, "")
	if _, err := e.svc.OnCall(e.ctx, call("receivedTheCall", "r1", "0501234567", "")); err != nil {
		t.Fatal(err)
	}
	if m := e.out.all(); len(m) != 1 || m[0].To != "telegram:1" || !strings.Contains(m[0].Text, "нового номера") {
		t.Fatalf("unknown caller goes to the default manager: %+v", m)
	}
	e.svc.Ingest(e.ctx, order("web-1"))
	e.drain()
	if _, err := e.svc.OnCall(e.ctx, call("receivedTheCall", "r2", "+38 (050) 123-45-67", "")); err != nil {
		t.Fatal(err)
	}
	m := e.out.all()
	if len(m) != 2 || !strings.Contains(m[1].Text, "Іра Коваль") || !strings.HasPrefix(m[1].To, "telegram:10") || m[1].To == "telegram:1" {
		t.Fatalf("known caller goes to their manager with the card: %+v", m)
	}
}

func poll(t *testing.T, e *env) {
	t.Helper()
	e.clk.Add(11 * time.Minute)
	if _, err := e.svc.PollTTNs(e.ctx); err != nil {
		t.Fatal(err)
	}
	e.drain()
}

func TestParcelStatusDrivesFunnelAndCustomerMessages(t *testing.T) {
	e := newEnv(t, service.Config{}, "")
	o := order("web-1")
	o.TTN = "59000000000001"
	e.svc.Ingest(e.ctx, o)
	e.drain()

	e.np.Set(o.TTN, 5)
	poll(t, e)
	e.np.Set(o.TTN, 7)
	poll(t, e)
	if got := e.sd.Orders()[0].StatusID; got != 21 {
		t.Fatalf("arrived must move the deal to status 21, got %d", got)
	}
	e.np.Set(o.TTN, 5) // a flapping answer must not drag it back...
	poll(t, e)
	e.np.Set(o.TTN, 7) // ...and its return must not announce arrival a second time
	poll(t, e)
	if got := e.sd.Orders()[0].StatusID; got != 21 {
		t.Fatalf("a backwards status must be ignored, deal is at %d", got)
	}
	e.np.Set(o.TTN, 9)
	poll(t, e)
	if got := e.sd.Orders()[0].StatusID; got != 22 {
		t.Fatalf("received must move the deal to 22, got %d", got)
	}
	reqs, _ := e.np.Stats()
	poll(t, e)
	if r2, _ := e.np.Stats(); r2 != reqs {
		t.Fatal("a finished parcel must not be polled again")
	}
	var texts []string
	for _, m := range e.out.all() {
		if m.To != "telegram:42" {
			t.Fatalf("customer messages go to the customer: %+v", m)
		}
		texts = append(texts, m.Text)
	}
	joined := strings.Join(texts, "|")
	if len(texts) != 3 || !strings.Contains(joined, "в дорозі") || !strings.Contains(joined, "прибула на відділення") || !strings.Contains(joined, "отримано") {
		t.Fatalf("want one message per stage (transit, arrived, received), got %q", joined)
	}
}

func TestRefusalAlertsTheManager(t *testing.T) {
	e := newEnv(t, service.Config{}, "")
	o := order("web-1")
	o.TTN = "59000000000002"
	e.svc.Ingest(e.ctx, o)
	e.drain()
	e.np.Set(o.TTN, 102)
	poll(t, e)
	m := e.out.all()
	if len(m) != 1 || m[0].To != "telegram:1" || !strings.Contains(m[0].Text, "відмова") {
		t.Fatalf("%+v", m)
	}
	if e.sd.Orders()[0].StatusID != 23 {
		t.Fatal("refusal must map to its CRM status")
	}
}

func TestSalesDriveWebhookRegistersParcelAndLearnsID(t *testing.T) {
	e := newEnv(t, service.Config{}, "")
	e.svc.Ingest(e.ctx, order("web-1")) // create job not processed yet
	var w service.SDWebhook
	w.Data.ID, w.Data.ExternalID = 1001, "web-1"
	w.Data.Delivery = []struct {
		Provider       string `json:"provider"`
		TrackingNumber any    `json:"trackingNumber"`
	}{{"novaposhta", float64(20451253019078)}, {"ukrposhta", "UA123"}, {"novaposhta", ""}}
	if err := e.svc.OnSalesDrive(e.ctx, w); err != nil {
		t.Fatal(err)
	}
	if err := e.svc.OnSalesDrive(e.ctx, w); err != nil { // webhooks repeat
		t.Fatal(err)
	}
	v, _ := e.svc.GetOrder(e.ctx, "web-1")
	if v.SDOrderID != 1001 || len(v.Parcels) != 1 || v.Parcels[0].Number != "20451253019078" || v.Parcels[0].SDOrderID != 1001 {
		t.Fatalf("%+v", v)
	}
	if err := e.svc.OnSalesDrive(e.ctx, service.SDWebhook{}); err == nil {
		t.Fatal("a webhook without an order id must be rejected")
	}
}

func TestTrackingIsBatched(t *testing.T) {
	e := newEnv(t, service.Config{}, "")
	for i := 0; i < 250; i++ {
		if err := e.svc.AddTTN(e.ctx, fmt.Sprintf("5900000%07d", i), ""); err != nil {
			t.Fatal(err)
		}
	}
	e.clk.Add(11 * time.Minute)
	total := 0
	for {
		n, err := e.svc.PollTTNs(e.ctx)
		if err != nil {
			t.Fatal(err)
		}
		if n == 0 {
			break
		}
		total += n
	}
	reqs, nums := e.np.Stats()
	if total != 250 || reqs != 3 || nums != 250 {
		t.Fatalf("250 parcels must take 3 requests (100+100+50): polled=%d requests=%d numbers=%d", total, reqs, nums)
	}
}

func TestNovaPoshtaOutageIsNotFatal(t *testing.T) {
	e := newEnv(t, service.Config{}, "")
	e.svc.AddTTN(e.ctx, "59000000000009", "")
	e.np.FailNext(1)
	e.clk.Add(11 * time.Minute)
	if _, err := e.svc.PollTTNs(e.ctx); err == nil {
		t.Fatal("the failed poll should be reported")
	}
	e.np.Set("59000000000009", 7)
	if n, err := e.svc.PollTTNs(e.ctx); err != nil || n != 1 {
		t.Fatalf("the next round must pick the parcel up again: %d %v", n, err)
	}
}
