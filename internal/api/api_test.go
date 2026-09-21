package api_test

import (
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/valpere/crmbridge/internal/api"
	"github.com/valpere/crmbridge/internal/fakenp"
	"github.com/valpere/crmbridge/internal/fakesalesdrive"
	"github.com/valpere/crmbridge/internal/novaposhta"
	"github.com/valpere/crmbridge/internal/salesdrive"
	"github.com/valpere/crmbridge/internal/service"
	"github.com/valpere/crmbridge/internal/store"
)

type sink struct {
	mu  sync.Mutex
	got []string
}

func (s *sink) Send(_ context.Context, to, text string) error {
	s.mu.Lock()
	s.got = append(s.got, to+": "+text)
	s.mu.Unlock()
	return nil
}

type rig struct {
	t   *testing.T
	ts  *httptest.Server
	svc *service.Service
	sd  *fakesalesdrive.Server
	np  *fakenp.Server
	out *sink
}

func newRig(t *testing.T) *rig {
	t.Helper()
	sd := fakesalesdrive.New("k")
	sts := httptest.NewServer(sd.Handler())
	t.Cleanup(sts.Close)
	np := fakenp.New()
	nts := httptest.NewServer(np.Handler())
	t.Cleanup(nts.Close)
	st, err := store.Open(":memory:")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { st.Close() })
	out := &sink{}
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	svc := service.New(st, salesdrive.New(salesdrive.Config{BaseURL: sts.URL, APIKey: "k"}),
		novaposhta.New(novaposhta.Config{BaseURL: nts.URL}), out, service.Config{
			StatusMap: map[int]int{7: 21}, DefaultManager: "telegram:1", PollEvery: time.Nanosecond, BackoffBase: time.Millisecond}, log)
	ts := httptest.NewServer(api.New(svc, api.Options{Token: "tok", WebhookSecret: "sec"}, log).Handler())
	t.Cleanup(ts.Close)
	return &rig{t: t, ts: ts, svc: svc, sd: sd, np: np, out: out}
}

func (r *rig) req(method, path, ctype, auth, body string) (int, map[string]any) {
	r.t.Helper()
	req, _ := http.NewRequest(method, r.ts.URL+path, strings.NewReader(body))
	if ctype != "" {
		req.Header.Set("Content-Type", ctype)
	}
	if auth != "" {
		req.Header.Set("Authorization", "Bearer "+auth)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		r.t.Fatal(err)
	}
	defer resp.Body.Close()
	var out map[string]any
	_ = json.NewDecoder(resp.Body).Decode(&out)
	return resp.StatusCode, out
}

func (r *rig) drain() {
	r.t.Helper()
	for i := 0; i < 20; i++ {
		n, err := r.svc.ProcessOnce(context.Background())
		if err != nil {
			r.t.Fatal(err)
		}
		if n == 0 {
			return
		}
		time.Sleep(2 * time.Millisecond)
	}
}

const site = `{"id":"web-7","customer":{"first_name":"Іра","phone":"0501234567","telegram_chat":"42"},"items":[{"sku":"A","name":"Кава","price":9900,"qty":1}],"city":"Київ","warehouse":"5"}`

func TestSiteIntakeAuthAndIdempotency(t *testing.T) {
	r := newRig(t)
	if code, _ := r.req("POST", "/site/orders", "application/json", "", site); code != 401 {
		t.Fatalf("no token: %d", code)
	}
	if code, _ := r.req("POST", "/site/orders", "application/json", "tok", `{"id":"x"}`); code != 400 {
		t.Fatalf("invalid order: %d", code)
	}
	if code, _ := r.req("POST", "/site/orders", "application/json", "tok", `{"id":"x","bogus":1}`); code != 400 {
		t.Fatalf("unknown fields are rejected: %d", code)
	}
	code, res := r.req("POST", "/site/orders", "application/json", "tok", site)
	if code != 202 || res["duplicate"] != false {
		t.Fatalf("intake: %d %v", code, res)
	}
	if _, res = r.req("POST", "/site/orders", "application/json", "tok", site); res["duplicate"] != true {
		t.Fatalf("retry: %v", res)
	}
	r.drain()
	if len(r.sd.Orders()) != 1 {
		t.Fatalf("want 1 CRM order, got %d", len(r.sd.Orders()))
	}
	if code, o := r.req("GET", "/api/orders/web-7", "", "tok", ""); code != 200 || o["salesdrive_order_id"].(float64) == 0 {
		t.Fatalf("order view: %d %v", code, o)
	}
	if code, _ := r.req("GET", "/api/orders/nope", "", "tok", ""); code != 404 {
		t.Fatalf("unknown order: %d", code)
	}
}

func TestBinotelWebhookShapesAndSecret(t *testing.T) {
	r := newRig(t)
	js := `{"requestType":"apiCallCompleted","callDetails":{"generalCallID":"9001","callType":"0","externalNumber":"0671112233","internalNumber":"101","disposition":"NOANSWER"}}`
	if code, _ := r.req("POST", "/webhooks/binotel", "application/json", "", js); code != 401 {
		t.Fatalf("webhook without the secret: %d", code)
	}
	code, res := r.req("POST", "/webhooks/binotel?token=sec", "application/json", "", js)
	if code != 200 || res["status"] != "success" || res["action"] != "lead" {
		t.Fatalf("json: %d %v", code, res)
	}
	form := "requestType=apiCallCompleted&callDetails%5BgeneralCallID%5D=9002&callDetails%5BcallType%5D=0&callDetails%5BexternalNumber%5D=0671112233&callDetails%5Bdisposition%5D=BUSY"
	if _, res = r.req("POST", "/webhooks/binotel?token=sec", "application/x-www-form-urlencoded", "", form); res["action"] != "note" {
		t.Fatalf("form: %v", res)
	}
	if code, _ := r.req("POST", "/webhooks/binotel?token=sec", "application/json", "", `{"requestType":"x"}`); code != 400 {
		t.Fatalf("incomplete event: %d", code)
	}
	// a ring for a number the CRM does not know: answered, message goes to the default manager
	ring := `{"requestType":"receivedTheCall","callDetails":{"generalCallID":"9003","callType":"0","externalNumber":"0990000000"}}`
	if code, res := r.req("POST", "/webhooks/binotel?token=sec", "application/json", "", ring); code != 200 || res["action"] != "screen_pop" {
		t.Fatalf("ring: %d %v", code, res)
	}
	if len(r.out.got) != 1 || !strings.HasPrefix(r.out.got[0], "telegram:1:") {
		t.Fatalf("notifications: %v", r.out.got)
	}
	r.drain()
	if o := r.sd.Orders(); len(o) != 1 || len(o[0].Notes) != 1 {
		t.Fatalf("one lead with one note: %+v", o)
	}
}

func TestSalesDriveWebhookToTrackingToFunnel(t *testing.T) {
	r := newRig(t)
	r.req("POST", "/site/orders", "application/json", "tok", site)
	r.drain()
	id := r.sd.Orders()[0].ID
	hook := `{"info":{"webhookType":"order","webhookEvent":"status_change","account":"demo"},"data":{"id":` + itoa(id) +
		`,"externalId":"web-7","ord_delivery_data":[{"provider":"novaposhta","trackingNumber":20451253019078}]}}`
	if code, _ := r.req("POST", "/webhooks/salesdrive", "application/json", "", hook); code != 401 {
		t.Fatalf("no secret: %d", code)
	}
	if code, _ := r.req("POST", "/webhooks/salesdrive?token=sec", "application/json", "", hook); code != 200 {
		t.Fatalf("webhook: %d", code)
	}
	r.np.Set("20451253019078", 7)
	if _, err := r.svc.PollTTNs(context.Background()); err != nil {
		t.Fatal(err)
	}
	r.drain()
	if got := r.sd.Orders()[0].StatusID; got != 21 {
		t.Fatalf("funnel status = %d, want 21", got)
	}
	if len(r.out.got) != 1 || !strings.Contains(r.out.got[0], "telegram:42") {
		t.Fatalf("customer message: %v", r.out.got)
	}
	if code, _ := r.req("POST", "/webhooks/salesdrive?token=sec", "application/json", "", `{"data":{}}`); code != 400 {
		t.Fatalf("webhook without an id: %d", code)
	}
}

func itoa(n int64) string { b, _ := json.Marshal(n); return string(b) }
