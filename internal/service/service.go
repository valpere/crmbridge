// Package service holds the bridge's rules: site orders and missed calls
// become CRM orders/leads exactly once, screen pops reach the right manager,
// and Nova Poshta parcel statuses move the CRM funnel.
package service

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/valpere/crmbridge/internal/binotel"
	"github.com/valpere/crmbridge/internal/notify"
	"github.com/valpere/crmbridge/internal/novaposhta"
	"github.com/valpere/crmbridge/internal/phone"
	"github.com/valpere/crmbridge/internal/salesdrive"
	"github.com/valpere/crmbridge/internal/store"
)

var ErrInvalid = errors.New("invalid request")

type CRM interface {
	CreateOrder(ctx context.Context, o salesdrive.NewOrder) (int64, error)
	FindByExternalID(ctx context.Context, ext string) (int64, bool, error)
	UpdateStatus(ctx context.Context, ref salesdrive.Ref, statusID int) error
	AddNote(ctx context.Context, orderID int64, text string) error
	ManagerByPhone(ctx context.Context, phone string) (*salesdrive.Contact, error)
}

type Tracker interface {
	Track(ctx context.Context, numbers []string) (map[string]novaposhta.Tracking, error)
}

type Config struct {
	SiteName           string
	StatusMap          map[int]int       // Nova Poshta status code -> CRM status id
	MissedDispositions []string          // Binotel dispositions that count as a missed call
	LeadDedupe         time.Duration     // repeat missed calls inside this window add a note, not a lead
	Managers           map[string]string // internal number -> notify target
	DefaultManager     string
	PollEvery          time.Duration
	MaxTrackAge        time.Duration
	MaxAttempts        int
	BackoffBase        time.Duration
	BackoffMax         time.Duration
}

type Service struct {
	st  *store.Store
	crm CRM
	np  Tracker
	n   notify.Notifier
	cfg Config
	log *slog.Logger
	now func() time.Time
}

func New(st *store.Store, crm CRM, np Tracker, n notify.Notifier, cfg Config, log *slog.Logger) *Service {
	if len(cfg.MissedDispositions) == 0 {
		cfg.MissedDispositions = []string{"NOANSWER", "BUSY", "CANCEL", "CONGESTION", "CHANUNAVAIL"}
	}
	if cfg.LeadDedupe == 0 {
		cfg.LeadDedupe = 30 * time.Minute
	}
	if cfg.PollEvery == 0 {
		cfg.PollEvery = 10 * time.Minute
	}
	if cfg.MaxTrackAge == 0 {
		cfg.MaxTrackAge = 14 * 24 * time.Hour
	}
	if cfg.MaxAttempts == 0 {
		cfg.MaxAttempts = 8
	}
	if cfg.BackoffBase == 0 {
		cfg.BackoffBase = 2 * time.Second
	}
	if cfg.BackoffMax == 0 {
		cfg.BackoffMax = 5 * time.Minute
	}
	return &Service{st: st, crm: crm, np: np, n: n, cfg: cfg, log: log, now: time.Now}
}

func (s *Service) SetClock(now func() time.Time) { s.now = now }

// ---- site / bot orders ---------------------------------------------------

type Item struct {
	SKU   string `json:"sku"`
	Name  string `json:"name"`
	Price int64  `json:"price"` // kopecks per unit
	Qty   int    `json:"qty"`
}

type Customer struct {
	FirstName string `json:"first_name"`
	LastName  string `json:"last_name"`
	Phone     string `json:"phone"`
	Email     string `json:"email"`
	Telegram  string `json:"telegram_chat"` // numeric chat id, for status messages
}

type SiteOrder struct {
	ID            string   `json:"id"`
	Source        string   `json:"source"` // site | telegram | ...
	Customer      Customer `json:"customer"`
	Items         []Item   `json:"items"`
	PaymentMethod string   `json:"payment_method"`
	City          string   `json:"city"`
	Warehouse     string   `json:"warehouse"` // Nova Poshta branch number
	Comment       string   `json:"comment"`
	TTN           string   `json:"ttn"` // when the parcel is already created
	UTMSource     string   `json:"utm_source"`
	UTMMedium     string   `json:"utm_medium"`
	UTMCampaign   string   `json:"utm_campaign"`
}

type IngestResult struct {
	ExternalID string `json:"external_id"`
	Duplicate  bool   `json:"duplicate"`
}

// Ingest validates a site or bot order and queues its creation in the CRM.
// The order id is the idempotency key: a retried request is a no-op.
func (s *Service) Ingest(ctx context.Context, in SiteOrder) (IngestResult, error) {
	ph := phone.Normalize(in.Customer.Phone)
	if in.ID == "" || ph == "" || len(in.Items) == 0 {
		return IngestResult{}, fmt.Errorf("%w: id, a valid Ukrainian phone and at least one item are required", ErrInvalid)
	}
	if in.Source == "" {
		in.Source = "site"
	}
	o := salesdrive.NewOrder{FName: in.Customer.FirstName, LName: in.Customer.LastName, Phone: ph, Email: in.Customer.Email,
		PaymentMethod: in.PaymentMethod, Comment: in.Comment, ExternalID: in.ID, Site: s.cfg.SiteName,
		UTMSource: in.UTMSource, UTMMedium: in.UTMMedium, UTMCampaign: in.UTMCampaign}
	if in.Customer.Telegram != "" {
		o.ConTelegram = in.Customer.Telegram
	}
	for i, it := range in.Items {
		if it.Name == "" || it.Price <= 0 || it.Qty <= 0 {
			return IngestResult{}, fmt.Errorf("%w: item %d needs name, price > 0 and qty > 0", ErrInvalid, i+1)
		}
		o.Products = append(o.Products, salesdrive.Product{ID: it.SKU, SKU: it.SKU, Name: it.Name,
			CostPerItem: float64(it.Price) / 100, Amount: float64(it.Qty)})
	}
	if in.City != "" || in.Warehouse != "" || in.TTN != "" {
		o.ShippingMethod = "Нова Пошта"
		o.NovaPoshta = &salesdrive.NovaPoshta{ServiceType: "WarehouseWarehouse", City: in.City, WarehouseNumber: in.Warehouse, TTN: in.TTN}
	}
	contact := ""
	if in.Customer.Telegram != "" {
		contact = "telegram:" + in.Customer.Telegram
	}
	res := IngestResult{ExternalID: in.ID}
	err := s.st.Do(ctx, func(tx *store.Tx) error {
		ok, err := tx.InsertOrder(ctx, store.Order{ExternalID: in.ID, Source: in.Source, Phone: ph, Contact: contact, CreatedAt: s.now()})
		if err != nil {
			return err
		}
		if !ok {
			res.Duplicate = true
			return nil
		}
		if err := s.enqueue(ctx, tx, "create:"+in.ID, store.KindCreateOrder, createPayload{ExternalID: in.ID, Order: o}); err != nil {
			return err
		}
		if in.TTN != "" {
			_, err = tx.AddTTN(ctx, in.TTN, in.ID, 0, s.now())
		}
		return err
	})
	return res, err
}

type createPayload struct {
	ExternalID string              `json:"external_id"`
	Order      salesdrive.NewOrder `json:"order"`
}

type notePayload struct {
	ExternalID string `json:"external_id"`
	Text       string `json:"text"`
}

type statusPayload struct {
	SDOrderID  int64  `json:"sd_order_id"`
	ExternalID string `json:"external_id"`
	StatusID   int    `json:"status_id"`
}

func (s *Service) enqueue(ctx context.Context, tx *store.Tx, key, kind string, payload any) error {
	b, _ := json.Marshal(payload)
	_, err := tx.Enqueue(ctx, key, kind, b, s.now())
	return err
}

// ---- telephony -----------------------------------------------------------

// CallResult reports what a call event led to.
type CallResult struct {
	Action string `json:"action"` // screen_pop | lead | note | duplicate | ignored | answered
}

// OnCall handles a Binotel event. An incoming ring pushes the caller's card
// to the responsible manager; a finished call that was not answered becomes a
// lead, or a note on the lead the same caller already produced recently.
func (s *Service) OnCall(ctx context.Context, ev binotel.Event) (CallResult, error) {
	ph := phone.Normalize(ev.ExternalNumber)
	if !ev.Incoming || ph == "" {
		return CallResult{Action: "ignored"}, nil
	}
	switch ev.RequestType {
	case "receivedTheCall":
		return CallResult{Action: "screen_pop"}, s.screenPop(ctx, ph, ev)
	case "apiCallCompleted":
		return s.completed(ctx, ph, ev)
	}
	return CallResult{Action: "ignored"}, nil
}

func (s *Service) screenPop(ctx context.Context, ph string, ev binotel.Event) error {
	ct, err := s.crm.ManagerByPhone(ctx, ph)
	if err != nil {
		return err
	}
	to, text := s.cfg.DefaultManager, "Дзвінок від нового номера +"+ph+": у CRM клієнта ще немає."
	if ct != nil {
		if t, ok := s.cfg.Managers[ct.InternalNumber]; ok {
			to = t
		}
		who := ct.ClientName
		if ct.ClientCompany != "" {
			who += " (" + ct.ClientCompany + ")"
		}
		text = fmt.Sprintf("Дзвінок від %s, +%s. Відповідальний: %s.", strings.TrimSpace(who), ph, ct.ManagerName)
	}
	return s.n.Send(ctx, to, text)
}

func (s *Service) missed(d string) bool { return slices.Contains(s.cfg.MissedDispositions, d) }

func (s *Service) completed(ctx context.Context, ph string, ev binotel.Event) (CallResult, error) {
	var res CallResult
	err := s.st.Do(ctx, func(tx *store.Tx) error {
		kind := "answered"
		if s.missed(ev.Disposition) {
			kind = "missed"
		}
		fresh, err := tx.InsertCall(ctx, ev.GeneralCallID, ph, kind, ev.Disposition, s.now())
		if err != nil {
			return err
		}
		switch {
		case !fresh:
			res.Action = "duplicate"
		case kind == "answered":
			res.Action = "answered"
		default:
			res.Action, err = s.missedCall(ctx, tx, ph, ev)
		}
		return err
	})
	return res, err
}

func (s *Service) missedCall(ctx context.Context, tx *store.Tx, ph string, ev binotel.Event) (string, error) {
	at := s.now().Format("15:04")
	if lead, err := tx.RecentLead(ctx, ph, s.now().Add(-s.cfg.LeadDedupe)); err == nil {
		return "note", s.enqueue(ctx, tx, "note:"+ev.GeneralCallID, store.KindNote,
			notePayload{ExternalID: lead.ExternalID, Text: "Ще один пропущений дзвінок о " + at})
	} else if !errors.Is(err, store.ErrNotFound) {
		return "", err
	}
	ext := "call-" + ev.GeneralCallID
	if _, err := tx.InsertOrder(ctx, store.Order{ExternalID: ext, Source: store.SourceCall, Phone: ph, CreatedAt: s.now()}); err != nil {
		return "", err
	}
	o := salesdrive.NewOrder{Phone: ph, ExternalID: ext, Site: "Телефон",
		Comment: fmt.Sprintf("Пропущений дзвінок о %s на лінію %s. Передзвонити.", at, ev.InternalNumber)}
	return "lead", s.enqueue(ctx, tx, "create:"+ext, store.KindCreateOrder, createPayload{ExternalID: ext, Order: o})
}

// ---- SalesDrive webhook ----------------------------------------------------

// SDWebhook is the part of a SalesDrive webhook the bridge reads.
type SDWebhook struct {
	Info struct {
		WebhookEvent string `json:"webhookEvent"`
	} `json:"info"`
	Data struct {
		ID         int64  `json:"id"`
		ExternalID string `json:"externalId"`
		Delivery   []struct {
			Provider       string `json:"provider"`
			TrackingNumber any    `json:"trackingNumber"`
		} `json:"ord_delivery_data"`
	} `json:"data"`
}

func ttnString(v any) string {
	switch t := v.(type) {
	case string:
		return strings.TrimSpace(t)
	case float64:
		return strconv.FormatFloat(t, 'f', 0, 64)
	}
	return ""
}

// OnSalesDrive learns the CRM order id early (a webhook can beat the create
// call's own answer) and starts tracking any Nova Poshta TTN on the order.
func (s *Service) OnSalesDrive(ctx context.Context, w SDWebhook) error {
	if w.Data.ID == 0 {
		return fmt.Errorf("%w: webhook without data.id", ErrInvalid)
	}
	return s.st.Do(ctx, func(tx *store.Tx) error {
		if w.Data.ExternalID != "" {
			if err := tx.SetSDOrderID(ctx, w.Data.ExternalID, w.Data.ID); err != nil {
				return err
			}
		}
		for _, d := range w.Data.Delivery {
			if d.Provider != "novaposhta" {
				continue
			}
			if n := ttnString(d.TrackingNumber); n != "" {
				if _, err := tx.AddTTN(ctx, n, w.Data.ExternalID, w.Data.ID, s.now()); err != nil {
					return err
				}
			}
		}
		return nil
	})
}

// AddTTN starts tracking a parcel by hand.
func (s *Service) AddTTN(ctx context.Context, number, ext string) error {
	number = strings.TrimSpace(number)
	if len(number) < 10 {
		return fmt.Errorf("%w: a Nova Poshta TTN has 14 digits", ErrInvalid)
	}
	return s.st.Do(ctx, func(tx *store.Tx) error {
		var id int64
		if ext != "" {
			if o, err := tx.GetOrder(ctx, ext); err == nil {
				id = o.SDOrderID
			}
		}
		_, err := tx.AddTTN(ctx, number, ext, id, s.now())
		return err
	})
}

// ---- read models -----------------------------------------------------------

type OrderView struct {
	store.Order
	Parcels []store.TTN `json:"parcels"`
}

func (s *Service) GetOrder(ctx context.Context, ext string) (*OrderView, error) {
	var v OrderView
	err := s.st.Do(ctx, func(tx *store.Tx) error {
		o, err := tx.GetOrder(ctx, ext)
		if err != nil {
			return err
		}
		v.Order = *o
		v.Parcels, err = tx.TTNsForOrder(ctx, ext)
		return err
	})
	if v.Parcels == nil {
		v.Parcels = []store.TTN{}
	}
	return &v, err
}

func (s *Service) Jobs(ctx context.Context, state string) ([]store.Job, error) {
	var out []store.Job
	err := s.st.Do(ctx, func(tx *store.Tx) (e error) {
		out, e = tx.JobsByState(ctx, state, 200)
		return
	})
	return out, err
}
