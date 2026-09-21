// Package fakesalesdrive is an in-memory stand-in for the SalesDrive API,
// shaped after its published OpenAPI spec (X-Api-Key header, /handler/,
// /api/order/update/, /api/order/note/, /api/order/list/,
// /api/get_manager_by_phone_number/) and able to emit its webhook. It serves
// the demo and the tests.
package fakesalesdrive

import (
	"bytes"
	"encoding/json"
	"net/http"
	"strconv"
	"sync"

	"github.com/valpere/crmbridge/internal/phone"
	"github.com/valpere/crmbridge/internal/salesdrive"
)

type Order struct {
	ID         int64               `json:"id"`
	ExternalID string              `json:"externalId"`
	StatusID   int                 `json:"statusId"`
	Manager    int                 `json:"manager"`
	TTN        string              `json:"ttn,omitempty"`
	Notes      []string            `json:"notes"`
	Data       salesdrive.NewOrder `json:"data"`
}

type Server struct {
	APIKey     string
	Managers   []string // manager names, assigned round-robin; internal numbers 101, 102, ...
	WebhookURL string   // where to POST webhooks; empty disables them
	NewStatus  int      // status id of a fresh order

	mu       sync.Mutex
	orders   []*Order
	seq      int64
	failNext int
	loseNext int // create the order, then answer 500
	Calls    int // requests seen on /handler/
}

func New(apiKey string) *Server {
	return &Server{APIKey: apiKey, Managers: []string{"Олексій", "Марія"}, NewStatus: 1}
}

func (s *Server) FailNext(n int) { s.mu.Lock(); s.failNext = n; s.mu.Unlock() }

// LoseNext makes the next n /handler/ calls create the order but answer 500.
func (s *Server) LoseNext(n int) { s.mu.Lock(); s.loseNext = n; s.mu.Unlock() }

func (s *Server) Orders() []Order {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]Order, len(s.orders))
	for i, o := range s.orders {
		out[i] = *o
		out[i].Notes = append([]string(nil), o.Notes...)
	}
	return out
}

func (s *Server) HandlerCalls() int { s.mu.Lock(); defer s.mu.Unlock(); return s.Calls }

func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("POST /handler/", s.auth(s.create))
	mux.HandleFunc("POST /api/order/update/", s.auth(s.update))
	mux.HandleFunc("POST /api/order/note/", s.auth(s.note))
	mux.HandleFunc("GET /api/order/list/", s.auth(s.list))
	mux.HandleFunc("GET /api/get_manager_by_phone_number/", s.auth(s.byPhone))
	mux.HandleFunc("POST /_fail", func(w http.ResponseWriter, r *http.Request) {
		var v struct{ N int }
		_ = json.NewDecoder(r.Body).Decode(&v)
		s.FailNext(v.N)
		w.WriteHeader(http.StatusNoContent)
	})
	mux.HandleFunc("POST /_lose", func(w http.ResponseWriter, r *http.Request) {
		var v struct{ N int }
		_ = json.NewDecoder(r.Body).Decode(&v)
		s.LoseNext(v.N)
		w.WriteHeader(http.StatusNoContent)
	})
	// A manager creates a TTN inside SalesDrive: the order changes and the
	// status_change webhook fires.
	mux.HandleFunc("POST /_ttn", func(w http.ResponseWriter, r *http.Request) {
		var v struct {
			ID  int64  `json:"id"`
			TTN string `json:"ttn"`
		}
		_ = json.NewDecoder(r.Body).Decode(&v)
		s.mu.Lock()
		var o *Order
		for _, x := range s.orders {
			if x.ID == v.ID {
				o = x
			}
		}
		if o == nil {
			s.mu.Unlock()
			jsonOut(w, 404, map[string]any{"success": false, "message": "order not found"})
			return
		}
		o.TTN = v.TTN
		s.mu.Unlock()
		s.emit("status_change", o)
		w.WriteHeader(http.StatusNoContent)
	})
	mux.HandleFunc("GET /_orders", func(w http.ResponseWriter, r *http.Request) { jsonOut(w, 200, s.Orders()) })
	return mux
}

func jsonOut(w http.ResponseWriter, code int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(v)
}

func fail(w http.ResponseWriter, code int, msg string) {
	jsonOut(w, code, map[string]any{"success": false, "message": msg})
}

func (s *Server) auth(next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("X-Api-Key") != s.APIKey {
			fail(w, http.StatusUnauthorized, "Invalid API key")
			return
		}
		next(w, r)
	}
}

func (s *Server) create(w http.ResponseWriter, r *http.Request) {
	s.mu.Lock()
	s.Calls++
	if s.failNext > 0 {
		s.failNext--
		s.mu.Unlock()
		fail(w, http.StatusServiceUnavailable, "temporarily unavailable")
		return
	}
	var in salesdrive.NewOrder
	if err := json.NewDecoder(r.Body).Decode(&in); err != nil {
		s.mu.Unlock()
		fail(w, http.StatusBadRequest, "invalid JSON")
		return
	}
	if in.Phone == "" && in.Email == "" {
		s.mu.Unlock()
		fail(w, http.StatusBadRequest, "phone or email is required")
		return
	}
	s.seq++
	o := &Order{ID: 1000 + s.seq, ExternalID: in.ExternalID, StatusID: s.NewStatus, Manager: int(s.seq)%len(s.Managers) + 1, Data: in,
		Notes: []string{}}
	if in.NovaPoshta != nil {
		o.TTN = in.NovaPoshta.TTN
	}
	s.orders = append(s.orders, o)
	lose := s.loseNext > 0
	if lose {
		s.loseNext--
	}
	s.mu.Unlock()
	s.emit("new_order", o)
	if lose {
		fail(w, http.StatusInternalServerError, "internal error")
		return
	}
	jsonOut(w, 200, map[string]any{"success": true, "data": map[string]any{"orderId": o.ID, "userId": o.Manager}})
}

func (s *Server) find(id int64, ext string) *Order {
	for _, o := range s.orders {
		if (id != 0 && o.ID == id) || (id == 0 && ext != "" && o.ExternalID == ext) {
			return o
		}
	}
	return nil
}

func (s *Server) update(w http.ResponseWriter, r *http.Request) {
	var in struct {
		ID         int64  `json:"id"`
		ExternalID string `json:"externalId"`
		Data       struct {
			StatusID int `json:"statusId"`
		} `json:"data"`
	}
	if json.NewDecoder(r.Body).Decode(&in) != nil {
		fail(w, http.StatusBadRequest, "invalid JSON")
		return
	}
	s.mu.Lock()
	if s.failNext > 0 {
		s.failNext--
		s.mu.Unlock()
		fail(w, http.StatusServiceUnavailable, "temporarily unavailable")
		return
	}
	o := s.find(in.ID, in.ExternalID)
	if o == nil {
		s.mu.Unlock()
		fail(w, http.StatusBadRequest, "Order not found.")
		return
	}
	o.StatusID = in.Data.StatusID
	s.mu.Unlock()
	s.emit("status_change", o)
	jsonOut(w, 200, map[string]any{"success": true})
}

func (s *Server) note(w http.ResponseWriter, r *http.Request) {
	var in struct {
		OrderID int64  `json:"orderId"`
		Text    string `json:"text"`
	}
	if json.NewDecoder(r.Body).Decode(&in) != nil {
		fail(w, http.StatusBadRequest, "invalid JSON")
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	o := s.find(in.OrderID, "")
	if o == nil {
		fail(w, http.StatusBadRequest, "Order "+strconv.FormatInt(in.OrderID, 10)+" not found.")
		return
	}
	o.Notes = append(o.Notes, in.Text)
	jsonOut(w, 200, map[string]any{"success": true, "data": map[string]any{"noteId": len(o.Notes)}})
}

func (s *Server) list(w http.ResponseWriter, r *http.Request) {
	ext := r.URL.Query().Get("filter[externalId]")
	s.mu.Lock()
	defer s.mu.Unlock()
	data := []map[string]any{}
	for _, o := range s.orders {
		if ext == "" || o.ExternalID == ext {
			data = append(data, map[string]any{"id": o.ID, "externalId": o.ExternalID, "statusId": o.StatusID})
		}
	}
	jsonOut(w, 200, map[string]any{"status": "success", "data": data})
}

func (s *Server) byPhone(w http.ResponseWriter, r *http.Request) {
	want := phone.Normalize(r.URL.Query().Get("phone"))
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, o := range s.orders {
		if want != "" && phone.Normalize(o.Data.Phone) == want {
			jsonOut(w, 200, map[string]any{"status": "success",
				"manager": map[string]any{"name": s.Managers[(o.Manager-1)%len(s.Managers)], "internal_number": strconv.Itoa(100 + o.Manager)},
				"client":  map[string]any{"id": o.ID, "fName": o.Data.FName, "lName": o.Data.LName, "company": ""}})
			return
		}
	}
	jsonOut(w, 200, map[string]any{"status": "error"})
}

// emit posts a SalesDrive-shaped webhook (no signature: the spec defines none).
func (s *Server) emit(event string, o *Order) {
	s.mu.Lock()
	url := s.WebhookURL
	body := map[string]any{
		"info": map[string]any{"webhookType": "order", "webhookEvent": event, "account": "demo"},
		"data": map[string]any{"id": o.ID, "externalId": o.ExternalID, "statusId": o.StatusID,
			"contacts": []map[string]any{{"fName": o.Data.FName, "lName": o.Data.LName, "phone": []string{o.Data.Phone}}},
			"ord_delivery_data": func() []map[string]any {
				if o.TTN == "" {
					return []map[string]any{}
				}
				return []map[string]any{{"provider": "novaposhta", "trackingNumber": o.TTN}}
			}()},
	}
	s.mu.Unlock()
	if url == "" {
		return
	}
	b, _ := json.Marshal(body)
	go func() {
		resp, err := http.Post(url, "application/json", bytes.NewReader(b))
		if err == nil {
			resp.Body.Close()
		}
	}()
}
