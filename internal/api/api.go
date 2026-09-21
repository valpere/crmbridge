// Package api is the HTTP surface: site/bot order intake, the Binotel and
// SalesDrive webhooks, and a small management API.
package api

import (
	"crypto/subtle"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"strconv"
	"strings"

	"github.com/valpere/crmbridge/internal/binotel"
	"github.com/valpere/crmbridge/internal/service"
	"github.com/valpere/crmbridge/internal/store"
)

type Options struct {
	Token         string // bearer for /site/orders and /api/*; empty disables auth (demo only)
	WebhookSecret string // ?token= for webhooks; empty disables the check (demo only)
}

type Server struct {
	svc *service.Service
	opt Options
	log *slog.Logger
}

func New(svc *service.Service, opt Options, log *slog.Logger) *Server {
	return &Server{svc: svc, opt: opt, log: log}
}

func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) { _, _ = io.WriteString(w, "ok\n") })
	mux.HandleFunc("POST /site/orders", s.bearer(s.siteOrder))
	mux.HandleFunc("POST /webhooks/binotel", s.secret(s.binotel))
	mux.HandleFunc("POST /webhooks/salesdrive", s.secret(s.salesdrive))
	mux.HandleFunc("GET /api/orders/{id}", s.bearer(s.getOrder))
	mux.HandleFunc("POST /api/ttn", s.bearer(s.addTTN))
	mux.HandleFunc("GET /api/jobs", s.bearer(s.jobs))
	mux.HandleFunc("POST /api/jobs/{id}/requeue", s.bearer(s.requeue))
	return mux
}

func eq(a, b string) bool { return subtle.ConstantTimeCompare([]byte(a), []byte(b)) == 1 }

func (s *Server) bearer(next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if s.opt.Token != "" {
			got, _ := strings.CutPrefix(r.Header.Get("Authorization"), "Bearer ")
			if !eq(got, s.opt.Token) {
				httpError(w, http.StatusUnauthorized, "unauthorized")
				return
			}
		}
		next(w, r)
	}
}

// secret guards webhooks whose senders cannot sign requests: SalesDrive's
// webhook has no signature in its spec, so a shared token in the URL is used.
func (s *Server) secret(next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if s.opt.WebhookSecret != "" && !eq(r.URL.Query().Get("token"), s.opt.WebhookSecret) {
			s.log.Warn("webhook with a bad token rejected", "path", r.URL.Path)
			httpError(w, http.StatusUnauthorized, "unauthorized")
			return
		}
		next(w, r)
	}
}

func writeJSON(w http.ResponseWriter, code int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(v)
}

func httpError(w http.ResponseWriter, code int, msg string) {
	writeJSON(w, code, map[string]string{"error": msg})
}

func (s *Server) fail(w http.ResponseWriter, err error) {
	switch {
	case errors.Is(err, service.ErrInvalid):
		httpError(w, http.StatusBadRequest, err.Error())
	case errors.Is(err, store.ErrNotFound):
		httpError(w, http.StatusNotFound, "not found")
	default:
		s.log.Error("request failed", "err", err)
		httpError(w, http.StatusInternalServerError, "internal error")
	}
}

func (s *Server) siteOrder(w http.ResponseWriter, r *http.Request) {
	var in service.SiteOrder
	dec := json.NewDecoder(io.LimitReader(r.Body, 1<<20))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&in); err != nil {
		httpError(w, http.StatusBadRequest, "bad JSON: "+err.Error())
		return
	}
	res, err := s.svc.Ingest(r.Context(), in)
	if err != nil {
		s.fail(w, err)
		return
	}
	writeJSON(w, http.StatusAccepted, res)
}

func (s *Server) binotel(w http.ResponseWriter, r *http.Request) {
	body, err := io.ReadAll(io.LimitReader(r.Body, 1<<20))
	if err != nil {
		httpError(w, http.StatusBadRequest, "unreadable body")
		return
	}
	ev, err := binotel.Parse(r.Header.Get("Content-Type"), body)
	if err != nil {
		httpError(w, http.StatusBadRequest, err.Error())
		return
	}
	res, err := s.svc.OnCall(r.Context(), ev)
	if err != nil {
		if res.Action == "screen_pop" {
			// The pop-up is best effort: a CRM hiccup must not make Binotel retry.
			s.log.Warn("screen pop failed", "call", ev.GeneralCallID, "err", err)
		} else {
			s.log.Error("call event failed", "call", ev.GeneralCallID, "err", err)
			httpError(w, http.StatusInternalServerError, "try again")
			return
		}
	}
	s.log.Info("call event", "type", ev.RequestType, "call", ev.GeneralCallID, "action", res.Action)
	writeJSON(w, http.StatusOK, map[string]string{"status": "success", "action": res.Action})
}

func (s *Server) salesdrive(w http.ResponseWriter, r *http.Request) {
	var wh service.SDWebhook
	if err := json.NewDecoder(io.LimitReader(r.Body, 4<<20)).Decode(&wh); err != nil {
		httpError(w, http.StatusBadRequest, "bad JSON")
		return
	}
	if err := s.svc.OnSalesDrive(r.Context(), wh); err != nil {
		s.fail(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]bool{"ok": true})
}

func (s *Server) getOrder(w http.ResponseWriter, r *http.Request) {
	v, err := s.svc.GetOrder(r.Context(), r.PathValue("id"))
	if err != nil {
		s.fail(w, err)
		return
	}
	writeJSON(w, http.StatusOK, v)
}

func (s *Server) addTTN(w http.ResponseWriter, r *http.Request) {
	var in struct {
		Number  string `json:"number"`
		OrderID string `json:"order_id"`
	}
	if json.NewDecoder(io.LimitReader(r.Body, 1<<16)).Decode(&in) != nil {
		httpError(w, http.StatusBadRequest, "bad JSON")
		return
	}
	if err := s.svc.AddTTN(r.Context(), in.Number, in.OrderID); err != nil {
		s.fail(w, err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func (s *Server) jobs(w http.ResponseWriter, r *http.Request) {
	js, err := s.svc.Jobs(r.Context(), r.URL.Query().Get("state"))
	if err != nil {
		s.fail(w, err)
		return
	}
	writeJSON(w, http.StatusOK, js)
}

func (s *Server) requeue(w http.ResponseWriter, r *http.Request) {
	id, err := strconv.ParseInt(r.PathValue("id"), 10, 64)
	if err != nil {
		httpError(w, http.StatusBadRequest, "bad job id")
		return
	}
	if err := s.svc.Requeue(r.Context(), id); err != nil {
		s.fail(w, err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}
