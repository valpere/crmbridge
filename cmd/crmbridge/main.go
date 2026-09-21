// Command crmbridge connects a shop, Binotel telephony and Nova Poshta to a
// SalesDrive CRM: orders and missed calls become deals once, callers reach
// the right manager, and parcel statuses move the funnel.
package main

import (
	"context"
	"errors"
	"flag"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/valpere/crmbridge/internal/api"
	"github.com/valpere/crmbridge/internal/config"
	"github.com/valpere/crmbridge/internal/notify"
	"github.com/valpere/crmbridge/internal/novaposhta"
	"github.com/valpere/crmbridge/internal/salesdrive"
	"github.com/valpere/crmbridge/internal/service"
	"github.com/valpere/crmbridge/internal/store"
)

func main() {
	path := flag.String("c", "examples/config.yaml", "config file")
	flag.Parse()
	log := slog.New(slog.NewTextHandler(os.Stderr, nil))
	if err := run(*path, log); err != nil {
		log.Error("fatal", "err", err)
		os.Exit(1)
	}
}

func run(path string, log *slog.Logger) error {
	cfg, err := config.Load(path)
	if err != nil {
		return err
	}
	if i := strings.LastIndex(cfg.DB, "/"); i > 0 {
		if err := os.MkdirAll(cfg.DB[:i], 0o755); err != nil {
			return err
		}
	}
	st, err := store.Open(cfg.DB)
	if err != nil {
		return err
	}
	defer st.Close()

	var n notify.Notifier = notify.Log{L: log}
	if cfg.TelegramToken != "" {
		n = notify.Telegram{Token: cfg.TelegramToken, Next: n}
	}
	svc := service.New(st,
		salesdrive.New(salesdrive.Config{BaseURL: cfg.SalesDrive.BaseURL, APIKey: cfg.SalesDrive.APIKey}),
		novaposhta.New(novaposhta.Config{BaseURL: cfg.NovaPoshta.BaseURL, APIKey: cfg.NovaPoshta.APIKey}),
		n, service.Config{
			SiteName: cfg.SalesDrive.SiteName, StatusMap: cfg.StatusMap,
			MissedDispositions: cfg.Binotel.MissedDispositions, LeadDedupe: cfg.Binotel.LeadDedupe,
			Managers: cfg.Managers, DefaultManager: cfg.DefaultManager,
			PollEvery: cfg.Tracking.PollEvery, MaxTrackAge: cfg.Tracking.MaxAge,
			MaxAttempts: cfg.Worker.MaxAttempts, BackoffBase: cfg.Worker.BackoffBase, BackoffMax: cfg.Worker.BackoffMax,
		}, log)

	if cfg.APIToken == "" || cfg.WebhookSecret == "" {
		log.Warn("api_token or webhook_secret is empty: those endpoints are unauthenticated (demo mode)")
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	go svc.Run(ctx, cfg.Worker.Tick)

	srv := &http.Server{Addr: cfg.Listen, ReadHeaderTimeout: 10 * time.Second,
		Handler: api.New(svc, api.Options{Token: cfg.APIToken, WebhookSecret: cfg.WebhookSecret}, log).Handler()}
	go func() {
		<-ctx.Done()
		sh, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		_ = srv.Shutdown(sh)
	}()
	log.Info("crmbridge listening", "addr", cfg.Listen, "salesdrive", cfg.SalesDrive.BaseURL)
	if err := srv.ListenAndServe(); !errors.Is(err, http.ErrServerClosed) {
		return err
	}
	return nil
}
