// Package notify delivers short messages to managers and customers.
package notify

import (
	"context"
	"fmt"
	"log/slog"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"
)

// Notifier sends text to a target: "telegram:<chat id>" or anything else
// (which is only logged).
type Notifier interface {
	Send(ctx context.Context, to, text string) error
}

type Log struct{ L *slog.Logger }

func (n Log) Send(_ context.Context, to, text string) error {
	n.L.Info("notify", "to", to, "text", text)
	return nil
}

type Telegram struct {
	Token string
	API   string // default https://api.telegram.org
	HTTP  *http.Client
	Next  Notifier
}

func (n Telegram) Send(ctx context.Context, to, text string) error {
	chat, ok := strings.CutPrefix(to, "telegram:")
	if _, err := strconv.ParseInt(chat, 10, 64); !ok || err != nil {
		return n.Next.Send(ctx, to, text)
	}
	api := n.API
	if api == "" {
		api = "https://api.telegram.org"
	}
	hc := n.HTTP
	if hc == nil {
		hc = &http.Client{Timeout: 10 * time.Second}
	}
	form := url.Values{"chat_id": {chat}, "text": {text}}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, api+"/bot"+n.Token+"/sendMessage", strings.NewReader(form.Encode()))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	resp, err := hc.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("telegram: HTTP %d", resp.StatusCode)
	}
	return nil
}
