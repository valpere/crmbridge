// Package salesdrive is a small client for the SalesDrive CRM REST API
// (https://api.salesdrive.me/api/docs/): create an order/lead, update its
// status, add a note, look an order up by external id, and find the manager
// and client behind a phone number. Field names follow the published spec.
package salesdrive

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"time"
)

type APIError struct {
	Status  int
	Message string
}

func (e *APIError) Error() string { return fmt.Sprintf("salesdrive: HTTP %d: %s", e.Status, e.Message) }

// Retryable: server errors, throttling (the API limits requests per minute)
// and network failures may succeed later; other 4xx answers will not.
func Retryable(err error) bool {
	var ae *APIError
	if errors.As(err, &ae) {
		return ae.Status >= 500 || ae.Status == http.StatusTooManyRequests || ae.Status == http.StatusRequestTimeout
	}
	return err != nil && !errors.Is(err, context.Canceled)
}

type Config struct {
	BaseURL string // https://<account>.salesdrive.me
	APIKey  string
	HTTP    *http.Client
}

type Client struct {
	cfg Config
	hc  *http.Client
}

func New(cfg Config) *Client {
	hc := cfg.HTTP
	if hc == nil {
		hc = &http.Client{Timeout: 20 * time.Second}
	}
	return &Client{cfg: cfg, hc: hc}
}

type Product struct {
	ID          string  `json:"id,omitempty"`
	Name        string  `json:"name"`
	SKU         string  `json:"sku,omitempty"`
	CostPerItem float64 `json:"costPerItem"` // UAH
	Amount      float64 `json:"amount"`
}

// NovaPoshta is the delivery block of an order.
type NovaPoshta struct {
	ServiceType     string `json:"ServiceType,omitempty"`
	City            string `json:"city,omitempty"`
	WarehouseNumber string `json:"WarehouseNumber,omitempty"`
	TTN             string `json:"ttn,omitempty"`
}

// NewOrder is the body of POST /handler/.
type NewOrder struct {
	FName           string      `json:"fName,omitempty"`
	LName           string      `json:"lName,omitempty"`
	Phone           string      `json:"phone,omitempty"`
	Email           string      `json:"email,omitempty"`
	Products        []Product   `json:"products,omitempty"`
	PaymentMethod   string      `json:"payment_method,omitempty"`
	ShippingMethod  string      `json:"shipping_method,omitempty"`
	ShippingAddress string      `json:"shipping_address,omitempty"`
	Comment         string      `json:"comment,omitempty"`
	ExternalID      string      `json:"externalId,omitempty"`
	Site            string      `json:"sajt,omitempty"`
	NovaPoshta      *NovaPoshta `json:"novaposhta,omitempty"`
	ConTelegram     string      `json:"con_telegram,omitempty"`
	UTMSource       string      `json:"utmSource,omitempty"`
	UTMMedium       string      `json:"utmMedium,omitempty"`
	UTMCampaign     string      `json:"utmCampaign,omitempty"`
}

// Ref identifies an order by SalesDrive id or by our external id.
type Ref struct {
	ID         int64
	ExternalID string
}

func (c *Client) do(ctx context.Context, method, path string, q url.Values, in, out any) error {
	var rd io.Reader
	if in != nil {
		b, err := json.Marshal(in)
		if err != nil {
			return err
		}
		rd = bytes.NewReader(b)
	}
	u := c.cfg.BaseURL + path
	if len(q) > 0 {
		u += "?" + q.Encode()
	}
	req, err := http.NewRequestWithContext(ctx, method, u, rd)
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-Api-Key", c.cfg.APIKey)
	resp, err := c.hc.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	data, _ := io.ReadAll(io.LimitReader(resp.Body, 4<<20))
	if resp.StatusCode/100 != 2 {
		return &APIError{Status: resp.StatusCode, Message: message(data)}
	}
	if out != nil && len(data) > 0 {
		return json.Unmarshal(data, out)
	}
	return nil
}

func message(data []byte) string {
	var v struct {
		Message string `json:"message"`
	}
	if json.Unmarshal(data, &v) == nil && v.Message != "" {
		return v.Message
	}
	if len(data) > 200 {
		data = data[:200]
	}
	return string(data)
}

// CreateOrder posts a new order or lead and returns its SalesDrive id.
func (c *Client) CreateOrder(ctx context.Context, o NewOrder) (int64, error) {
	body := struct {
		NewOrder
		GetResultData int `json:"getResultData"`
	}{o, 1}
	var out struct {
		Success bool `json:"success"`
		Data    struct {
			OrderID int64 `json:"orderId"`
		} `json:"data"`
		Message string `json:"message"`
	}
	if err := c.do(ctx, http.MethodPost, "/handler/", nil, body, &out); err != nil {
		return 0, err
	}
	if !out.Success || out.Data.OrderID == 0 {
		return 0, &APIError{Status: http.StatusBadGateway, Message: "no order id in answer: " + out.Message}
	}
	return out.Data.OrderID, nil
}

// FindByExternalID looks an order up by the external id we gave it. It is
// what makes retrying CreateOrder safe when an answer was lost.
func (c *Client) FindByExternalID(ctx context.Context, ext string) (int64, bool, error) {
	var out struct {
		Data []struct {
			ID int64 `json:"id"`
		} `json:"data"`
	}
	q := url.Values{"filter[externalId]": {ext}, "limit": {"1"}}
	if err := c.do(ctx, http.MethodGet, "/api/order/list/", q, nil, &out); err != nil {
		return 0, false, err
	}
	if len(out.Data) == 0 {
		return 0, false, nil
	}
	return out.Data[0].ID, true, nil
}

// UpdateStatus moves an order to another funnel status.
func (c *Client) UpdateStatus(ctx context.Context, ref Ref, statusID int) error {
	body := map[string]any{"data": map[string]any{"statusId": statusID}}
	if ref.ID != 0 {
		body["id"] = ref.ID
	} else {
		body["externalId"] = ref.ExternalID
	}
	return c.do(ctx, http.MethodPost, "/api/order/update/", nil, body, nil)
}

// AddNote appends a note to the "communication with the client" block.
func (c *Client) AddNote(ctx context.Context, orderID int64, text string) error {
	return c.do(ctx, http.MethodPost, "/api/order/note/", nil, map[string]any{"orderId": orderID, "text": text}, nil)
}

// Contact is the client and manager behind a phone number.
type Contact struct {
	ManagerName    string
	InternalNumber string
	ClientName     string
	ClientCompany  string
	KnownClient    bool
}

// ManagerByPhone answers who owns a caller: the basis of a screen pop.
// A nil Contact means the number is unknown to the CRM.
func (c *Client) ManagerByPhone(ctx context.Context, phone string) (*Contact, error) {
	var out struct {
		Status  string `json:"status"`
		Manager struct {
			Name           string `json:"name"`
			InternalNumber string `json:"internal_number"`
		} `json:"manager"`
		Client struct {
			ID      int64  `json:"id"`
			FName   string `json:"fName"`
			LName   string `json:"lName"`
			Company string `json:"company"`
		} `json:"client"`
	}
	if err := c.do(ctx, http.MethodGet, "/api/get_manager_by_phone_number/", url.Values{"phone": {phone}}, nil, &out); err != nil {
		return nil, err
	}
	if out.Status != "success" {
		return nil, nil
	}
	name := out.Client.FName
	if out.Client.LName != "" {
		name += " " + out.Client.LName
	}
	return &Contact{ManagerName: out.Manager.Name, InternalNumber: out.Manager.InternalNumber,
		ClientName: name, ClientCompany: out.Client.Company, KnownClient: out.Client.ID != 0}, nil
}
