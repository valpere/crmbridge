// Package novaposhta tracks parcels through the Nova Poshta JSON API
// (POST https://api.novaposhta.ua/v2.0/json/, model TrackingDocument, method
// getStatusDocuments) and classifies status codes into delivery stages.
package novaposhta

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
	"time"
)

// Stage is a coarse delivery stage derived from a status code.
type Stage string

const (
	StageUnknown  Stage = "unknown"
	StageCreated  Stage = "created"  // 1: waiting for the sender to hand it over
	StageTransit  Stage = "transit"  // 4, 5, 6, 101: on its way
	StageArrived  Stage = "arrived"  // 7, 8: at the branch / postomat
	StageReceived Stage = "received" // 9, 10, 11
	StageRefused  Stage = "refused"  // 102-108: refusal / return
	StageDeleted  Stage = "deleted"  // 2, 3
)

// Classify maps a status code to its stage and a rank. Rank only grows along
// the normal path, so a flapping or out-of-order answer can be ignored.
func Classify(code int) (Stage, int) {
	switch {
	case code == 1:
		return StageCreated, 1
	case code == 4 || code == 5 || code == 6 || code == 101:
		return StageTransit, 2 + map[int]int{4: 0, 5: 1, 6: 2, 101: 3}[code]
	case code == 7 || code == 8:
		return StageArrived, 6
	case code == 9 || code == 10 || code == 11:
		return StageReceived, 8
	case code >= 102 && code <= 108:
		return StageRefused, 8
	case code == 2 || code == 3:
		return StageDeleted, 0
	}
	return StageUnknown, 0
}

// Final reports whether a parcel needs no more polling.
func Final(code int) bool {
	s, _ := Classify(code)
	return s == StageReceived || s == StageRefused || code == 2
}

type Tracking struct {
	Number string
	Code   int
	Status string
}

type Config struct {
	BaseURL string // https://api.novaposhta.ua
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

// BatchSize is the API limit of documents per request.
const BatchSize = 100

// flexInt reads a number that Nova Poshta sends as a string ("9") or a number.
type flexInt int

func (f *flexInt) UnmarshalJSON(b []byte) error {
	s := strings.Trim(string(b), `"`)
	if s == "" || s == "null" {
		return nil
	}
	n, err := strconv.Atoi(s)
	if err != nil {
		return err
	}
	*f = flexInt(n)
	return nil
}

// Track returns the status of up to BatchSize parcels in one request.
func (c *Client) Track(ctx context.Context, numbers []string) (map[string]Tracking, error) {
	if len(numbers) == 0 {
		return map[string]Tracking{}, nil
	}
	if len(numbers) > BatchSize {
		return nil, fmt.Errorf("novaposhta: %d documents, the limit is %d per request", len(numbers), BatchSize)
	}
	docs := make([]map[string]string, len(numbers))
	for i, n := range numbers {
		docs[i] = map[string]string{"DocumentNumber": n, "Phone": ""}
	}
	body, _ := json.Marshal(map[string]any{"apiKey": c.cfg.APIKey, "modelName": "TrackingDocument",
		"calledMethod": "getStatusDocuments", "methodProperties": map[string]any{"Documents": docs}})
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.cfg.BaseURL+"/v2.0/json/", bytes.NewReader(body))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := c.hc.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	data, _ := io.ReadAll(io.LimitReader(resp.Body, 4<<20))
	if resp.StatusCode/100 != 2 {
		return nil, &HTTPError{Status: resp.StatusCode}
	}
	var out struct {
		Success bool `json:"success"`
		Data    []struct {
			Number     string  `json:"Number"`
			StatusCode flexInt `json:"StatusCode"`
			Status     string  `json:"Status"`
		} `json:"data"`
		Errors []string `json:"errors"`
	}
	if err := json.Unmarshal(data, &out); err != nil {
		return nil, fmt.Errorf("novaposhta: bad answer: %w", err)
	}
	if !out.Success {
		return nil, errors.New("novaposhta: " + strings.Join(out.Errors, "; "))
	}
	res := make(map[string]Tracking, len(out.Data))
	for _, d := range out.Data {
		res[d.Number] = Tracking{Number: d.Number, Code: int(d.StatusCode), Status: d.Status}
	}
	return res, nil
}

type HTTPError struct{ Status int }

func (e *HTTPError) Error() string { return fmt.Sprintf("novaposhta: HTTP %d", e.Status) }
