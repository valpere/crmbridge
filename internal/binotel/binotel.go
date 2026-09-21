// Package binotel parses Binotel call-event webhooks (requestType
// receivedTheCall, apiCallCompleted, ...) into one Event. The payload is
// accepted as JSON or as form data whose callDetails is either a JSON string
// or bracketed keys (callDetails[externalNumber]).
//
// The field names follow Binotel's public API description as far as it could
// be checked; Binotel's own developer site was unreachable while this was
// written, so verify them against your account before production.
package binotel

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
	"strings"
)

type Event struct {
	RequestType    string
	GeneralCallID  string
	Incoming       bool
	ExternalNumber string // the customer
	InternalNumber string // the employee line
	Disposition    string // ANSWER, NOANSWER, BUSY, CANCEL, ...
}

func str(v any) string {
	switch t := v.(type) {
	case nil:
		return ""
	case string:
		return t
	case float64:
		return fmt.Sprintf("%.0f", t)
	default:
		return fmt.Sprint(t)
	}
}

func fromDetails(rt string, d map[string]any) Event {
	return Event{RequestType: rt, GeneralCallID: str(d["generalCallID"]), Incoming: str(d["callType"]) == "0",
		ExternalNumber: str(d["externalNumber"]), InternalNumber: str(d["internalNumber"]), Disposition: str(d["disposition"])}
}

// Parse decodes a webhook body of the given content type.
func Parse(contentType string, body []byte) (Event, error) {
	if strings.Contains(contentType, "json") {
		var v struct {
			RequestType string         `json:"requestType"`
			CallDetails map[string]any `json:"callDetails"`
		}
		if err := json.Unmarshal(body, &v); err != nil {
			return Event{}, fmt.Errorf("binotel: %w", err)
		}
		return check(fromDetails(v.RequestType, v.CallDetails))
	}
	q, err := url.ParseQuery(string(body))
	if err != nil {
		return Event{}, fmt.Errorf("binotel: %w", err)
	}
	d := map[string]any{}
	if raw := q.Get("callDetails"); raw != "" {
		if err := json.Unmarshal([]byte(raw), &d); err != nil {
			return Event{}, fmt.Errorf("binotel: callDetails: %w", err)
		}
	}
	for k, v := range q {
		if f, ok := strings.CutPrefix(k, "callDetails["); ok && len(v) > 0 {
			d[strings.TrimSuffix(f, "]")] = v[0]
		}
	}
	return check(fromDetails(q.Get("requestType"), d))
}

func check(e Event) (Event, error) {
	if e.RequestType == "" || e.GeneralCallID == "" {
		return Event{}, errors.New("binotel: requestType and callDetails.generalCallID are required")
	}
	return e, nil
}
