package binotel_test

import (
	"testing"

	"github.com/valpere/crmbridge/internal/binotel"
)

func TestParseShapes(t *testing.T) {
	want := binotel.Event{RequestType: "apiCallCompleted", GeneralCallID: "777", Incoming: true,
		ExternalNumber: "0501234567", InternalNumber: "101", Disposition: "NOANSWER"}
	cases := map[string]struct{ ct, body string }{
		"json":         {"application/json", `{"requestType":"apiCallCompleted","callDetails":{"generalCallID":777,"callType":"0","externalNumber":"0501234567","internalNumber":"101","disposition":"NOANSWER"}}`},
		"form json":    {"application/x-www-form-urlencoded", `requestType=apiCallCompleted&callDetails=%7B%22generalCallID%22%3A%22777%22%2C%22callType%22%3A%220%22%2C%22externalNumber%22%3A%220501234567%22%2C%22internalNumber%22%3A%22101%22%2C%22disposition%22%3A%22NOANSWER%22%7D`},
		"form bracket": {"application/x-www-form-urlencoded", `requestType=apiCallCompleted&callDetails%5BgeneralCallID%5D=777&callDetails%5BcallType%5D=0&callDetails%5BexternalNumber%5D=0501234567&callDetails%5BinternalNumber%5D=101&callDetails%5Bdisposition%5D=NOANSWER`},
	}
	for name, c := range cases {
		got, err := binotel.Parse(c.ct, []byte(c.body))
		if err != nil || got != want {
			t.Errorf("%s: %+v %v", name, got, err)
		}
	}
}

func TestParseRejectsIncomplete(t *testing.T) {
	for _, b := range []string{`{}`, `{"requestType":"x"}`, `{"callDetails":{"generalCallID":"1"}}`, `not json`} {
		if _, err := binotel.Parse("application/json", []byte(b)); err == nil {
			t.Errorf("%q must be rejected", b)
		}
	}
	if e, _ := binotel.Parse("application/json", []byte(`{"requestType":"x","callDetails":{"generalCallID":"1","callType":"1"}}`)); e.Incoming {
		t.Error("callType 1 is outgoing")
	}
}
