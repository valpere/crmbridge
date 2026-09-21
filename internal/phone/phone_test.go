package phone_test

import (
	"testing"

	"github.com/valpere/crmbridge/internal/phone"
)

func TestNormalize(t *testing.T) {
	for in, want := range map[string]string{
		"+380 (50) 123-45-67": "380501234567",
		"380501234567":        "380501234567",
		"0501234567":          "380501234567",
		"80501234567":         "380501234567",
		"501234567":           "380501234567",
		"050 123 45 67":       "380501234567",
		"":                    "",
		"12345":               "",
		"+48 501 234 567":     "",
		"abc":                 "",
	} {
		if got := phone.Normalize(in); got != want {
			t.Errorf("Normalize(%q) = %q, want %q", in, got, want)
		}
	}
}
