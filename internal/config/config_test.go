package config_test

import (
	"testing"
	"time"

	"github.com/valpere/crmbridge/internal/config"
)

func TestParse(t *testing.T) {
	t.Setenv("SD_KEY", "k123")
	c, err := config.Parse([]byte(`
salesdrive: {base_url: "https://acme.salesdrive.me", api_key: "${SD_KEY}"}
np_status_map: {7: 21, 9: 22}
tracking: {poll_every: 5m}
`))
	if err != nil {
		t.Fatal(err)
	}
	if c.SalesDrive.APIKey != "k123" || c.StatusMap[7] != 21 || c.Tracking.PollEvery != 5*time.Minute ||
		c.Listen != ":8788" || c.NovaPoshta.BaseURL != "https://api.novaposhta.ua" {
		t.Fatalf("%+v", c)
	}
	if _, err := config.Parse([]byte("listen: ':1'")); err == nil {
		t.Fatal("missing SalesDrive settings must be an error")
	}
}
