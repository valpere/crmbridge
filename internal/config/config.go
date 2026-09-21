// Package config loads the YAML configuration; ${VAR} references are
// expanded from the environment so secrets stay out of the file.
package config

import (
	"fmt"
	"os"
	"time"

	"gopkg.in/yaml.v3"
)

type Config struct {
	Listen        string `yaml:"listen"`
	DB            string `yaml:"db"`
	APIToken      string `yaml:"api_token"`      // bearer for /site/orders and /api/*
	WebhookSecret string `yaml:"webhook_secret"` // ?token= on the Binotel and SalesDrive webhooks

	SalesDrive struct {
		BaseURL  string `yaml:"base_url"`
		APIKey   string `yaml:"api_key"`
		SiteName string `yaml:"site_name"`
	} `yaml:"salesdrive"`

	NovaPoshta struct {
		BaseURL string `yaml:"base_url"`
		APIKey  string `yaml:"api_key"`
	} `yaml:"novaposhta"`

	StatusMap      map[int]int       `yaml:"np_status_map"` // Nova Poshta status code -> SalesDrive status id
	Managers       map[string]string `yaml:"managers"`      // Binotel internal number -> notify target
	DefaultManager string            `yaml:"default_manager"`
	TelegramToken  string            `yaml:"telegram_token"`

	Binotel struct {
		MissedDispositions []string      `yaml:"missed_dispositions"`
		LeadDedupe         time.Duration `yaml:"lead_dedupe"`
	} `yaml:"binotel"`

	Tracking struct {
		PollEvery time.Duration `yaml:"poll_every"`
		MaxAge    time.Duration `yaml:"max_age"`
	} `yaml:"tracking"`

	Worker struct {
		Tick        time.Duration `yaml:"tick"`
		MaxAttempts int           `yaml:"max_attempts"`
		BackoffBase time.Duration `yaml:"backoff_base"`
		BackoffMax  time.Duration `yaml:"backoff_max"`
	} `yaml:"worker"`
}

func Load(path string) (*Config, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	return Parse(raw)
}

func Parse(raw []byte) (*Config, error) {
	var c Config
	if err := yaml.Unmarshal([]byte(os.ExpandEnv(string(raw))), &c); err != nil {
		return nil, fmt.Errorf("config: %w", err)
	}
	if c.Listen == "" {
		c.Listen = ":8788"
	}
	if c.DB == "" {
		c.DB = "data/crmbridge.db"
	}
	if c.NovaPoshta.BaseURL == "" {
		c.NovaPoshta.BaseURL = "https://api.novaposhta.ua"
	}
	if c.Worker.Tick == 0 {
		c.Worker.Tick = time.Second
	}
	if c.SalesDrive.BaseURL == "" || c.SalesDrive.APIKey == "" {
		return nil, fmt.Errorf("config: salesdrive.base_url and salesdrive.api_key are required")
	}
	return &c, nil
}
