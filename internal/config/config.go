package config

import (
	"encoding/json"
	"fmt"
	"os"
	"time"
)

type Config struct {
	TSNet        TSNetConfig        `json:"tsnet"`
	Server       ServerConfig       `json:"server"`
	Destinations DestinationsConfig `json:"destinations"`
}

type TSNetConfig struct {
	Hostname string `json:"hostname"`
	AuthKey  string `json:"auth_key"`
}

type ServerConfig struct {
	Addr string `json:"addr"`
	// TLSEnabled controls whether the server listens with TLS via tsnet.ListenTLS.
	// Defaults to true if not specified in config.
	TLSEnabled *bool `json:"tls_enabled,omitempty"`
}

type DestinationsConfig struct {
	NDJSON  *NDJSONConfig           `json:"ndjson"`
	HTTP    []HTTPDestinationConfig `json:"http"`
	Parquet *ParquetConfig          `json:"parquet"`
}

type NDJSONConfig struct {
	Directory string `json:"directory"`
	Rotate    string `json:"rotate"` // duration string (e.g., "10m", "1h")
}

// ParquetConfig configures the parquet destination, which parses Splunk-HEC-style
// netlog event streams into structured parquet files. Durations mirror siem-to-parquet.
type ParquetConfig struct {
	Directory   string `json:"directory"`    // output dir for parquet (and optional ndjson) files
	Rotate      string `json:"rotate"`       // rollup interval, e.g. "5m", "1h" (default 5m)
	Journal     string `json:"journal"`      // journal flush interval, >=1m, used when rotate>journal (default 5m)
	DailyMerge  string `json:"daily_merge"`  // daily consolidation interval, >=1h, "0" disables (default 24h)
	NDJSONFiles bool   `json:"ndjson_files"` // also write raw events as network_<ts>.ndjson
}

type HTTPDestinationConfig struct {
	URL              string `json:"url"`
	Token            string `json:"token"`
	JournalDirectory string `json:"journal_directory"`
	InitialBackoff   string `json:"initial_backoff"`
	MaxBackoff       string `json:"max_backoff"`
}

func Load(path string) (*Config, error) {
	if path == "" {
		// default to env var or local file
		if v := os.Getenv("SIEM_TO_SIEMS_CONFIG"); v != "" {
			path = v
		} else {
			path = "config.json"
		}
	}
	b, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read config: %w", err)
	}
	var c Config
	if err := json.Unmarshal(b, &c); err != nil {
		return nil, fmt.Errorf("parse config: %w", err)
	}
	// defaults
	if c.Server.TLSEnabled == nil {
		// default TLS on
		t := true
		c.Server.TLSEnabled = &t
	}
	if c.Server.Addr == "" {
		if c.Server.TLSEnabled != nil && *c.Server.TLSEnabled {
			c.Server.Addr = ":443"
		} else {
			c.Server.Addr = ":80"
		}
	}
	if c.TSNet.Hostname == "" {
		c.TSNet.Hostname = "siem-to-siems"
	}
	return &c, nil
}

func ParseDurationOr(durStr string, def time.Duration) time.Duration {
	if durStr == "" {
		return def
	}
	d, err := time.ParseDuration(durStr)
	if err != nil {
		return def
	}
	return d
}
