package config

import (
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestParseDurationOr(t *testing.T) {
	cases := []struct {
		in   string
		def  time.Duration
		want time.Duration
	}{
		{"", time.Hour, time.Hour},
		{"10m", time.Hour, 10 * time.Minute},
		{"not-a-duration", time.Hour, time.Hour},
		{"0", time.Hour, 0},
		{"90s", time.Minute, 90 * time.Second},
	}
	for _, c := range cases {
		if got := ParseDurationOr(c.in, c.def); got != c.want {
			t.Errorf("ParseDurationOr(%q, %v) = %v, want %v", c.in, c.def, got, c.want)
		}
	}
}

func writeConfig(t *testing.T, body string) string {
	t.Helper()
	p := filepath.Join(t.TempDir(), "config.json")
	if err := os.WriteFile(p, []byte(body), 0o644); err != nil {
		t.Fatal(err)
	}
	return p
}

func TestLoadDefaults(t *testing.T) {
	c, err := Load(writeConfig(t, `{"destinations":{}}`))
	if err != nil {
		t.Fatal(err)
	}
	if c.Server.TLSEnabled == nil || !*c.Server.TLSEnabled {
		t.Error("TLS should default on")
	}
	if c.Server.Addr != ":443" {
		t.Errorf("addr = %q, want :443", c.Server.Addr)
	}
	if c.TSNet.Hostname != "siem-to-siems" {
		t.Errorf("hostname = %q, want siem-to-siems", c.TSNet.Hostname)
	}
}

func TestLoadTLSDisabledDefaultsPort80(t *testing.T) {
	c, err := Load(writeConfig(t, `{"server":{"tls_enabled":false}}`))
	if err != nil {
		t.Fatal(err)
	}
	if c.Server.Addr != ":80" {
		t.Errorf("addr = %q, want :80", c.Server.Addr)
	}
}

func TestLoadParsesDestinations(t *testing.T) {
	c, err := Load(writeConfig(t, `{
		"server": {"addr": ":8080"},
		"destinations": {
			"ndjson": {"directory": "./logs", "rotate": "30m"},
			"parquet": {"directory": "./pq", "rotate": "5m", "daily_merge": "12h", "ndjson_files": true},
			"http": [{"url": "https://x/y", "token": "t"}]
		}
	}`))
	if err != nil {
		t.Fatal(err)
	}
	if c.Server.Addr != ":8080" {
		t.Errorf("addr = %q", c.Server.Addr)
	}
	if c.Destinations.NDJSON == nil || c.Destinations.NDJSON.Rotate != "30m" {
		t.Errorf("ndjson = %+v", c.Destinations.NDJSON)
	}
	if c.Destinations.Parquet == nil || c.Destinations.Parquet.Directory != "./pq" ||
		c.Destinations.Parquet.DailyMerge != "12h" || !c.Destinations.Parquet.NDJSONFiles {
		t.Errorf("parquet = %+v", c.Destinations.Parquet)
	}
	if len(c.Destinations.HTTP) != 1 || c.Destinations.HTTP[0].URL != "https://x/y" {
		t.Errorf("http = %+v", c.Destinations.HTTP)
	}
}

func TestLoadFromEnv(t *testing.T) {
	p := writeConfig(t, `{"tsnet":{"hostname":"from-env"}}`)
	t.Setenv("SIEM_TO_SIEMS_CONFIG", p)
	c, err := Load("")
	if err != nil {
		t.Fatal(err)
	}
	if c.TSNet.Hostname != "from-env" {
		t.Errorf("hostname = %q, want from-env", c.TSNet.Hostname)
	}
}

func TestLoadMissingFile(t *testing.T) {
	if _, err := Load(filepath.Join(t.TempDir(), "nope.json")); err == nil {
		t.Error("expected error for missing file")
	}
}

func TestLoadInvalidJSON(t *testing.T) {
	if _, err := Load(writeConfig(t, `{not valid json`)); err == nil {
		t.Error("expected parse error for invalid JSON")
	}
}
