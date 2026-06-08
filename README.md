siem-to-siems

A small tsnet service that receives events over HTTP and fans them out to configured destinations.

## Build

```bash
GO111MODULE=on go build ./cmd/siem-to-siems
```

## Run

The service listens on Tailscale tsnet and exposes a POST endpoint at `/streaming`.

```bash
SIEM_TO_SIEMS_CONFIG=/path/to/config.json ./siem-to-siems
```

Send an event:

```bash
curl -X POST --data '{"message":"hello"}' https://<tsnet-addr>/streaming
```

## Configuration

Configuration is a single JSON file (see `config.json.example`). The path is
resolved from, in order: the argument to `config.Load`, the `SIEM_TO_SIEMS_CONFIG`
environment variable, or `./config.json`.

```json
{
  "tsnet":  { "hostname": "siem-to-siems", "auth_key": "" },
  "server": { "addr": ":8080", "tls_enabled": true },
  "destinations": {
    "ndjson": { "...": "..." },
    "http":   [ { "...": "..." } ]
  }
}
```

`destinations` is the important part: every configured destination receives a
copy of every event. You can enable NDJSON, one or more HTTP forwarders, or both
at once — the service fans out to all of them concurrently and always returns
`200` to the sender regardless of downstream success (failures are logged and,
for HTTP, journaled).

At least one destination must be configured or the service exits on startup.

### Destination: NDJSON files (local logging)

Use this when you want events written to disk as newline-delimited JSON — for
archival, local inspection, or as the source for a separate log shipper
(Filebeat, Fluent Bit, Vector, etc.).

```json
"destinations": {
  "ndjson": {
    "directory": "./logs",
    "rotate": "1h"
  }
}
```

| Field       | Meaning                                                                 |
|-------------|-------------------------------------------------------------------------|
| `directory` | Directory for output files. Created if missing.                         |
| `rotate`    | Rotation interval as a Go duration (`"10m"`, `"1h"`, `"24h"`). Default `1h`. |

Behavior:

- Each event is written verbatim as one line followed by `\n`.
- Files are named `events-<UTC-timestamp>.ndjson` (e.g. `events-20260608-093010.ndjson`).
- Rotation is driven by a background ticker, so files roll over on schedule even
  when no events are arriving. To avoid a flood of empty files, a rotation only
  happens when at least one event was written since the last roll.
- Writes are buffered by the OS (no `fsync` per line for throughput); the current
  file is synced and closed on shutdown.

Omit the `ndjson` block entirely to disable file logging.

### Destination: HTTP forwarders (other SIEMs)

Use this to forward events to another SIEM or HTTP ingest endpoint — e.g. Splunk
HEC, Elastic, a Logstash/Vector HTTP input, an OpenTelemetry collector, or
another `siem-to-siems` instance. `http` is a list, so you can fan out to several
SIEMs simultaneously.

```json
"destinations": {
  "http": [
    {
      "url": "https://splunk.example.com:8088/services/collector/raw",
      "token": "00000000-0000-0000-0000-000000000000",
      "journal_directory": "/var/lib/siem-to-siems/splunk-journal",
      "initial_backoff": "1s",
      "max_backoff": "1m"
    },
    {
      "url": "https://elastic.example.com/_bulk",
      "token": "",
      "initial_backoff": "2s",
      "max_backoff": "5m"
    }
  ]
}
```

| Field               | Meaning                                                                          |
|---------------------|----------------------------------------------------------------------------------|
| `url`               | Destination endpoint. Events are POSTed with `Content-Type: application/json`.    |
| `token`             | Optional bearer token; sent as `Authorization: Bearer <token>` when non-empty.    |
| `journal_directory` | Where failed events are journaled. Defaults to a temp dir if empty.               |
| `initial_backoff`   | First retry delay (Go duration). Default `1s`.                                    |
| `max_backoff`       | Cap on the exponential backoff between retries. Default `1m`.                     |

Delivery semantics:

- Events are delivered **in order** per forwarder; a failing event is retried with
  exponential backoff before later events are sent.
- On the first failure, an event is written to the journal (`failed-<date>.ndjson`)
  so it survives restarts; the journal is replayed on startup.
- The request body is the raw event bytes as received — `siem-to-siems` does not
  reshape payloads, so format events to suit the target SIEM upstream (or point a
  forwarder at a collector that does the transformation).

Omit the `http` list (or leave it empty) to disable forwarding.

### Destination: Parquet (Tailscale netlog → columnar files)

This destination is a port of the standalone `siem-to-parquet` service. It expects
events in **Splunk-HEC stream form** — a sequence of JSON objects shaped like
`{"time": ..., "event": {...}, "fields": {...}}` — where `event` is a Tailscale
network-flow log or a configuration-audit log. It parses them into columnar
**parquet** files (ZSTD-compressed) with the same schemas, file names, and rollup
cadence as `siem-to-parquet`.

```json
"destinations": {
  "parquet": {
    "directory": "./parquet",
    "rotate": "5m",
    "journal": "5m",
    "daily_merge": "24h",
    "ndjson_files": false
  }
}
```

| Field          | Meaning                                                                                              |
|----------------|------------------------------------------------------------------------------------------------------|
| `directory`    | Output directory for parquet files. Created if missing.                                              |
| `rotate`       | How often journal files are rolled up into final parquet files (default `5m`).                       |
| `journal`      | How often buffered rows are flushed to small journal files; only used when `rotate` > `journal`. Minimum `1m`, clamped to `rotate` (default `5m`). |
| `daily_merge`  | How often per-rotation files are consolidated into daily files. Minimum `1h`; set `"0"` to disable (default `24h`). |
| `ndjson_files` | Also write the raw netlog events as `network_<ts>.ndjson` alongside the parquet files.               |

Output files (timestamps are `YYYYMMDD_HHMMSS`):

- Network flows → `structured_network_journal_*.parquet` (journals) → rolled up to
  `structured_events_*.parquet` → consolidated to `structured_events_daily_*.parquet`.
- Configuration-audit events → `configuration_logs_journal_*.parquet` → `configuration_logs_*.parquet`
  → `configuration_logs_daily_*.parquet`.

Rows are buffered in memory and flushed on the `journal`/`rotate` tickers and on
shutdown; merges use a temp file + atomic rename. Because the parquet sink expects
the netlog/HEC event shape, point Tailscale network-flow logging (or another
`siem-to-parquet`-compatible producer) at this service's `/streaming` endpoint.

> This destination is for the Tailscale netlog schema specifically — for arbitrary
> JSON events use the `ndjson` or `http` destinations instead.

### Logging to NDJSON *and* other SIEMs at once

Combine both blocks to keep a local archive while forwarding live to one or more
SIEMs:

```json
"destinations": {
  "ndjson": { "directory": "/var/log/siem-to-siems", "rotate": "1h" },
  "http": [
    { "url": "https://splunk.example.com:8088/services/collector/raw", "token": "..." }
  ]
}
```

## Docker

A multi-stage `Dockerfile` is provided for non-Nix users: it builds with the
official `golang` image and ships the static binary on a minimal `distroless`
runtime (no shell, nonroot, ~27 MB total).

```bash
docker build -t siem-to-siems .

docker run --rm \
  -p 443:443 \
  -e TS_AUTHKEY=tskey-auth-... \
  -v "$PWD/config.json:/config.json:ro" \
  -v siem-data:/data \
  siem-to-siems
```

- The service reads its config from `/config.json` (`SIEM_TO_SIEMS_CONFIG`); mount
  your file there (see `config.json.example`).
- `TS_AUTHKEY` is used for first-time Tailscale registration when `tsnet.auth_key`
  in the config is empty — keep the key in the environment, not the config.
- `/data` holds tsnet node state (`$HOME/.config`) and any relative output paths
  from the config (`./logs`, `./parquet`); persist it with a volume so the node
  isn't re-registered on each restart.

Or with the bundled `docker-compose.yml` (set `TS_AUTHKEY` in your shell or a
`.env` file):

```bash
docker compose up -d
```

## Nix

A flake is provided:

```bash
nix build .#siem-to-siems        # build the binary -> ./result/bin/siem-to-siems
nix run  .#siem-to-siems         # build and run
nix develop                      # dev shell with Go + gopls + staticcheck
```

### NixOS module

The flake exports a NixOS module (`nixosModules.siem-to-siems`, also `.default`) that
runs the receiver as a hardened systemd service. It renders `config.json` from a
freeform `settings` attribute (mapped 1:1 to the JSON schema above) and points the
service at it via `SIEM_TO_SIEMS_CONFIG`.

```nix
{
  inputs.siem-to-siems.url = "github:scottjab/siem-to-siems";

  outputs = { nixpkgs, siem-to-siems, ... }: {
    nixosConfigurations.myhost = nixpkgs.lib.nixosSystem {
      modules = [
        siem-to-siems.nixosModules.default
        {
          services.siem-to-siems = {
            enable = true;
            openFirewall = true;
            # Secrets stay out of the Nix store; tsnet reads TS_AUTHKEY when
            # tsnet.auth_key is unset.
            environmentFile = "/run/secrets/siem-to-siems.env"; # TS_AUTHKEY=tskey-...
            settings = {
              tsnet.hostname = "siem-to-siems";
              server = { addr = ":443"; tls_enabled = true; };
              destinations = {
                ndjson = { directory = "/var/lib/siem-to-siems/logs"; rotate = "1h"; };
                parquet = { directory = "/var/lib/siem-to-siems/parquet"; rotate = "5m"; daily_merge = "24h"; };
                http = [ { url = "https://splunk.example.com:8088/services/collector/raw"; } ];
              };
            };
          };
        }
      ];
    };
  };
}
```

Notes:

- `settings` keys are written verbatim to JSON, so use the schema names exactly
  (`tls_enabled`, `daily_merge`, `ndjson_files`, `journal_directory`, …).
- The service runs under a `DynamicUser` with `StateDirectory`/`HOME` at
  `stateDir` (default `/var/lib/siem-to-siems`); tsnet state and relative output
  paths land there. It is granted `CAP_NET_BIND_SERVICE` for binding `:443`.
- `openFirewall` opens the port parsed from `settings.server.addr` (default 443).
- Put **no secrets** in `settings` — the rendered file is world-readable in the
  store. Use `environmentFile` for the Tailscale auth key and any HTTP tokens you
  can supply via the environment.

## Requirements

- Go 1.26+ (or use the provided Nix flake)
- Tailscale auth key if using `TSNet.AuthKey`


