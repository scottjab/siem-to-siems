siem-to-siems

A small service that receives events over HTTP and fans them out to configured destinations.

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

- See `config.json.example` for structure.
- Destinations:
  - NDJSON file writer with rotation
  - HTTP forwarders with retry and journaling

## Requirements

- Go 1.25+
- Tailscale auth key if using `TSNet.AuthKey`


