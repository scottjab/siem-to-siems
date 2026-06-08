# syntax=docker/dockerfile:1

# ---- Build stage: official upstream Go toolchain ----
FROM golang:1.26 AS build

WORKDIR /src

# Download modules first so this layer is cached unless go.mod/go.sum change.
COPY go.mod go.sum ./
RUN go mod download

# Build a fully static binary (CGO off) so it runs on a scratch/distroless image.
COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -trimpath -ldflags="-s -w" \
        -o /out/siem-to-siems ./cmd/siem-to-siems

# Pre-create a data dir owned by the nonroot runtime uid (distroless has no shell to chown later).
RUN mkdir -p /data

# ---- Runtime stage: minimal distroless image (no shell, nonroot, CA certs included) ----
FROM gcr.io/distroless/static-debian12:nonroot

COPY --from=build /out/siem-to-siems /usr/local/bin/siem-to-siems
COPY --from=build --chown=65532:65532 /data /data

# tsnet node state ($HOME/.config) and relative output dirs both live under /data,
# so a single volume persists everything across restarts.
ENV HOME=/data \
    SIEM_TO_SIEMS_CONFIG=/config.json
WORKDIR /data
VOLUME ["/data"]

# tsnet's ListenTLS binds :443 by default (override via server.addr in the config).
EXPOSE 443

ENTRYPOINT ["/usr/local/bin/siem-to-siems"]
