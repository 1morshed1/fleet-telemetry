# Multi-stage build producing one image that contains all four service binaries.
# The processor depends on uber/h3-go, which uses cgo, so CGO must be enabled and
# the runtime image must provide libc — hence debian-slim rather than scratch.

FROM golang:1.25-bookworm AS build
WORKDIR /src

# Cache module downloads across builds.
COPY go.mod go.sum ./
RUN go mod download

COPY . .
ENV CGO_ENABLED=1
RUN go build -trimpath -o /out/simulator ./cmd/simulator \
 && go build -trimpath -o /out/ingestion ./cmd/ingestion \
 && go build -trimpath -o /out/processor ./cmd/processor \
 && go build -trimpath -o /out/dashboard ./cmd/dashboard

FROM debian:bookworm-slim
RUN apt-get update \
 && apt-get install -y --no-install-recommends ca-certificates \
 && rm -rf /var/lib/apt/lists/*
COPY --from=build /out/ /usr/local/bin/
COPY web/ /app/web/
WORKDIR /app
# The compose file selects which binary to run via `command:`.
