# ── Build stage ──────────────────────────────────────────────────────────────
FROM golang:1.26-alpine AS builder
ARG VERSION=dev
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build \
    -trimpath -ldflags="-s -w -X github.com/fyaz05/ThunderGo/internal/http.Version=${VERSION}" \
    -o /out/thundergo ./cmd/thundergo

# ── Certs stage ──────────────────────────────────────────────────────────────
FROM alpine:3.24 AS certs
RUN apk add --no-cache ca-certificates

# ── Dirs stage ───────────────────────────────────────────────────────────────
FROM alpine:3.24 AS dirs
RUN mkdir -p /app/data && chown -R 65532:65532 /app/data

# ── Runtime stage ────────────────────────────────────────────────────────────
FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=certs /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/ca-certificates.crt
COPY --from=builder /out/thundergo /app/thundergo
COPY --from=dirs --chown=65532:65532 /app/data /app/data

WORKDIR /app
USER nonroot:nonroot
VOLUME ["/app/data"]
ENV TG_DATA_DIR=/app/data
ENV TG_LOG_FILE=/app/data/thundergo.log
EXPOSE 8080
HEALTHCHECK --interval=30s --timeout=5s --start-period=60s --retries=3 \
    CMD ["/app/thundergo", "-healthcheck"]
ENTRYPOINT ["/app/thundergo"]
