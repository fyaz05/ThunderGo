# HTTP API Reference

ThunderGo's HTTP gateway: file streaming, the HTML player page, the activation redirect, and health/status JSON. For architecture see [ARCHITECTURE.md](ARCHITECTURE.md); for deployment see [DEPLOYMENT.md](DEPLOYMENT.md); for env vars see [CONFIGURATION.md](CONFIGURATION.md).

> **Terminology:** the `{token}` URL parameter in `/f/{token}/...` is a legacy chi-router name. The value is a deterministic **file hash** (128-bit SHA-256 prefix, 32 hex chars). Activation tokens (in `/activate/{token}`) are different — 128-bit Crockford-base32 strings, 26 chars.

---

## Base URL

Whatever you set `TG_URL` to (e.g. `https://bot.example.com`). The gateway listens on `TG_BIND_ADDRESS:TG_HTTP_PORT` (default `0.0.0.0` + port auto-derived from `TG_URL`: 443 for `https`, 8080 for `http`, or the explicit URL port). The docker-compose setup publishes `127.0.0.1:8080:8080` (loopback only) — pair with a reverse proxy for public HTTPS.

---

## Routes

| Method | Path | Description |
|---|---|---|
| `GET` | `/` | 302 redirect to GitHub |
| `GET` | `/health` | Lightweight liveness probe: `{"status":"ok"}` |
| `GET` | `/status` | JSON status (version, uptime, pool/client metrics) |
| `GET` | `/activate/{token}` | 302 redirect to `tg://<bot>?start={token}` (does NOT consume token — that happens in `/start`) |
| `GET` | `/f/{hash}/{filename}` | HTML player page |
| `GET` | `/f/{hash}/{filename}/raw` | Raw file stream with Range support |
| `HEAD` | `/f/{hash}/{filename}` | Headers only (player page) |
| `HEAD` | `/f/{hash}/{filename}/raw` | Headers only (raw stream) |
| `OPTIONS` | `*` | CORS preflight (always 200) |

### Query parameters

- `GET /f/{hash}/{filename}/raw?disposition=inline` — set `Content-Disposition: inline` (browser plays inline). Anything else (including omitted) → `Content-Disposition: attachment` (browser downloads).
- `Range: bytes=N-M` header on `GET/HEAD /f/{hash}/{filename}/raw` — request a byte range (RFC 7233). Multi-range coalesced to the first range.

### Examples

```bash
# Health check
curl -s https://bot.example.com/health
# {"status":"ok"}

# Status
curl -s https://bot.example.com/status | jq .

# Player page
curl -sI https://bot.example.com/f/a1b2c3d4e5f6a7b8c9d0e1f2a3b4c5d6/movie.mp4
# HTTP/2 200  content-type: text/html; charset=utf-8

# Full download
curl -s -o movie.mp4 https://bot.example.com/f/a1b2c3d4e5f6a7b8c9d0e1f2a3b4c5d6/movie.mp4/raw

# Inline disposition (browser plays)
curl -sI "https://bot.example.com/f/a1b2c3d4e5f6a7b8c9d0e1f2a3b4c5d6/movie.mp4/raw?disposition=inline"

# Activation link (302 to tg:// deep link)
curl -sI https://bot.example.com/activate/ab1cdefghjkmnpqrs2tvwxyz23
# HTTP/2 302  location: tg://MyThunderGoBot?start=ab1cdefghjkmnpqrs2tvwxyz23

# HEAD: get size without downloading
curl -sI -X HEAD https://bot.example.com/f/a1b2c3d4e5f6a7b8c9d0e1f2a3b4c5d6/movie.mp4/raw | grep -i content-length

# Open in a media player (Range support → seek works)
vlc https://bot.example.com/f/a1b2c3d4e5f6a7b8c9d0e1f2a3b4c5d6/movie.mp4/raw
mpv https://bot.example.com/f/a1b2c3d4e5f6a7b8c9d0e1f2a3b4c5d6/movie.mp4/raw
```

---

## Range Request Examples

```bash
# First 1000 bytes
curl -r 0-999 -o first.bin https://bot.example.com/f/a1b2c3d4e5f6a7b8c9d0e1f2a3b4c5d6/movie.mp4/raw
# 206 Partial Content  Content-Range: bytes 0-999/<size>  Content-Length: 1000

# Last 1000 bytes
curl -r -1000 -o last.bin https://bot.example.com/f/a1b2c3d4e5f6a7b8c9d0e1f2a3b4c5d6/movie.mp4/raw
# Content-Range: bytes <size-1000>-<size-1>/<size>

# From byte 50000 to end
curl -r 50000- -o rest.bin https://bot.example.com/f/a1b2c3d4e5f6a7b8c9d0e1f2a3b4c5d6/movie.mp4/raw

# Arbitrary range
curl -r 1000-1999 -o middle.bin https://bot.example.com/f/a1b2c3d4e5f6a7b8c9d0e1f2a3b4c5d6/movie.mp4/raw

# Unsatisfiable range
curl -r 999999999- -i https://bot.example.com/f/a1b2c3d4e5f6a7b8c9d0e1f2a3b4c5d6/movie.mp4/raw
# 416 Range Not Satisfiable  Content-Range: bytes */<size>  Cache-Control: no-store
```

---

## Common Headers (all routes)

Set by `corsMiddleware` on every response:

| Header | Value | Purpose |
|---|---|---|
| `Access-Control-Allow-Origin` | `*` | File links are public by design (see [CORS](#cors)) |
| `Access-Control-Allow-Methods` | `GET, HEAD, OPTIONS` | Only these methods are routed |
| `Access-Control-Allow-Headers` | `Range, Content-Type, Accept, Authorization` | Range for seek + auth for players |
| `Access-Control-Expose-Headers` | `Content-Length, Content-Range, Content-Disposition` | Lets JS players read these |
| `Access-Control-Max-Age` | `86400` | Cache preflight for 1 day |

`Access-Control-Allow-Credentials` is NOT set — file URLs are not authenticated.

## File Stream Response Headers (`/f/{hash}/{filename}/raw`)

| Header | Value |
|---|---|
| `Content-Type` | MIME from record (fallback `application/octet-stream`) |
| `Content-Disposition` | `attachment; filename="<ascii>"; filename*=UTF-8''<percent-encoded>` (or `inline` if `?disposition=inline`) |
| `Content-Length` | File size (or range length for 206) |
| `Accept-Ranges` | `bytes` |
| `Cache-Control` | `public, max-age=31536000` (1 year — file content is immutable) |
| `Connection` | `keep-alive` |
| `X-Content-Type-Options` | `nosniff` |
| `Content-Range` | `bytes N-M/<size>` (only on 206) |

---

## CORS

ThunderGo serves file links as **bearer-style URL credentials** — the hash in the URL IS the secret. Anyone with the link can stream/download. There is no per-file authentication.

This means the HTTP layer MUST allow cross-origin requests — pasting a `/f/{hash}/...` URL into any website, forum, or chat must work. `Access-Control-Allow-Origin: *` is set on every response. `Access-Control-Allow-Credentials` is NOT set (no credentials to send — file URLs are unauthenticated). `Range` is a CORS-safelisted request header, so most media-player requests don't even trigger a preflight.

For the security implications see [../SECURITY.md](../SECURITY.md).

---

## Error Responses

All error responses set `Cache-Control: no-store` to prevent shared caches from storing the failure for a year.

| Status | When | Body |
|---|---|---|
| `200 OK` | Successful full-body request | file bytes |
| `206 Partial Content` | Successful Range request | requested byte range |
| `302 Found` | `GET /` (GitHub) or `GET /activate/{token}` (Telegram deep link) | empty |
| `400 Bad Request` | Malformed `Range` header | `Bad Request\n` |
| `404 Not Found` | Hash not found, file size <= 0, empty activation token, or vault message gone (stale DB record deleted) | `404 page not found\n` |
| `405 Method Not Allowed` | Unsupported method on a known route | (chi default) |
| `416 Range Not Satisfiable` | Range outside the file's bounds | `Range Not Satisfiable\n` + `Content-Range: bytes */<size>` |
| `500 Internal Server Error` | DB lookup error | `internal error\n` |
| `503 Service Unavailable` | No client in pool, transient vault error, or bot username not cached (activate route) | `no client available\n` / `Service Unavailable\n` / `bot username not available\n` |

> **Vault message gone:** if `GetMessages` returns `MESSAGE_ID_INVALID` or `MESSAGE_DELETED` (and NOT `FLOOD_WAIT`/`timeout`), the vault message is gone — the bot deletes the stale DB record and returns 404.

---

## `/status` Response Schema

```typescript
interface StatusResponse {
  status: string;          // always "ok" if the bot is running
  version: string;         // injected at link time, "dev" for local builds
  uptime: string;          // human-readable ("2h34m10s")
  uptime_secs: number;
  bot_username: string;    // bot's @username (cached at startup)
  client_count: number;    // 1 + len(TG_EXTRA_BOTSN)
  total_inflight: number;  // sum of per-client inflight
  clients: ClientStatus[];
}

interface ClientStatus {
  index: number;           // 0 = primary, 1..N = secondaries
  inflight: number;        // current in-flight downloads on this client
  dc: number;              // Telegram data center (0 if unknown)
}
```

`Cache-Control: no-store` is always set — operational telemetry must never be cached.

Example:

```json
{
  "status": "ok",
  "version": "v1.0.0",
  "uptime": "2h34m10s",
  "uptime_secs": 9250,
  "bot_username": "MyThunderGoBot",
  "client_count": 3,
  "total_inflight": 2,
  "clients": [
    { "index": 0, "inflight": 1, "dc": 2 },
    { "index": 1, "inflight": 1, "dc": 2 },
    { "index": 2, "inflight": 0, "dc": 4 }
  ]
}
```
