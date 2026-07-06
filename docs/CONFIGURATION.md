# Configuration

Every ThunderGo environment variable. For deployment see [DEPLOYMENT.md](DEPLOYMENT.md); for the HTTP API see [API.md](API.md); for architecture see [ARCHITECTURE.md](ARCHITECTURE.md).

> **How config loads:** `app.New()` reads `.env` + `.env.local` (best-effort). Existing env vars take precedence over file values. `caarlos0/env/v11` parses into a `Config` struct, then `validate()` runs. Startup fails on any validation error.
>
> **Booleans** accept `true/false/1/0/t/f/T/F/TRUE/FALSE` (case-insensitive). Empty = `false`. **Lists:** `TG_BANNED_CHANNEL_IDS` is comma-separated (empty = empty list). **Indexed bots:** `TG_EXTRA_BOTS1`, `TG_EXTRA_BOTS2`, ... are scanned contiguously (gaps silently skipped). There is NO comma-separated `TG_EXTRA_BOTS` var.
>
> **HTTPS:** scheme in `TG_URL` is auto-detected — `http://` is accepted. In production, terminate TLS at a reverse proxy and use `https://` so the file hash doesn't traverse the network in cleartext. See [../SECURITY.md](../SECURITY.md).

---

## Quick Reference — All Env Vars

| Var | Required | Default | What |
|---|---|---|---|
| `TG_API_ID` | **yes** | — | Telegram API ID |
| `TG_API_HASH` | **yes** | — | Telegram API hash |
| `TG_BOT_TOKEN` | **yes** | — | Bot token from @BotFather |
| `TG_VAULT_CHANNEL_ID` | **yes** | — | Private vault channel ID (starts with `-100`) |
| `TG_OWNER_USER_ID` | **yes** | — | Your Telegram user ID |
| `TG_MONGO_URI` | **yes** | — | MongoDB connection string |
| `TG_URL` | **yes** | — | Full public URL (scheme auto-detected) |
| `TG_EXTRA_BOTS1`, `TG_EXTRA_BOTS2`, ... | no | (none) | Indexed extra bot tokens for parallel downloads |
| `TG_MAX_CONCURRENT_PER_CLIENT` | no | `8` | Max simultaneous downloads per bot client |
| `TG_STREAM_THREADS` | no | `4` | Parallel download threads per full-body stream (1–8) |
| `TG_LOG_FILE` | no | (auto) | Log file path (see Storage & Logging) |
| `TG_PRIVATE_MODE` | no | `false` | Restrict to owner + authorized users |
| `TG_FORCE_SUB_CHANNEL_ID` | no | `0` | Require users to join a channel first |
| `TG_TOKEN_ENABLED` | no | `false` | Require one-time-link activation |
| `TG_TOKEN_TTL_HOURS` | no | `24` | How long an activation lasts (hours) |
| `TG_CHANNEL_AUTO_PROCESS` | no | `false` | Auto-process media posted in broadcast channels |
| `TG_BANNED_CHANNEL_IDS` | no | (empty) | Comma-separated banned channel IDs |
| `TG_RATE_LIMIT` | no | `0` | Max files per user per minute (0 = disabled) |
| `TG_GLOBAL_RPS` | no | `0` | Global requests-per-second cap across all users (0 = disabled, burst = 2× RPS) |
| `TG_SHORTENER_API_KEY` | no | (empty) | URL shortener API key |
| `TG_SHORTENER_SITE` | no | (empty) | URL shortener base URL |
| `TG_HTTP_PORT` | no | `0` (auto) | Listen port override |
| `TG_BIND_ADDRESS` | no | `0.0.0.0` | Interface to listen on |
| `TG_KEEPALIVE_SECS` | no | `300` | Self-ping interval (0 = disabled) |
| `TG_FILE_TTL_DAYS` | no | `0` | Auto-expire files N days after conversion (0 = never) |
| `TG_LOG_LEVEL` | no | `info` | `debug` / `info` / `warn` / `error` |
| `TG_BATCH_CAP` | no | `50` | Max files per `/link N` batch |
| `UPSTREAM_REPO` | no | (empty) | Git URL for auto-update on restart (non-Docker only) |
| `UPSTREAM_BRANCH` | no | `main` | Branch to track for auto-update |

---

## Required

These MUST be set. The bot will not start without them.

### `TG_API_ID`

Telegram application API ID (integer). Get it from https://my.telegram.org → "API development tools" → copy the **App api_id**. **If unset:** startup fails with `env: parse error on field "APIID"`.

```ini
TG_API_ID=123456
```

### `TG_API_HASH`

Telegram application API hash (32-char hex string). Same source as `TG_API_ID`. **If unset:** startup fails with `required environment variable "TG_API_HASH" is not set`.

```ini
TG_API_HASH=abcdef1234567890abcdef1234567890
```

### `TG_BOT_TOKEN`

Bot token from @BotFather (looks like `123456789:ABC...XYZ`). Authenticates the primary client. **If unset:** startup fails with `required environment variable "TG_BOT_TOKEN" is not set`, or `LoginBot` fails with `USER_DEACTIVATED` / `AUTH_KEY_INVALID`.

```ini
TG_BOT_TOKEN=123456789:ABCdefGHIjklMNO_pqrSTUvwxYZ
```

### `TG_VAULT_CHANNEL_ID`

ID of the private Telegram channel the bot uses as a file vault. Must be `<= -1000000000000` (i.e. `-100`-prefixed). **How to get it:** create a private channel → add bot as admin with "Post Messages" → forward any message from it to @userinfobot → copy the ID. **If unset or wrong:** startup fails with `must be a -100-prefixed channel ID`.

```ini
TG_VAULT_CHANNEL_ID=-1001234567890
```

> A small negative number like `-100` or `-1` means you forwarded from the wrong place.

### `TG_OWNER_USER_ID`

Your Telegram user ID (positive integer). Grants owner privileges: bypasses every pre-flight check + enables owner-only commands. **How to get it:** open @userinfobot → `/start` → copy the ID. **If unset:** startup fails with `TG_OWNER_USER_ID is required`.

```ini
TG_OWNER_USER_ID=123456789
```

### `TG_MONGO_URI`

MongoDB connection string. This is the ONLY way to configure MongoDB — the bundled `docker-compose.yml` does NOT ship a self-hosted Mongo. Use [MongoDB Atlas](https://www.mongodb.com/atlas) free tier (recommended) or any external Mongo. **If unset:** startup fails with `required environment variable "TG_MONGO_URI" is not set`.

```ini
# Atlas free tier (recommended)
TG_MONGO_URI=mongodb+srv://thundergo:password@cluster0.abcde.mongodb.net/?retryWrites=true&w=majority

# Self-hosted with auth
TG_MONGO_URI=mongodb://thundergo:password@mongo.example.com:27017
```

> If your password contains special chars (`@`, `:`, `/`, `?`, `#`), URL-encode them (`%40`, `%3A`, etc.).

### `TG_URL`

The bot's full public URL. Drives file-URL builders, the default listen port (443 for `https`, 8080 for `http`, or the explicit URL port), the keepalive ping target, and the activation link base. Scheme is auto-detected — `http://` is accepted. The bot strips trailing slashes. **If unset or malformed:** startup fails with `TG_URL must be a full URL like https://bot.herokuapp.com`.

```ini
TG_URL=https://bot.example.com
TG_URL=https://my-thundergo.herokuapp.com
TG_URL=https://bot.example.com:8443   # explicit port
TG_URL=http://localhost:8080          # local testing
```

> If `TG_URL` port and the actual listen port differ (reverse-proxy case), override with `TG_HTTP_PORT`.

---

## Optional: Speed / Throughput

### `TG_EXTRA_BOTS1`, `TG_EXTRA_BOTS2`, ... (indexed)

Additional bot tokens for parallel downloads. Each index holds ONE token — do NOT comma-separate. Each token becomes a download-only client in the pool. **When to set:** primary bot hits FLOOD_WAIT in logs, or you want faster downloads for many users. **If unset:** pool has only the primary client — all downloads go through it.

```ini
TG_EXTRA_BOTS1=123456789:AAA...BBB
TG_EXTRA_BOTS2=987654321:CCC...DDD
TG_EXTRA_BOTS3=111111111:EEE...FFF
```

> Each extra bot must be created via @BotFather and added as an admin to the vault channel with "Post Messages" permission. Use contiguous indices.

### `TG_MAX_CONCURRENT_PER_CLIENT`

Max simultaneous downloads per bot client before the pool falls back to least-loaded overall. Lower it (e.g. `4`) if you see FLOOD_WAIT errors. Raise it (e.g. `16`) if your bots have headroom. Must be >= 1. **If unset:** default `8` is a safe middle ground.

```ini
TG_MAX_CONCURRENT_PER_CLIENT=8
```

### `TG_STREAM_THREADS`

Number of parallel download workers gogram spawns per **full-body** stream (i.e. `GET /f/{hash}/{filename}/raw` without a `Range` header). `4` = parallel (default; `1` = sequential). `2`–`8` = parallel chunk downloads via an `orderedWriter` that reassembles bytes in order — typically 2–3x faster for large files. **Range requests always use a single sequential loop.** Must be in `[1, 8]`. **If unset:** `4`.

```ini
TG_STREAM_THREADS=4    # 2-3x faster large-file downloads
TG_STREAM_THREADS=1    # best for high-concurrency gateways (many users, different files)
```

> Do NOT exceed `8` — Telegram per-session flood limits. For gateways serving many users each requesting different files, keep `1` (sequential) so per-client parallelism stays under `TG_MAX_CONCURRENT_PER_CLIENT`.

---

## Optional: Access Control

### `TG_PRIVATE_MODE`

`true` = restrict the bot to owner + authorized users only. Pre-flight step 2 rejects unauthorized users. Authorized users (added via `/authorize`) bypass. **If unset:** `false` (bot is open to everyone).

```ini
TG_PRIVATE_MODE=true
```

### `TG_FORCE_SUB_CHANNEL_ID`

Require users to join a channel before using the bot. Pre-flight step 3 checks membership via `GetChatMember`. Non-members get a Join button. `/start` always passes. Must be `<= -1000000000000` (or `0` to unset). **If unset:** no force-sub check.

```ini
TG_FORCE_SUB_CHANNEL_ID=-1009876543210
```

### `TG_CHANNEL_AUTO_PROCESS`

`true` = auto-process media posted in broadcast channels where the bot is an admin. The bot forwards to the vault and posts the link back to the channel. **If unset:** `false` — the bot only ingests via `/link` (groups) or private chat. Processes broadcast-channel posts only, not supergroups.

```ini
TG_CHANNEL_AUTO_PROCESS=true
```

### `TG_BANNED_CHANNEL_IDS`

Comma-separated channel IDs to ban at startup. The bot leaves these channels and rejects messages from them. Runtime bans via `/ban` still work (stored in MongoDB). **If unset:** no config-seeded bans.

```ini
TG_BANNED_CHANNEL_IDS=-1001111111111,-1002222222222
```

---

## Optional: Token Activation Gate

Requires unauthorized users to "activate" via a one-time link before using the bot. Owner + authorized users bypass. File URLs themselves are NOT gated — only bot command access. See [ARCHITECTURE.md](ARCHITECTURE.md#pre-flight-chain).

### `TG_TOKEN_ENABLED`

`true` = enable the token activation gate. Unactivated users get an activation prompt with an inline button linking to `https://yourdomain/activate/{token}`. Clicking the button redirects to `tg://<bot>?start={token}`; the bot's `/start` handler consumes the token atomically (FindOneAndDelete) and activates the user for `TG_TOKEN_TTL_HOURS`. **If unset:** `false` — no token gate.

```ini
TG_TOKEN_ENABLED=true
```

### `TG_TOKEN_TTL_HOURS`

How long an activation lasts, in hours. After expiry, the user must re-activate. MongoDB TTL index auto-expires the record. Must be > 0 when `TG_TOKEN_ENABLED=true`. **If unset:** default `24` (1 day).

```ini
TG_TOKEN_TTL_HOURS=168   # 1 week
```

> **One-time activation tokens** (the `activation_tokens` collection) have a fixed 10-minute TTL — the time the user has between receiving the prompt and clicking the button. This is NOT configurable. `TG_TOKEN_TTL_HOURS` controls how long the resulting activation lasts.
>
> **Cache lag:** `IsUserActivated` is cached for 5 min. After expiry, a user may briefly retain access (up to 5 min). Harmless — re-activating is idempotent.

---

## Optional: Rate Limiting

### `TG_RATE_LIMIT`

Max files (`/link` or private media ingestion) per user per **minute**. The window is hardcoded to 60 seconds. Owner + authorized users bypass. Per-user GCRA limiter (16 shards, atomic CAS). `0` = disabled. **If unset:** `0` — no per-user limit. Pool cap + Telegram flood limits are the only throttles.

```ini
TG_RATE_LIMIT=10
```

### `TG_GLOBAL_RPS`

Global requests-per-second cap across **ALL** users (a token-bucket circuit-breaker that prevents Telegram `FLOOD_WAIT`s when many users are active simultaneously). Owner + authorized users bypass. `0` = disabled. Smooths the rate over time and lets short bursts through up to **`2 × TG_GLOBAL_RPS`** (burst is hardcoded). **If unset:** `0` — no global cap (per-user GCRA via `TG_RATE_LIMIT` is the only throttle).

```ini
TG_GLOBAL_RPS=10   # allows bursts of 20, then smooths to 10/sec
```

---

## Optional: URL Shortener

### `TG_SHORTENER_API_KEY`

API key for an external URL shortener. When set alongside `TG_SHORTENER_SITE`, the bot shortens stream/download links for non-authorized users. Owner + authorized users always receive full-length links. **If unset:** no shortening.

```ini
TG_SHORTENER_API_KEY=abc123def456
```

### `TG_SHORTENER_SITE`

Base URL of the shortener API. The bot validates that the shortened URL's host matches this site (open-redirect defense). Must be `https://`. **If unset:** shortener is disabled even if `TG_SHORTENER_API_KEY` is set.

```ini
TG_SHORTENER_SITE=https://is.gd
```

---

## Optional: Network / HTTP

### `TG_HTTP_PORT`

TCP port the HTTP gateway listens on. `0` (default) = auto-derive from `TG_URL` (explicit URL port wins, otherwise 443 for `https`, 8080 for `http`). `>0` = override the URL-derived port — `TG_URL` stays as the public-facing URL used in generated file links, but the actual listener uses `TG_HTTP_PORT`. **When to set:** reverse-proxy / PaaS-router setups where URL port ≠ listen port (e.g. URL says 443 but bot listens on 8080; Heroku `$PORT`).

```ini
TG_HTTP_PORT=8080
```

> If `TG_HTTP_PORT` and the URL port disagree, `TG_HTTP_PORT` wins for the **listener** while `TG_URL` stays as the **public-facing** URL.

### `TG_BIND_ADDRESS`

Network interface to bind to. Default `0.0.0.0` (all interfaces) is correct for containers (port publishing controls exposure) and PaaS. Set to `127.0.0.1` ONLY on bare metal behind a reverse proxy that proxies to localhost. **If unset:** `0.0.0.0`.

```ini
TG_BIND_ADDRESS=127.0.0.1
```

### `TG_KEEPALIVE_SECS`

Self-ping interval in seconds. Pings `BaseURL + /health` every N seconds to keep the process awake on PaaS providers that sleep idle processes (Heroku free tier). `0` = disabled. Must be >= 0. **If unset:** default `300` (5 min).

```ini
TG_KEEPALIVE_SECS=0   # disable on paid PaaS
```

---

## Optional: Auto-Update

Controls the `thunder.sh` startup script and the `/restart` Telegram command. Only works when Go + git are available at runtime (bare metal, Heroku buildpack, VPS). Docker users: update by restarting to pull the latest GHCR image.

### `UPSTREAM_REPO`

Git repository URL to track for automatic updates. When set:

- **`thunder.sh`** (startup script) — clones/pulls the repo and rebuilds the binary before every start
- **`/restart`** (Telegram command) — pulls from this repo instead of hardcoded `origin main`

```ini
UPSTREAM_REPO=https://github.com/fyaz05/ThunderGo.git
```

### `UPSTREAM_BRANCH`

Branch to track when `UPSTREAM_REPO` is set. Default: `main`.

```ini
UPSTREAM_BRANCH=main
```

---

## Optional: Storage & Logging

### `TG_FILE_TTL_DAYS`

Auto-expire files N days after they were **converted** (ingested), regardless of access. When > 0, a MongoDB TTL index removes file records whose `created_at` is older than N days. **TTL is on `created_at`, NOT `last_seen_at`.** Must be >= 0. **If unset:** `0` (never expire). Only the DB record is deleted — the vault message in Telegram is NOT deleted.

```ini
TG_FILE_TTL_DAYS=30
```

### `TG_LOG_LEVEL`

Minimum log level. `debug` (verbose — preflight rejections, cache misses), `info` (normal), `warn` (problems that don't break the bot), `error` (failures needing attention). Unknown values fall back to `info` with a warning. **If unset:** `info`.

```ini
TG_LOG_LEVEL=debug
```

### `TG_LOG_FILE`

Log file path for the lumberjack-rotated file logger. If empty, defaults to `/var/log/thundergo/thundergo.log`. Rotation: 100 MiB max, 5 backups, 14-day retention, uncompressed. **If unset:** auto-detected from fallback chain. The Dockerfile sets this to `/app/data/thundergo.log`.

```ini
TG_LOG_FILE=/var/log/thundergo/thundergo.log
```

### `TG_BATCH_CAP`

Max batch size for `/link N`. Telegram's batch-get endpoint caps at 100 anyway; 50 keeps the reply under Telegram's 4096-char message limit. Falls back to 50 if < 1. **If unset:** default `50`.

```ini
TG_BATCH_CAP=50
```

---

## Common Configuration Patterns

### Pattern 1: Public Bot (default)

Anyone can use the bot. File links are public. Just the 7 REQUIRED fields; everything else uses defaults.

### Pattern 2: Private Bot (owner + allowlist)

```ini
TG_PRIVATE_MODE=true
```

Then add users via Telegram: `/authorize <user_id>` (from @userinfobot).

### Pattern 3: Token-Gated Bot (activation link)

```ini
TG_TOKEN_ENABLED=true
TG_TOKEN_TTL_HOURS=24
```

Flow: unauthorized user sends any command → bot replies with an Activate button → user clicks → `/activate/{token}` 302-redirects to `tg://<bot>?start={token}` → bot consumes token + activates user for 24h.

### Pattern 4: Force-Sub Bot (channel membership)

```ini
TG_FORCE_SUB_CHANNEL_ID=-1009876543210
```

Flow: unauthorized user sends any command (except `/start`) → bot replies with a Join button → user joins → retries → bot verifies membership.

### Pattern 5: Combined (max restriction)

```ini
TG_TOKEN_ENABLED=true
TG_TOKEN_TTL_HOURS=24
TG_FORCE_SUB_CHANNEL_ID=-1009876543210
TG_RATE_LIMIT=10
```

### Pattern 6: High-Throughput (multiple bots)

```ini
TG_EXTRA_BOTS1=123456789:AAA...BBB
TG_EXTRA_BOTS2=987654321:CCC...DDD
TG_EXTRA_BOTS3=111111111:EEE...FFF
TG_MAX_CONCURRENT_PER_CLIENT=16
TG_STREAM_THREADS=4
```

Each extra bot must be created via @BotFather and added as admin to the vault channel with "Post Messages" permission.

### Pattern 7: Reverse Proxy on Bare Metal

```ini
TG_URL=https://bot.example.com      # public URL (port 443 implied)
TG_HTTP_PORT=8080                   # bot listens on local high port
TG_BIND_ADDRESS=127.0.0.1           # loopback only
```

The proxy (Caddy/Nginx) terminates TLS on 443 and forwards to `127.0.0.1:8080`. See [DEPLOYMENT.md](DEPLOYMENT.md#step-4--reverse-proxy-tls).

### Pattern 8: Cloud PaaS (Heroku / Koyeb / Render)

```ini
TG_URL=https://my-thundergo.herokuapp.com
TG_BIND_ADDRESS=0.0.0.0
# TG_HTTP_PORT is auto-detected from $PORT when unset
TG_KEEPALIVE_SECS=300       # keep dyno awake
```

The platform's router terminates TLS. Logs go to stdout (file logger is a no-op on ephemeral filesystems).

### Pattern 9: Auto-Update on Restart (non-Docker)

```ini
UPSTREAM_REPO=https://github.com/fyaz05/ThunderGo.git
UPSTREAM_BRANCH=main
```

On every restart, `thunder.sh` clones the latest code and rebuilds. The `/restart` Telegram command also uses these values. Docker users ignore this — update by restarting to pull `:latest`.

---

## Validation Rules

`config.validate()` runs at startup. Any failure aborts with a clear error.

| Check | Error |
|---|---|
| `TG_URL` not a full URL (missing scheme/host) | `TG_URL must be a full URL like https://bot.herokuapp.com (got: %s)` |
| `TG_API_ID` <= 0 | `TG_API_ID must be a positive integer` |
| `TG_VAULT_CHANNEL_ID` > -1000000000000 | `TG_VAULT_CHANNEL_ID must be a -100-prefixed channel ID (<= -1000000000000), got %d` |
| `TG_FORCE_SUB_CHANNEL_ID` != 0 and > -1000000000000 | `TG_FORCE_SUB_CHANNEL_ID must be a -100-prefixed channel ID or 0 (unset), got %d` |
| `TG_OWNER_USER_ID` == 0 | `TG_OWNER_USER_ID is required` |
| `TG_STREAM_THREADS` not in `[1, 8]` | `TG_STREAM_THREADS must be in [1, 8], got %d` |
| `TG_BATCH_CAP` < 1 | (clamped to 50; warning logged on stderr) |
| `TG_HTTP_PORT` not 0 and not in [1, 65535] | `TG_HTTP_PORT must be 0 or in [1, 65535], got %d` |
| `TG_MAX_CONCURRENT_PER_CLIENT` < 1 | `TG_MAX_CONCURRENT_PER_CLIENT must be >= 1, got %d` |
| `TG_KEEPALIVE_SECS` < 0 | `TG_KEEPALIVE_SECS must be >= 0 (0 = disabled), got %d` |
| `TG_FILE_TTL_DAYS` < 0 | `TG_FILE_TTL_DAYS must be >= 0 (0 = never expire), got %d` |
| `TG_TOKEN_ENABLED=true` and `TG_TOKEN_TTL_HOURS` <= 0 | `TG_TOKEN_TTL_HOURS must be positive when TG_TOKEN_ENABLED is true, got %d` |
| `TG_LOG_LEVEL` not one of `debug/info/warn/error` | (warns + falls back to `info`) |
