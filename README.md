<p align="center">
  <img src="https://cdn.jsdelivr.net/gh/fyaz05/Resources@main/ThunderGo/logo.png" alt="ThunderGo Logo" width="120">
  <h1 align="center">ThunderGo</h1>
</p>

<p align="center">
  <b>Telegram File-to-Link Bot — Direct Links & Streaming in Go</b>
</p>

<p align="center">
  <a href="https://github.com/fyaz05/ThunderGo/actions/workflows/ci.yml"><img src="https://github.com/fyaz05/ThunderGo/actions/workflows/ci.yml/badge.svg" alt="CI Status"></a>
  <a href="https://go.dev/"><img src="https://img.shields.io/github/go-mod/go-version/fyaz05/ThunderGo" alt="Go Version"></a>
  <a href="LICENSE"><img src="https://img.shields.io/github/license/fyaz05/ThunderGo" alt="License"></a>
  <a href="https://github.com/fyaz05/ThunderGo/pkgs/container/thundergo"><img src="https://img.shields.io/badge/docker-GHCR-blue" alt="Docker Image"></a>
</p>

<p align="center">
  <a href="https://app.koyeb.com/deploy?type=docker&image=ghcr.io/fyaz05/thundergo:latest&name=thundergo&ports=8080;http;/&env%5BTG_API_ID%5D=&env%5BTG_API_HASH%5D=&env%5BTG_BOT_TOKEN%5D=&env%5BTG_VAULT_CHANNEL_ID%5D=&env%5BTG_OWNER_USER_ID%5D=&env%5BTG_MONGO_URI%5D=&env%5BTG_URL%5D="><img src="https://img.shields.io/badge/Deploy%20to-Koyeb-blue?style=for-the-badge&logo=koyeb" alt="Deploy to Koyeb"></a>
  <a href="https://render.com/deploy"><img src="https://img.shields.io/badge/Deploy%20to-Render-blue?style=for-the-badge&logo=render" alt="Deploy to Render"></a>
  <a href="https://heroku.com/deploy"><img src="https://img.shields.io/badge/Deploy%20to-Heroku-purple?style=for-the-badge&logo=heroku" alt="Deploy to Heroku"></a>
</p>

---

## Table of Contents

- [About](#about)
- [Features](#features)
- [How It Works](#how-it-works)
- [Quick Start](#quick-start)
  - [Prerequisites](#prerequisites)
  - [Step-by-Step Setup](#step-by-step-setup)
  - [Verification](#verification)
- [Commands](#commands)
  - [Public Commands](#public-commands)
  - [Owner Commands](#owner-commands)
- [Configuration](#configuration)
- [Tech Stack](#tech-stack)
- [Documentation](#documentation)
- [License](#license)

---

## About

**ThunderGo** turns Telegram files into direct HTTP links. Send any file to the bot and get back a link anyone can stream or download — no Telegram account needed.

**Perfect for:**

- **Bypassing Telegram's download limits** for speedier transfers
- **Unlimited cloud storage** with streaming support
- **Content distribution** to communities without a Telegram client
- **Embedded HTML5 player** with seek, resume, and external player support

---

## Features

### Core
- **Direct HTTP(S) Links** — any Telegram file becomes a downloadable link
- **Permanent Links** — links work as long as the file stays in the vault channel
- **Browser Playback** — HTML5 player with seek and resume (Vidstack)
- **All File Formats** — videos, audio, images, documents, archives

### Performance
- **Zero Disk I/O** — streams from Telegram directly to the client
- **Low Memory** — ~1 MiB RAM per active stream
- **Go Concurrency** — goroutines with configurable stream threads (1-8)
- **Multi-Bot Pool** — extra bot tokens for more download throughput

### Access Control
- **Token Activation** — time-limited tokens for bot access
- **Private Mode** — only you and authorized users can use the bot
- **Force Sub Gate** — users must join a channel first
- **Rate Limiting** — per-user + global caps
- **Bans** — block users or channels

### Customization
- **Custom Domain** — use your own domain for links
- **URL Shortener** — shorten links via external API
- **Batch Processing** — link up to `TG_BATCH_CAP` files at once
- **Channel Auto-Process** — auto-link media in channels the bot manages

---

## How It Works

```
User          Bot          Vault Channel    MongoDB        Browser
  │            │                │              │              │
  │── send ──▶ │                │              │              │
  │            │── forward ──▶ │              │              │
  │            │── store ────▶ │              │              │
  │            │◀─ reply ───── │              │              │
  │            │                │              │              │
  │◀─ link ─── │                │              │              │
  │            │                │              │              │
  │────────── GET /f/{hash}/{filename} ────────────────────▶ │
  │            │                │◀──── fetch ──│              │
  │            │                │── stream ────────────────▶ │
```

Bytes flow from Telegram to the browser with **zero disk writes** and **~1 MiB memory per stream**.

---

## Quick Start

### Prerequisites

You need:

- [ ] **Docker & Docker Compose** installed on your machine
- [ ] **A Telegram account** with a phone number
- [ ] **A MongoDB connection string** (MongoDB Atlas free tier M0 cluster recommended)
- [ ] **A private Telegram channel** where your bot is added as an administrator with **Post Messages** permission

### Step-by-Step Setup

**Step 1: Clone the repository**

```bash
git clone https://github.com/fyaz05/ThunderGo.git
cd ThunderGo
```

**Step 2: Prepare configuration**

```bash
cp .env.example .env
```

Open `.env` and fill in the **7 required fields**:

| Variable | How to Get It |
|---|---|
| `TG_API_ID` | Go to https://my.telegram.org → **API development tools** → Create app → Copy **App api_id** |
| `TG_API_HASH` | Same page → Copy **App api_hash** |
| `TG_BOT_TOKEN` | Message [@BotFather](https://t.me/BotFather) → `/newbot` → Choose name → Copy token (format: `123456789:ABCdefGhI...`) |
| `TG_VAULT_CHANNEL_ID` | Create a **Private Channel** → Settings → Administrators → Add your bot as admin (enable **Post Messages**) → Send a test message → Forward it to [@userinfobot](https://t.me/userinfobot) → Copy the **numeric ID** (must start with `-100`, e.g. `-1001234567890`) |
| `TG_OWNER_USER_ID` | Send `/start` to [@userinfobot](https://t.me/userinfobot) → Copy your numeric user ID |
| `TG_MONGO_URI` | Create a free M0 cluster on [MongoDB Atlas](https://www.mongodb.com/atlas) → Create database user → Network Access: add `0.0.0.0/0` → Connect → Drivers → Copy connection string (replace `<password>`) |
| `TG_URL` | Your bot's public URL (e.g. `https://thunder.yourdomain.com`). For local testing: `http://localhost:8080` |

> If your MongoDB password has special chars (`@`, `:`, `/`, `?`, `#`), URL-encode them (`@` → `%40`, `:` → `%3A`).

**Step 3: Start the service**

```bash
docker compose up -d --build
```

> The service binds to `127.0.0.1:8080` (local only). Use a reverse proxy like Caddy or Nginx for TLS in production. See [Deployment Guide](docs/DEPLOYMENT.md).

**Step 4: Use the bot**

1. Open Telegram and send any media file to your bot
2. The bot replies with two links:
   - **Player Link**: `https://your-domain/f/{hash}/{filename}` — browser player with seek
   - **Direct Download Link**: `https://your-domain/f/{hash}/{filename}/raw` — raw file download with Range support
3. Open either link in a browser to stream or download

> The `{hash}` is an 80-bit SHA-256 prefix (20 hex chars). Same file = same link. Links are **public by design** — see [Security Model](SECURITY.md).

### Verification

Check the service is working:

```bash
# Health check
curl -s http://localhost:8080/health
# Expected: {"status":"ok"}

# Status endpoint
curl -s http://localhost:8080/status
# Expected: JSON with version, uptime, bot_username, client metrics
```

Expected startup log output (visible via `docker compose logs -f`):

1. `starting ThunderGo version=...`
2. `MongoDB connected`
3. `telegram client pool ready total_clients=1 primary_dc=...`
4. `bot started username=... owner_id=...`
5. `HTTP gateway listening addr=... base_url=...`

For systemd, Heroku, PaaS, or reverse proxy setups, see the [Deployment Guide](docs/DEPLOYMENT.md).

### Quick Deploy (One-Click)

Deploy directly to a PaaS using the pre-built Docker image from GHCR:

| Platform | How |
|---|---|
| **Koyeb** | Click the badge above — set your 7 env vars in the Koyeb dashboard after deploy |
| **Render** | Click the badge → create a new Web Service → pick the `ghcr.io/fyaz05/thundergo:latest` Docker image → set env vars |
| **Heroku** | `heroku create && heroku stack:set container && heroku config:set TG_... && git push heroku main` (see [Deployment Guide](docs/DEPLOYMENT.md)) |

**Non-Docker deployments** (Heroku buildpack, bare metal, VPS): use `Procfile` + `thunder.sh`. Set `UPSTREAM_REPO` to auto-update on restart (requires Go + git at runtime).

---

## Commands

### Public Commands

| Command | Description |
|---|---|
| `/start [token]` | Welcome message + consume activation token |
| `/help` | Show available commands |
| `/ping` | Check bot latency |
| `/dc` | Show which Telegram data center hosts a file or user |
| `/link [N]` | Generate links for replied-to media in groups (1–`TG_BATCH_CAP` files) |
| `/about` | Bot info and repo links |

### Owner Commands

| Command | Description |
|---|---|
| `/status` | Health check (pool, uptime, inflight) |
| `/users` | Total registered users |
| `/stats` | Live runtime stats (memory, goroutines, GC) |
| `/log` | Get server log (last 1 MiB) |
| `/ban <id> [reason]` | Ban a user or channel |
| `/unban <id>` | Unban a user or channel |
| `/broadcast [group]` | Broadcast replied-to message (`all`, `authorized`, or `regular`) |
| `/authorize <id>` | Add user to allowlist (bypasses rate limits + private mode) |
| `/deauthorize <id>` | Remove user from allowlist |
| `/listauth` | List authorized users |
| `/restart` | Pull, rebuild, restart |

> Batch links are sent 10 per message to stay within Telegram's character limits.

---

## Configuration

Copy `.env.example` to `.env` and fill in the **7 required fields**. All options are documented in [Configuration Reference](docs/CONFIGURATION.md).

### Required Variables

| Variable | Description |
|---|---|
| `TG_API_ID` | Telegram API ID from https://my.telegram.org |
| `TG_API_HASH` | Telegram API Hash |
| `TG_BOT_TOKEN` | Bot token from [@BotFather](https://t.me/BotFather) |
| `TG_VAULT_CHANNEL_ID` | Private storage channel ID (must start with `-100`) |
| `TG_OWNER_USER_ID` | Your Telegram user ID (for admin access) |
| `TG_MONGO_URI` | MongoDB connection string |
| `TG_URL` | Full public URL of your service |

### Common Pitfalls

- **No bundled database** — `docker-compose.yml` does not include MongoDB. Use an external or self-hosted instance.
- **Wrong channel ID** — vault channel ID must start with `-100` (e.g. `-1001234567890`).
- **Bot must be admin** — the bot needs **admin + Post Messages** in the vault channel. A regular invite won't work.
- **Special chars in passwords** — URL-encode `@`, `:`, `/`, `?`, `#` in MongoDB passwords.

### Key Optional Variables

`TG_PRIVATE_MODE`, `TG_TOKEN_ENABLED`, `TG_FORCE_SUB_CHANNEL_ID`, `TG_RATE_LIMIT`, `TG_GLOBAL_RPS`, `TG_FILE_TTL_DAYS`, `TG_LOG_LEVEL`, `TG_EXTRA_BOTS1..50`, `TG_STREAM_THREADS`, `TG_HTTP_PORT`, `TG_BIND_ADDRESS`, `UPSTREAM_REPO`. See [Configuration Reference](docs/CONFIGURATION.md).

---

## Tech Stack

**Go 1.26** · [gogram](https://github.com/amarnathcjd/gogram) · [MongoDB](https://www.mongodb.com/) · [chi](https://github.com/go-chi/chi) · [caarlos0/env](https://github.com/caarlos0/env) · `go:embed` · **9 direct deps** · **No CGO** · **~31 MB binary**

---

## Documentation

| Document | What It Covers |
|---|---|
| [Architecture](docs/ARCHITECTURE.md) | System design, stream lifecycle, concurrency |
| [Deployment](docs/DEPLOYMENT.md) | Docker, bare-metal, Heroku, PaaS, reverse proxy |
| [Configuration](docs/CONFIGURATION.md) | All env vars with examples |
| [API Reference](docs/API.md) | HTTP routes, Range requests, error codes |
| [Security](SECURITY.md) | Threat model, hardening, data storage |
| [Contributing](CONTRIBUTING.md) | Dev setup, tests, code style, PR guide |

---

## License

Apache-2.0 — see [LICENSE](LICENSE).
