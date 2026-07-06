# Deployment

How to deploy ThunderGo. For env vars see [CONFIGURATION.md](CONFIGURATION.md); for the HTTP API see [API.md](API.md); for architecture see [ARCHITECTURE.md](ARCHITECTURE.md).

---

## Prerequisites

| What | Where |
|---|---|
| Telegram account | https://telegram.org |
| MongoDB instance | [MongoDB Atlas](https://www.mongodb.com/atlas) free tier (recommended) or any external Mongo |
| Public URL | Your domain + reverse proxy (Caddy/Nginx) OR your PaaS provider's URL |

---

## Step 1 — Telegram Setup

1. **API credentials** — https://my.telegram.org → "API development tools" → create app → copy **App api_id** + **App api_hash** → `TG_API_ID`, `TG_API_HASH`.
2. **Bot** — [@BotFather](https://t.me/BotFather) → `/newbot` → pick name + username (ends in `bot`) → copy the token → `TG_BOT_TOKEN`.
3. **Vault channel** — create a **private** channel → add the bot as admin with **Post Messages** → forward any message from it to [@userinfobot](https://t.me/userinfobot) → copy the ID (starts with `-100`, e.g. `-1001234567890`) → `TG_VAULT_CHANNEL_ID`.
4. **Your user ID** — [@userinfobot](https://t.me/userinfobot) → `/start` → copy your numeric user ID → `TG_OWNER_USER_ID`. The owner bypasses every pre-flight check and can run owner-only commands.
5. **(Optional) Extra bots** — for higher throughput, create additional bots via @BotFather, add each as admin to the vault channel, set each token as `TG_EXTRA_BOTS1`, `TG_EXTRA_BOTS2`, ... (one token per variable).

> The vault channel ID MUST start with `-100` and be `<= -1000000000000`. If you got `-100` or `-1`, you forwarded from the wrong place.

---

## Step 2 — MongoDB Setup

### Option A: MongoDB Atlas (free tier — recommended)

1. Sign up at https://www.mongodb.com/atlas → create a free **M0** cluster (any region close to your bot host).
2. Under "Database Access", create a user with a strong password.
3. Under "Network Access", allow your bot host's IP (or `0.0.0.0/0`).
4. Click "Connect" → "Drivers" → copy the connection string:
   ```
   mongodb+srv://thundergo:<password>@cluster0.abcde.mongodb.net/?retryWrites=true&w=majority
   ```
5. Replace `<password>` with your actual password. This becomes `TG_MONGO_URI`.

### Option B: External / self-hosted MongoDB

```
mongodb://<user>:<password>@<host>:27017/<dbname>
```

The bot creates all required collections + indexes at startup — no manual setup needed.

> If your password contains special characters (`@`, `:`, `/`, `?`, `#`), URL-encode them (`%40`, `%3A`, etc.).

---

## Step 3 — Deploy

### Option A: Docker Compose (recommended)

Easiest path on a single VM. The bundled `docker-compose.yml` runs just the ThunderGo service — MongoDB is your own (see Step 2).

```bash
git clone https://github.com/fyaz05/ThunderGo.git
cd ThunderGo
cp .env.example .env
$EDITOR .env   # fill in the 7 REQUIRED fields (including TG_MONGO_URI)
docker compose up -d --build
```

The compose file:

- Publishes `127.0.0.1:8080:8080` (loopback only — pair with a reverse proxy for TLS)
- Mounts a named volume `thundergo-data` at `/app/data` for bot session files + logs
- Uses the binary's `-healthcheck` flag for the container healthcheck
- Hardens the service (non-root, read-only filesystem, all caps dropped, 512 MiB RAM cap)

```bash
docker compose up -d --build      # build + start
docker compose logs -f             # tail logs
docker compose restart thundergo   # restart just the bot
docker compose down                # stop + remove containers (keeps volumes)
```

### Option B: Heroku

Heroku restarts dynos daily (every 24h). Two deploy options:

> **One-click deploy:** Click the Deploy to Heroku badge in the [README](../README.md) — fill in the 7 required env vars and deploy instantly. Then run `heroku config:set TG_URL=https://YOUR-APP.herokuapp.com` with your actual URL.

#### Docker (recommended — uses `heroku.yml`)

Pre-compiled binary, fastest restart. Updates: push a new GHCR release → Heroku pulls the latest `:latest` tag on the next 24h restart (or manual `heroku restart`).

```bash
heroku create my-thundergo
heroku stack:set container
heroku config:set TG_API_ID=... TG_API_HASH=... TG_BOT_TOKEN=...
heroku config:set TG_VAULT_CHANNEL_ID=-1001234567890
heroku config:set TG_OWNER_USER_ID=123456789
heroku config:set TG_MONGO_URI=mongodb+srv://...
heroku config:set TG_URL=https://my-thundergo.herokuapp.com
heroku config:set TG_BIND_ADDRESS=0.0.0.0
git push heroku main
heroku ps:scale web=1
```

> Heroku's router terminates TLS and injects `$PORT`. You MUST set `TG_BIND_ADDRESS=0.0.0.0` (Heroku rejects loopback binds). The app auto-detects `$PORT` when `TG_HTTP_PORT` is unset.

The bundled `heroku.yml` tells Heroku to build from the `Dockerfile`. The bot pings `BaseURL + /health` every `TG_KEEPALIVE_SECS` (default 300s) — keeps free-tier dynos awake.

#### Buildpack (uses `Procfile` + `thunder.sh`)

Rebuilds from source on every dyno restart. Use with `UPSTREAM_REPO` for auto-updates.

```bash
heroku create my-thundergo
heroku buildpacks:set heroku/go
heroku config:set TG_API_ID=... ...
heroku config:set UPSTREAM_REPO=https://github.com/fyaz05/ThunderGo.git
git push heroku main
heroku ps:scale web=1
```

> The `Procfile` runs `sh thunder.sh`, which checks `UPSTREAM_REPO`, clones/pulls the source, builds the binary, then starts it. Adds 30–60s to startup time for the build.

### Option C: Other PaaS (Koyeb / Render / Railway / Fly.io)

One-click deploy from the README badges, or manually:

**Koyeb / Render (Docker image — instant)**

Deploy directly from `ghcr.io/fyaz05/thundergo:latest`:
- **Koyeb:** `https://app.koyeb.com/deploy?type=docker&image=ghcr.io/fyaz05/thundergo:latest&name=thundergo&ports=8080;http;/` — then set env vars in the dashboard
- **Render:** Click the badge → New Web Service → Docker image `ghcr.io/fyaz05/thundergo:latest` → set env vars

**Railway / Fly.io (source deploy — builds from Dockerfile)**

1. Connect your GitHub repo (uses the repo's Dockerfile).
2. Set env vars from `.env.example` (REQUIRED section + `TG_MONGO_URI`).
3. Set `TG_URL` to the platform-assigned HTTPS URL.
4. If the platform injects `$PORT`, set `TG_BIND_ADDRESS=0.0.0.0`. The app auto-detects `$PORT` when `TG_HTTP_PORT` is unset.
5. Deploy. Use Atlas free tier for MongoDB.

On paid PaaS, set `TG_KEEPALIVE_SECS=0` to disable the keepalive ping.

### Option D: Bare Metal with systemd

```bash
# Install Go 1.26+
wget https://go.dev/dl/go1.26.4.linux-amd64.tar.gz
sudo tar -C /usr/local -xzf go1.26.4.linux-amd64.tar.gz
echo 'export PATH=$PATH:/usr/local/go/bin' | sudo tee /etc/profile.d/go.sh && source /etc/profile.d/go.sh

# Build + install
git clone https://github.com/fyaz05/ThunderGo.git && cd ThunderGo
make build
sudo mkdir -p /opt/thundergo/data
sudo cp bin/thundergo /opt/thundergo/
sudo useradd --system --no-create-home --shell /usr/sbin/nologin thundergo
sudo chown -R thundergo:thundergo /opt/thundergo

# .env: copy .env.example, fill in REQUIRED fields,
# add TG_BIND_ADDRESS=127.0.0.1 and TG_HTTP_PORT=8080
sudo cp .env.example /opt/thundergo/.env
sudo $EDITOR /opt/thundergo/.env   # edit as the thundergo user OR set perms after
sudo chown thundergo:thundergo /opt/thundergo/.env && sudo chmod 600 /opt/thundergo/.env

# systemd unit
sudo tee /etc/systemd/system/thundergo.service > /dev/null <<'EOF'
[Unit]
Description=ThunderGo
After=network-online.target
Wants=network-online.target

[Service]
Type=simple
User=thundergo
Group=thundergo
WorkingDirectory=/opt/thundergo
EnvironmentFile=/opt/thundergo/.env
Environment=TG_DATA_DIR=/opt/thundergo/data
Environment=TG_LOG_FILE=/opt/thundergo/data/thundergo.log
ExecStart=/opt/thundergo/thundergo
Restart=on-failure
RestartSec=5s
NoNewPrivileges=true
PrivateTmp=true
ProtectSystem=strict
ProtectHome=true
ReadWritePaths=/opt/thundergo/data
RestrictAddressFamilies=AF_INET AF_INET6 AF_UNIX
SystemCallFilter=@system-service

[Install]
WantedBy=multi-user.target
EOF
sudo systemctl daemon-reload && sudo systemctl enable --now thundergo
sudo journalctl -u thundergo -f
```

For auto-updates on restart, set `UPSTREAM_REPO` in the `.env` and run `thunder.sh` instead of the binary directly:

```ini
# In /opt/thundergo/.env:
UPSTREAM_REPO=https://github.com/fyaz05/ThunderGo.git
```

Then change `ExecStart` to:
```
ExecStart=/opt/thundergo/thunder.sh
```

The script clones the latest code and rebuilds before every start. Pair with a reverse proxy for TLS (see Step 4).

---

## Step 4 — Reverse Proxy (TLS)

The bot binds to `0.0.0.0:8080` by default. To expose it over HTTPS, put a reverse proxy in front.

### Caddy (recommended — automatic TLS)

```Caddyfile
bot.example.com {
    reverse_proxy 127.0.0.1:8080 {
        flush_interval -1
        transport http {
            response_header_timeout 0
        }
    }
}
```

Install Caddy, drop the above in `/etc/caddy/Caddyfile`, run `systemctl reload caddy`. Caddy obtains + renews the Let's Encrypt cert automatically.

> **Nginx alternative:** use `proxy_buffering off; proxy_read_timeout 1800s; proxy_set_header Range $http_range;` in your `location /` block, with `proxy_pass http://127.0.0.1:8080`. Obtain the cert with `sudo certbot --nginx -d bot.example.com`.

> In production, always terminate TLS at a reverse proxy or PaaS router and use `https://` in `TG_URL`. For local testing, `TG_URL=http://localhost:8080` works out of the box.

---

## Step 5 — Post-Deploy Verification

### 5.1 Health

```bash
curl -s https://bot.example.com/health
# expected: {"status":"ok"}
```

### 5.2 Status

```bash
curl -s https://bot.example.com/status | jq .
# expected: JSON with version, uptime, bot_username, client_count, per-client metrics
```

### 5.3 Send a test file

1. Open your bot in Telegram → send a small media file.
2. The bot replies with stream + download links.
3. Open the stream link in a browser — the player should render.
4. Range request sanity check:

```bash
curl -s -r 0-99 -o /tmp/head.bin https://bot.example.com/f/{hash}/{filename}/raw
ls -l /tmp/head.bin   # expected: 100 bytes
```

### Expected startup log (in order)

1. `starting ThunderGo version=...`
2. `MongoDB connected`
3. `telegram client pool ready total_clients=1 primary_dc=...`
4. `bot started username=... owner_id=...`
5. `HTTP gateway listening addr=... base_url=...`

---

## Troubleshooting

| Symptom | Fix |
|---|---|
| `must be a -100-prefixed channel ID` | Re-forward from the vault channel to @userinfobot — ID must look like `-1001234567890`, not `-100` or `-1` |
| `GetMe failed` | Bot token wrong or Telegram unreachable. Verify: `curl -s "https://api.telegram.org/bot<TOKEN>/getMe"` |
| `connecting to MongoDB: ...` | Atlas: add your IP to Network Access; URL-encode special chars in the password. Self-hosted: check host:port is reachable |
| `/health` works but `/f/{hash}/...` returns 404 | Bot isn't an admin in the vault channel (forward fails with `CHAT_WRITE_FORBIDDEN`), vault ID is wrong, or MongoDB is read-only |
| `/f/{hash}/...` returns 503 | Pool is empty (shouldn't happen) or transient vault error |
| `/f/{hash}/...` returns 404 with `vault message gone` in logs | The Telegram vault message was deleted (`MESSAGE_ID_INVALID` / `MESSAGE_DELETED`). The bot deletes the stale DB record automatically — re-upload the file |
| Bot doesn't respond to commands | Check bot username, `TG_OWNER_USER_ID`, and BotFather's `/setprivacy` setting (must be DISABLED or the bot must be a group admin for `/link` to work in groups) |
| Bot can't send DMs | Users must `/start` the bot in private chat first (Telegram bots can't initiate DMs) |
| Container keeps restarting | Check `docker compose logs thundergo` — the config validator surfaces the specific field that failed |
