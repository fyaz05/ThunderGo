# Security Policy

## Reporting a Vulnerability

1. **Do NOT open a public GitHub issue.**
2. Email the maintainer (see commit history for contact) with a description, reproduction steps, and (if possible) a proof of concept.
3. Expect acknowledgment within 72 hours.

---

## Threat Model

ThunderGo is a **public file-sharing bot**. Anyone with a file link (`/f/{hash}/{filename}/...`) can stream or download the file. The hash IS the credential — there is no per-file authentication on the HTTP layer.

This document explains what is and is not protected, so operators can deploy with realistic expectations.

---

## File URLs: Deterministic Hash Model

The URL segment in `/f/{hash}/...` is a deterministic 80-bit SHA-256 prefix of the file key (`sha256(fileKey)[:10]` hex, 20 chars). The same file always produces the same hash, so:

- **Deduplication works** — re-uploading the same file produces the same URL.
- **Hashes are predictable to anyone who already has the file** — an attacker with the file content can compute its hash and check whether the bot has ingested it.
- **Hashes are NOT predictable to anyone who doesn't have the file** — 80 bits of SHA-256 prefix makes online enumeration infeasible (~2^80 guesses per file).

This is a deliberate trade-off: deduplication requires the same file → same URL mapping, which random per-file tokens would break. If you need true per-file bearer credentials for confidential documents, ThunderGo is not the right tool.

---

## Token Activation (optional bot-access gate)

When `TG_TOKEN_ENABLED=true`, unauthorized users must activate via a one-time link before they can use the bot. **This is a bot-access gate, not a file-access gate** — `/f/{hash}/...` is always public.

Flow: unauthorized user sends any command → bot replies with an **Activate** button linking to `https://yourdomain/activate/{token}` → user clicks → `/activate/{token}` 302-redirects to `tg://<bot>?start={token}` → the bot's `/start` handler consumes the token atomically (`FindOneAndDelete` — race-free) → user is activated for `TG_TOKEN_TTL_HOURS` (default 24h, MongoDB TTL index auto-expires).

| Who | Bypasses the gate? |
|---|---|
| Owner | Yes (bypasses the entire pre-flight chain) |
| Authorized users (`/authorize`) | Yes |
| `/start` command | Yes (always passes so the deep link can deliver the token) |
| Everyone else | No — must activate |

Token properties: 128 bits of `crypto/rand`, Crockford base32 (26 chars), 10-minute storage TTL, atomic one-shot consumption. The HTTP access log redacts `/activate/{token}` paths via `redactPath` — a log reader cannot hijack a token from the access log. The activation token is never logged in plaintext in the bot's preflight logging.

### What the activation system does NOT protect

- **File URLs** — `/f/{hash}/...` is always public. Activation is only about bot command access.
- **Post-activation abuse** — an attacker who obtains a valid token has 10 minutes to use it. Once consumed, they have `TG_TOKEN_TTL_HOURS` of bot access. `/ban` them — the banned check in pre-flight step 1 runs BEFORE the activation check, so a banned user is denied regardless of activation status.

---

## CORS Wildcard Policy

`Access-Control-Allow-Origin: *` is set on every HTTP response. This is intentional and correct:

- File URLs are bearer-style credentials — there is no cookie, no `Authorization` header, no session. Any origin may link to `/f/{hash}/file.mp4/raw` and the browser will fetch it. This is the intended "shareable link" behavior.
- `Access-Control-Allow-Credentials` is NOT set (file URLs are not authenticated — no credentials to send).
- `Range` is a CORS-safelisted request header — most media-player requests don't even trigger a preflight.

---

## What is NOT Protected (by design)

- **File URL enumeration** — 80 bits of SHA-256 makes online enumeration infeasible. Offline enumeration (knowing the file content → computing the hash → checking if the bot has it) requires the attacker to already have the file.
- **No per-IP rate limiting on `/f/...`** — hash entropy is the defense. Pool cap (`TG_MAX_CONCURRENT_PER_CLIENT`) bounds per-client concurrency.
- **No HSTS / CSP / X-Frame-Options on `/f/...`** — TLS is expected to be terminated by an upstream reverse proxy (Caddy/Nginx), which is the correct place for HSTS. See [docs/DEPLOYMENT.md](docs/DEPLOYMENT.md).
- **Vault channel messages are NOT deleted when a file auto-expires** (`TG_FILE_TTL_DAYS`) — only the DB record is removed.
- **Bot session files are NOT encrypted at rest** — gogram limitation. Filesystem-level protection is the operator's responsibility.

## What IS Protected

- **Bot command access** (optional): `TG_PRIVATE_MODE`, `TG_TOKEN_ENABLED`, `TG_FORCE_SUB_CHANNEL_ID` — all combinable.
- **Owner commands** (`/status`, `/ban`, `/broadcast`, `/authorize`, `/deauthorize`, `/listauth`, `/restart`, `/stats`, `/log`, `/users`): silently ignored for non-owners (no info leak), hidden from `/help`, not registered with Telegram's command menu.
- **Banned users/channels**: banned users get a notice + drop; banned channels → bot leaves. Bans cached 10 min. Fail-closed on DB error.
- **Rate limiting** (optional, two layers): **per-user GCRA** (`TG_RATE_LIMIT > 0`, 60s window) and **global token-bucket** (`TG_GLOBAL_RPS > 0`, burst = 2× RPS). Owner + authorized bypass both. Per-user applies to file requests only; global applies to all Telegram calls. Both in-memory, single-process.

---

## Pre-flight Chain

Every bot update runs through `bot.preflight(c)` before the handler. The owner bypasses the entire chain. Chain order: **banned → private-mode → token-activation → force-sub → rate-limit**. DB errors in steps 1 and 2.5 fail closed. See [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md#pre-flight-chain) for the full diagram.

---

## In Scope for Vulnerability Reports

- Bypass of the pre-flight chain (e.g. invoking owner-only commands as non-owner).
- Hash predictability regression (if `deterministicHash` ever becomes predictable for an attacker who doesn't have the file).
- Token reuse or race in `ConsumeActivationToken` (the atomic `FindOneAndDelete` is the defense).
- Token leakage in logs beyond the documented redaction points.
- MongoDB injection via `FindFileByHash` / `FindFileByKey` / `ConsumeActivationToken`.
- Path traversal in `Content-Disposition` filename escaping (RFC 6266 `filename*`).
- Shortener open-redirect or host-mismatch bypass.
- Integer overflow in the Range / Content-Length path.
- XSS in the player HTML page (filename is HTML-escaped; MIME type is auto-escaped by `html/template` and `printf "%q"` in JS context).
- Cache poisoning of error responses (all error paths set `Cache-Control: no-store`).

---

## Operational Hardening

Deployment-time concerns — see [docs/DEPLOYMENT.md](docs/DEPLOYMENT.md) for the full guide.

- **TLS**: terminate at a reverse proxy (Caddy/Nginx) or PaaS router. `TG_URL` SHOULD be `https://` in production — the file hash traverses the network in cleartext over plain `http://`. Scheme is auto-detected.
- **Bind address**: default `0.0.0.0` (safe in containers — port publishing controls exposure). Set to `127.0.0.1` on bare metal behind a reverse proxy.
- **Container hardening**: non-root user, no-new-privileges, read-only filesystem, all caps dropped, tmpfs on /tmp, resource limits. See `docker-compose.yml`.
- **MongoDB**: use Atlas (TLS, auth, network allowlist) or any external MongoDB with auth enabled. The bundled `docker-compose.yml` does NOT ship a self-hosted Mongo.
- **Session file protection**: `umask 0o077` at process startup + `chmod 0600` on each session file + non-root ownership of `/app/data`.
- **Log redaction**: HTTP access log masks `/f/{hash}/...` and `/activate/{token}` via `redactPath`. Bot preflight logging uses `TokenHash` (8-hex-char SHA-256 prefix) for correlation. Do not configure additional logging that bypasses these redaction points.

---

## References

- [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md) — pre-flight chain, request lifecycle, concurrency
- [docs/CONFIGURATION.md](docs/CONFIGURATION.md) — security-relevant env vars
- [docs/API.md](docs/API.md) — CORS headers, error responses, Range semantics
- [docs/DEPLOYMENT.md](docs/DEPLOYMENT.md) — TLS termination, reverse proxy, container hardening
- [CONTRIBUTING.md](CONTRIBUTING.md) — `make gosec`, `make vulncheck`
