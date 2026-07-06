# Contributing to ThunderGo

How to set up a dev environment, run tests, follow code style, and open a PR. For deployment see [docs/DEPLOYMENT.md](docs/DEPLOYMENT.md); for env vars see [docs/CONFIGURATION.md](docs/CONFIGURATION.md); for architecture see [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md).

---

## Prerequisites

- **Go 1.26+** (verify with `go version`)
- **GNU Make** (optional — `Makefile` wraps common commands)
- **Docker + Docker Compose** (only if you run via `make up`)
- **MongoDB** (local Docker container OR Atlas free tier)

The `Makefile` pins `gosec` and `govulncheck` versions — `make gosec` / `make vulncheck` use `go run` with pinned versions, so you don't need to install them globally.

---

## Development Setup

```sh
# 1. Clone + build
git clone https://github.com/fyaz05/ThunderGo.git
cd ThunderGo
make build

# 2. Local MongoDB via Docker
docker run -d --name thundergo-mongo -p 27017:27017 \
  -e MONGO_INITDB_ROOT_USERNAME=thundergo \
  -e MONGO_INITDB_ROOT_PASSWORD=devpassword \
  mongo:7

# 3. Configure .env (fill in REQUIRED fields; use the URI above for TG_MONGO_URI,
#    set TG_URL=http://localhost:8080 and TG_LOG_LEVEL=debug for local dev)
cp .env.example .env
$EDITOR .env

# 4. Run + test
make run
#   - Send a small media file to your bot in Telegram → verify stream + download links
#   - Range request sanity check:
curl -r 0-99 -o head.bin "http://localhost:8080/f/{hash}/{filename}/raw"
ls -l head.bin   # should be 100 bytes

# 5. Stop: Ctrl-C triggers graceful shutdown
```

Expected startup log (in order): `starting ThunderGo` → `MongoDB connected` → `telegram client pool ready` → `bot started` → `HTTP gateway listening`.

---

## Testing

Tests live alongside the code (`*_test.go`). Test packages: `config`, `store`, `pool`, `ratelimit`, `shortener`, `tgutil`, `http`, `stream`, `web` (no tests yet for `bot`/`log`/`app`).

```sh
make test       # go vet + go test -race -count=1 ./...
make test-ci    # go vet + go test -race -count=10 ./... (flake-hunting)
make coverage   # HTML coverage report → cover.html
make vet        # go vet ./...
make fmt        # go fmt ./...
make gosec      # gosec security scanner (pinned version)
make vulncheck  # govulncheck (pinned version)
make size       # asserts binary < 25 MB
```

Run a single test: `go test -race -count=1 -run TestParseRange ./internal/tgutil/...`

When writing tests: prefer table-driven (see `internal/tgutil/util_test.go`); use `-race` for concurrency; test pure helpers and entity structs in `store_test.go`; run MongoDB integration tests via `go test -tags integration ./internal/store/...`; test error paths; never swallow errors — fail fast with `t.Fatalf`.

---

## Code Style

- **gofmt is non-negotiable.** Run `gofmt -w .` (or `make fmt`) before every commit.
- **goimports** for import grouping (stdlib → third-party → local).
- Follow [Effective Go](https://go.dev/doc/effective_go) and the [Go Code Review Comments](https://github.com/golang/go/wiki/CodeReviewComments).
- Exported identifiers must have doc comments beginning with the identifier name.
- **Error handling:** wrap with `fmt.Errorf("...: %w", err)`; never swallow.
- Avoid `panic` in library code; return errors.
- Keep functions short; extract helpers when behavior is reused.
- The codebase uses **tabs** (canonical Go). `make fmt` runs `go fmt` on the whole tree.
- Multi-line comments use `//` on each line. Inline comments explain **WHY**, not WHAT. Keep comments short and direct — no history lessons, no finding-ID citations.
- Reference external standards (e.g. `RFC 7233`) where the WHY benefits from it; do NOT cite internal audit IDs or CWE numbers.

### gosec suppressions

When `gosec` flags something safe, suppress with `#nosec` AND a rationale (every `#nosec` must be justified in the comment AND mentioned in the PR description):

```go
//nosec G703 // sessionFile is built from the working directory + hardcoded format, not user input
if err := os.Chmod(sessionFile, 0o600); err != nil && !os.IsNotExist(err) {
    log.Warn("chmod session file failed", ...)
}
```

---

## Commit Messages

ThunderGo uses [Conventional Commits](https://www.conventionalcommits.org/) 1.0.0:

```
<type>(<scope>): <short summary>

<optional body>

<optional footers>
```

Allowed types: `feat`, `fix`, `security`, `perf`, `refactor`, `docs`, `test`, `build`, `ci`, `chore`.
Scope is the module short-name (`bot`, `store`, `http`, `stream`, `config`, `ratelimit`, `shortener`, `pool`) — omit if cross-cutting.

Branch naming: `fix/<short-description>`, `feat/<short-description>`, `chore/<short-description>`, or `docs/<short-description>`.

---

## PR Checklist

A PR may be merged only after **all** pass:

- [ ] `make vet` — clean
- [ ] `make test` — all packages pass
- [ ] `make vulncheck` — no unresolved vulnerabilities
- [ ] `make gosec` — no new findings (or each suppressed with a justified `#nosec`)
- [ ] Code is `gofmt`-clean + `goimports`-clean (import groups: stdlib, third-party, local)
- [ ] Commit messages follow Conventional Commits
- [ ] Branch follows the naming convention
- [ ] `CHANGELOG.md` `[Unreleased]` section updated if user-visible
- [ ] New code has tests (table-driven where applicable)
- [ ] New env vars documented in `.env.example` AND [docs/CONFIGURATION.md](docs/CONFIGURATION.md)
- [ ] New HTTP routes documented in [docs/API.md](docs/API.md)
- [ ] New bot commands documented in the README commands table (auto-listed in `/help`)
- [ ] Architecture changes reflected in [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md) (Mermaid preferred)
- [ ] Security-relevant changes reflected in [SECURITY.md](SECURITY.md)

---

## Updating the Changelog

User-visible changes must be reflected in `CHANGELOG.md` under `[Unreleased]`, using the Keep a Changelog categories (`Added`, `Changed`, `Deprecated`, `Removed`, `Fixed`, `Security`).

---

## Questions

Open a GitHub issue with the `question` label, or reach the maintainer via the contact details in the commit history.
