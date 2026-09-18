# Validation Gauntlet & Canary Cutover

Operational checklist for promoting the mtgo re-platform (`revival/mtgo-replatform`) to production. Code-level gates (build, vet, `-race` suite, characterization tests) run in CI; **this document covers what CI cannot prove**.

> Run the gauntlet on **production data centers only**. Throttling, slow-DC behavior, and cross-DC pacing do not reproduce on test DCs (revival plan §6).

## 1. Pre-flight

- [ ] `go build ./... && go vet ./... && go test -race ./...` green on the release commit
- [ ] `go mod verify` clean; vendor tree committed and in sync (`go mod vendor` produces no diff)
- [ ] `mtgo` pinned exactly (`v0.21.0`); no dependabot bumps merged since the last Phase-5-lite run
- [ ] Fork `mtgo-labs/mtgo` into your org (bus-factor insurance) and record the fork URL here: ___________
- [ ] Frozen rollback branch exists and is pushable: `pre-revival-freeze`
- [ ] Spike confirmations on record: `tgerr.AsFloodWait` present at the pinned version (✅ verified v0.21.0); gogram sessions not migrated — bot re-login from tokens (✅ automatic)

## 2. Staging gauntlet (production DCs, `tc`-throttled + lossy links)

- [ ] **Byte-exactness**: old vs new path `sha256` identical at **10 MB**, **500 MB**, **2 GB+** (probe via `?disposition=inline` and ranged `curl -r`)
- [ ] **Resume-at-offset**: kill the client mid-stream; resume with `Range` and confirm byte-exact continuation (never restart-from-0)
- [ ] **Cancel storms**: 100-way concurrent cancel storm (browsers aborting range requests) → zero slot leaks (`/status` `total_inflight` returns to 0), no goroutine growth
- [ ] **FloodWait injection**: inject FLOOD_WAIT on one bot → traffic shifts to healthy bots (cooldown in logs, 5-min sweep); all bots flooded → `503` + `Retry-After` estimate, no queuing; flood on the serving path before first byte → `429`
- [ ] **Primary-kill failover**: SIGSTOP/SIGKILL the primary bot connection → serving continues on extras; bot commands recover after reconnect
- [ ] **99%-completion**: byte counts on truncated-then-resumed large files; mid-stream failure truncates and clients resume with `Range` (never a corrupt 200)
- [ ] **File-ref expiry**: expire a file reference mid-download → refresh (≤3) + retry heals without a client-visible error
- [ ] **24h soak**: flat memory + flat goroutine count over 24 hours under mixed traffic (`/status` + pprof)
- [ ] **TTFB**: p50/p99 time-to-first-byte within **2× of the Python service** on the same DC

**Ship bars (plan §6):** byte-exact on all sizes · zero slot leaks · p99 TTFB within 2× · flat memory over 24h.

## 3. Phase-5-lite (before every mtgo promotion)

A 4-hour soak plus one cancel storm on the candidate bump. The Phase-4 characterization suite (`go test -race ./...`) is the merge gate — **red means pin back**, never blindly adapt.

## 4. Canary cutover (Phase 6)

1. Shadow **5%** of requests to the new path; `sha256` both paths per sampled request; compare nightly.
2. Raise to **25%** after 48h clean sampling; repeat byte-compare sampling.
3. **100%** only after 25% stays clean; hold **72h clean at 100%**.
4. Decommission the previous service only after the 72h hold.
5. Rollback at any stage: re-point traffic to the previous service; the `pre-revival-freeze` branch is the frozen rollback target (Mongo schema unchanged — rollback safe by construction).

## 5. mtgo flip conditions (tracked, not yet met)

Re-evaluate the backend choice when ALL of: `v1` tag exists · one independent production file-serving deployment · 6+ months maintainer continuity. Until then: monthly review max, Phase-5-lite before every promotion, and >3 months upstream silence (or a second scare) → re-target raw `gotd/td` behind the same seam (2-file job, not a rewrite).
