# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## What this is

A pure-Go library (`package tgconv`) that decodes, encodes, and converts Telegram MTProto **session strings** between every major client-library format (Telethon, Pyrogram, GramJS, mtcute, MTKruto, gogram, gotgproto). It also reads Telethon/Pyrogram `.session` SQLite files. A CLI lives in `cmd/tgconv`.

## Commands

```bash
go build ./...                       # build library + CLI
go test ./...                        # run all tests (root pkg only; cmd has no tests)
go test ./... -run TestRoundTripMtcute  # run a single test
go test ./... -run TestConvert        # run a group (e.g. all Convert* tests)
go vet ./...                         # lint
go install ./cmd/tgconv              # build+install the CLI as $GOPATH/bin/tgconv
```

There is no Makefile/justfile — use plain `go` commands.

The `tgconv generate` subcommand authenticates against live Telegram via the `github.com/mtgo-labs/mtgo` dependency (interactive phone/bot-token login). It is **not** part of the pure conversion library and needs network + credentials.

## Architecture

### Session is the canonical intermediate representation

`session.go` defines `Session` — the single struct every format decodes *into* and encodes *from*. Conversion is always `format → Session → format`, never format-to-format directly. The 256-byte `AuthKey` is the only field guaranteed to survive every round trip; not every format carries `AppID`/`UserID`/`ServerAddress`, so conversions are lossy in those optional fields.

### Per-format file contract

Each format is one file (`telethon.go`, `pyrogram.go`, `mtcute.go`, …) exposing a symmetric `EncodeX(s *Session) (string, error)` / `DecodeX(str string) (*Session, error)` pair following a fixed contract:

- **Encode**: call `s.validate()` first (requires 256-byte auth key + non-zero DC), then build the wire bytes, then base64.
- **Decode**: build a `Session`, then call `s.fillDefaults()` before returning so `ServerAddress`/`Port` are populated from the DC-ID table in `dc.go` for formats that omit them (e.g. Pyrogram strings carry no IP).

### Dispatch and detection

`converter.go` is the public surface: `Decode` (auto-detect), `DecodeFormat`, `Encode`, `Convert`, `ConvertFrom`, `DetectFormat`. The internal `decodeByFormat` switch and the `Encode` switch are the two places every format must be registered.

`DetectFormat` (`detect.go` + `converter.go`) identifies a format from a string using **structural heuristics in a fixed precedence order** — gogram `"1BvE"`/`"1BvX"` prefix → gotgproto base64-JSON → `"1"` prefix (Telethon vs GramJS, disambiguated by base64 charset) → Pyrogram 271-byte payload → mtcute version byte → MTKruto RLE (last resort). `Decode` first trusts the detected format, then brute-forces every format if that fails. **This ordering is fragile**: when changing a format's encoding, re-run `TestDetectFormat` and the cross-format tests, because an ambiguous string can be misrouted to an earlier heuristic.

Telethon strings are a single version — always prefix `"1"`, and `api_id` is never stored (real Telethon's `StringSession` serializes only dc/ip/port/auth_key). Telethon v2 (alpha, `v2` branch) has no string-session format at all, so `"1"` is the only real prefix.

### SQLite handling

`sqlite.go` uses the pure-Go `modernc.org/sqlite` driver (no CGO). `ReadSQLite` auto-detects Telethon vs Pyrogram by table names (`entities` → Telethon, `peers` → Pyrogram); the Pyrogram reader probes `PRAGMA table_info(sessions)` because the `api_id` column is only in newer schemas.

### Shared helpers

`dc.go` (DC→IP defaults, `defaultPort`, `validate`, `fillDefaults`), `tl.go` (TL bytes/bool serialization + IPv6 heuristic, used by mtcute/MTKruto), `rle.go` (MTKruto's zero-run encoding), `detect.go` (base64 and JSON-sniff helpers).

### Format interop notes

Encoders/decoders target the **canonical upstream wire format**. When changing one, cross-check the upstream library source rather than guessing — these were verified against upstream:

- **Telethon** (`LonamiWebs/Telethon` v1 `sessions/string.py`): single version, prefix `"1"`, struct `>B{}sH256s` = `dc[1] + ip[4|16] + port[2 BE] + authKey[256]`, base64url *with* padding. `api_id` is never serialized. Telethon v2 (alpha, `v2` branch) has **no string-session format at all** (only `MemorySession`/`SqliteSession`), so `"1"` is the only real string-session prefix.
- **GramJS** (`gram-js/gramjs` `sessions/StringSession.ts`): standard form is `"1" + base64std(dc[1] + addrLen[2 BE] + addrString + port[2 BE] + authKey[256])`. The encoder emits this string-IP form. The decoder additionally accepts a **4-byte binary IPv4** in the address field (`addrLen == 4` → render dotted-quad) — a variant some converters emit that real GramJS would misread. The encoder does *not* produce that variant.
- **MTKruto** (`MTKruto/MTKruto` `client/0_storage_operations.ts` `exportAuthString`/`importAuthString`): `base64url(rleEncode(TL_string(dc) + TL_bytes(authKey) + int32_LE(apiId) + byte(isBot) + int64_LE(userId)))`. The decoder is lenient about a missing tail, so minimal `dc + authKey`-only strings from other tools decode (with zero `apiId`/`userId`).
- **Field fidelity varies by format**: Pyrogram strings carry no IP/port (filled from `dc.go`); Telethon v1 and GramJS carry no `apiId`/`UserID`/`IsBot`; mtcute carries no `apiId`. Only the 256-byte auth key survives every cross-format chain. A `telethon(80) → pyrogram → telethon` round trip therefore loses the real port (defaults to 443).

## Adding a new format

1. Add a `Format` const and an entry in `AllFormats` (`session.go`).
2. Create `<format>.go` with `EncodeX`/`DecodeX` following the contract above (validate on encode, `fillDefaults` on decode).
3. Register in both switches: `Encode` and `decodeByFormat` (`converter.go`).
4. Add a detection branch to `DetectFormat` with a position in the precedence that won't be shadowed by earlier heuristics.
5. Add a `TestRoundTripX` plus an entry in `TestDetectFormat`'s table (`converter_test.go`). Tests use `makeTestSession`/`makeRandomSession`; the invariant they assert is byte-identical auth keys across encode→decode and across cross-format chains.
