<div align="center">

# session-converter

[![License](https://img.shields.io/badge/license-Apache--2.0-blue.svg)](LICENSE)

</div>

Telegram session string converter for Go — decode, encode, and convert between every major MTProto library format.

## Supported formats

| Format | String sessions | SQLite files |
|--------|:-:|:-:|
| Telethon | ✓ | ✓ |
| Pyrogram | ✓ | ✓ |
| GramJS | ✓ | |
| mtcute | ✓ | |
| MTKruto | ✓ | |
| gogram | ✓ | |
| gotgproto (gotd/td) | ✓ | |
| mtgo (native MTGO1) | ✓ | |

## Install

```bash
go get github.com/mtgo-labs/session-converter
```

CLI:

```bash
go install github.com/mtgo-labs/session-converter/cmd/tgconv@latest
```

## Quick start

### Go API

```go
import tgconv "github.com/mtgo-labs/session-converter"

// Auto-detect format and decode
session, format, err := tgconv.Decode(sessionString)

// Convert to another format
pyrogramString, err := tgconv.Convert(sessionString, tgconv.FormatPyrogram)

// Read a SQLite session file and export as a string
session, _, err := tgconv.ReadSQLite("session.session")
str, _ := tgconv.Encode(session, tgconv.FormatTelethon)
```

### CLI

```bash
# Convert (auto-detects source format)
tgconv convert "1ApWapzMBuwAAB..." -t pyrogram --api-id 2040 --user-id 123456789 --is-bot

# Show session info
tgconv info "AgAAB_gA-reNm..."

# Export from SQLite file
tgconv from-file session.session -t telethon

# List supported formats
tgconv list
```

## Format reference

| Format | Field | Encoding |
|--------|-------|----------|
| **Telethon** | dc, ip, port, auth_key | `"1"` + base64url |
| **Pyrogram** | dc, api_id, test_mode, auth_key, user_id, is_bot | base64url (no prefix) |
| **GramJS** | dc, addr_len, addr, port, auth_key | `"1"` + base64std |
| **mtcute** | version, flags, dc_option (TL), user_id, is_bot, auth_key | base64url |
| **MTKruto** | dc_string, auth_key, api_id, is_bot, user_id | RLE + base64url |
| **gogram** | auth_key, hash, dc_id, ip_addr, app_id | `"1BvE"` + base64url(JSON) |
| **gotgproto** | dc, addr, auth_key, auth_key_id, salt, config | base64std(JSON) |

## License

Apache License 2.0
