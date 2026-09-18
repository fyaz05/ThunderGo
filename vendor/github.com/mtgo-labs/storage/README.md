<div align="center">

# storage

</div>

Persistent storage adapters for [mtgo] Telegram clients — session data, peer cache, and conversation state.

[![License](https://img.shields.io/badge/license-Apache--2.0-blue.svg)](LICENSE)

## Features

- **Six built-in backends**: SQLite (pure-Go, no CGO), PostgreSQL, MongoDB, Redis, GORM, SQLC-generated
- **In-memory adapter**: `storage.NewMemory()` for testing and short-lived clients
- **Conformance test suite**: verify custom adapters with `internal/suite`
- **Conversation plugin support**: optional `ConversationStore` interface
- **Portable session strings**: export sessions in Telethon/Pyrogram format
- **Update state and dedup**: durable update queue with retry tracking

## Installation

```bash
go get github.com/mtgo-labs/storage
```

Install only the backend you need:

```bash
go get github.com/mtgo-labs/storage/sqlite
# or
go get github.com/mtgo-labs/storage/postgres
# or
go get github.com/mtgo-labs/storage/mongodb
```

## Usage

### SQLite

```go
ext, err := sqlite.Open("session.db")
if err != nil {
    log.Fatal(err)
}
defer ext.Close()

client, err := tg.NewClient(mustAtoi(apiID), apiHash, &tg.Config{
    BotToken:    botToken,
    SessionName: "storage_bot",
    SavePeers:   true,
    Storage:     storage.NewAdapter(ext),
})
```

### PostgreSQL

```go
ext, err := postgres.Open(postgres.Config{
    Host:     "localhost",
    Port:     5432,
    User:     "mtgo",
    Password: "secret",
    Database: "mtgo",
    SSLMode:  "require",
})
if err != nil {
    log.Fatal(err)
}
defer ext.Close()

client, err := tg.NewClient(mustAtoi(apiID), apiHash, &tg.Config{
    BotToken:    botToken,
    SessionName: "storage_bot",
    SavePeers:   true,
    Storage:     storage.NewAdapter(ext),
})
```

### MongoDB

```go
ext, err := mongodb.Open(ctx, mongodb.Config{
    URI:      "mongodb://localhost:27017",
    Database: "mtgo",
})
if err != nil {
    log.Fatal(err)
}
defer ext.Close()

client, err := tg.NewClient(mustAtoi(apiID), apiHash, &tg.Config{
    BotToken:    botToken,
    SessionName: "storage_bot",
    SavePeers:   true,
    Storage:     storage.NewAdapter(ext),
})
```

Note: Backends implement [`storage.Adapter`](storage.go) (`SessionStore` + `PeerStore` + `Close`). The [`storage.NewAdapter`](adapter.go) wrapper bridges that interface into the client's [`Storage`](storage.go) interface.

### Exporting a Session String

```go
str, err := sqlite.ExportSessionString(sess)
```

## Custom Adapters

Implement the [`storage.Adapter`](storage.go) interface (`SessionStore` + `PeerStore` + `Close`). For conversation plugin support, also implement [`ConversationStore`](storage.go).

A complete JSON-file example is in [`examples/custom_storage/`](examples/custom_storage/).

### Verifying with the Test Suite

```go
func TestMyAdapter(t *testing.T) {
    a := myadapter.Open()
    suite.Run(t, a)
}
```

Individual sub-suites are also available: `suite.RunSession`, `suite.RunPeers`, `suite.RunConversations`.

## Architecture

```
storage.go          — interfaces and domain types (Session, Peer, Conversation, UpdateState)
adapter.go          — wraps Adapter into the client's Storage interface
memory.go           — in-memory adapter (NewMemory) for testing
sqlite/             — SQLite adapter (pure-Go via modernc.org/sqlite)
postgres/           — PostgreSQL adapter (lib/pq)
mongodb/            — MongoDB adapter (mongo-driver/v2)
redis/              — Redis adapter
gorm/               — GORM adapter
sqlc/               — SQLC schema and generated queries for sqlite/postgres
internal/suite/     — conformance test suite for custom adapters
examples/           — custom adapter examples
```

## Contributing

Contributions are welcome! Please open an issue or pull request.

### Running Tests

```bash
go test ./...
```

## License

Licensed under the Apache License, Version 2.0. See [`LICENSE`](LICENSE) for details.