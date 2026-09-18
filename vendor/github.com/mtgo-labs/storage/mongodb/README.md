# mongodb

MongoDB storage adapter for [mtgo] Telegram clients.

## Install

```bash
go get github.com/mtgo-labs/storage/mongodb
```

## Usage

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

`Open` connects to the cluster, verifies connectivity with `Ping`, and creates indexes for peer username lookups and conversation composite queries. Collections are created implicitly on first use.

## Collections

**sessions**

Single-document collection. Fields map to [`storage.Session`]:

| Field           | Type    |
|-----------------|---------|
| dc_id           | int     |
| api_id          | int     |
| api_hash        | string  |
| test_mode       | int     |
| auth_key        | []byte  |
| state           | []byte  |
| user_id         | int64   |
| is_bot          | int     |
| first_name      | string  |
| last_name       | string  |
| username        | string  |
| date            | int     |
| server_address  | string  |
| port            | int     |

**peers**

| Field         | Type    | Index         |
|---------------|---------|---------------|
| _id           | int64   |               |
| type          | int     |               |
| access_hash   | int64   |               |
| username      | string  | indexed       |
| usernames     | string  |               |
| first_name    | string  |               |
| last_name     | string  |               |
| phone_number  | string  |               |
| is_bot        | int     |               |
| photo_id      | int64   |               |
| language      | string  |               |
| last_updated  | int64   |               |

**conversations**

| Field      | Type    | Index                  |
|------------|---------|------------------------|
| chat_id    | int64   | compound (chat_id, user_id) |
| user_id    | int64   |                        |
| name       | string  |                        |
| step       | int     |                        |
| data       | []byte  |                        |
| created_at | int64   |                        |
| updated_at | int64   |                        |

## Interfaces

```go
var _ storage.Adapter           = (*MongoDB)(nil)
var _ storage.ConversationStore = (*MongoDB)(nil)
```
