# sqlite

SQLite storage adapter for [mtgo] Telegram clients. Pure-Go via [modernc.org/sqlite] — no CGO required.

## Install

```bash
go get github.com/mtgo-labs/storage/sqlite
```

## Usage

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

`Open` creates the database file if it doesn't exist and initializes the `sessions` and `peers` tables. The `conversations` table is created lazily on first use.

## Exporting a Session String

```go
str, err := sqlite.ExportSessionString(sess)
```

Encodes session data in Telethon/Pyrogram format:

```
"1" + base64url( dc_id[1B] + api_id[4B] + test_mode[1B] + user_id[8B] + is_bot[1B] + auth_key[256B] )
```

## Schema

**sessions**

| Column          | Type    |
|-----------------|---------|
| dc_id           | INTEGER |
| api_id          | INTEGER |
| api_hash        | TEXT    |
| test_mode       | INTEGER |
| auth_key        | BLOB    |
| state           | BLOB    |
| user_id         | INTEGER |
| is_bot          | INTEGER |
| first_name      | TEXT    |
| last_name       | TEXT    |
| username        | TEXT    |
| date            | INTEGER |
| server_address  | TEXT    |
| port            | INTEGER |

**peers**

| Column        | Type    | Index         |
|---------------|---------|---------------|
| id            | INTEGER | PRIMARY KEY   |
| type          | INTEGER |               |
| access_hash   | INTEGER |               |
| username      | TEXT    | idx_peers_username |
| usernames     | TEXT    |               |
| first_name    | TEXT    |               |
| last_name     | TEXT    |               |
| phone_number  | TEXT    | idx_peers_phone   |
| is_bot        | INTEGER |               |
| photo_id      | INTEGER |               |
| language      | TEXT    |               |
| last_updated  | INTEGER |               |

**conversations** (created lazily)

| Column     | Type    |
|------------|---------|
| chat_id    | INTEGER |
| user_id    | INTEGER |
| name       | TEXT    |
| step       | INTEGER |
| data       | BLOB    |
| created_at | INTEGER |
| updated_at | INTEGER |

## Interfaces

```go
var _ storage.Adapter           = (*SQLite)(nil)
var _ storage.ConversationStore = (*SQLite)(nil)
```
