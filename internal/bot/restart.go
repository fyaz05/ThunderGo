package bot

import (
	"context"
	"time"

	"github.com/amarnathcjd/gogram/telegram"
)

// EditRestartMarkerIfPending checks the store for a pending restart marker.
// If one exists and is less than an hour old, it edits the original
// "Restarting…" message to "Restart Successful" and deletes the marker.
// Called once at startup.
func (b *Bot) EditRestartMarkerIfPending(ctx context.Context) error {
	marker, err := b.Store.PopRestartMarker(ctx)
	if err != nil {
		b.Log.Warn("checking restart marker", "error", err)
		return err
	}
	if marker == nil {
		return nil
	}
	if time.Since(marker.CreatedAt) > time.Hour {
		b.Log.Warn("stale restart marker; not editing", "age", time.Since(marker.CreatedAt))
		return nil
	}
	primary := b.Pool.Primary()
	if primary == nil {
		b.Log.Warn("restart marker: no primary client")
		return nil
	}
	_, err = primary.EditMessage(marker.ChatID, marker.MessageID,
		msgRestartSuccess,
		&telegram.SendOptions{ParseMode: "HTML"})
	if err != nil {
		b.Log.Warn("editing restart marker failed", "chat_id", marker.ChatID, "msg_id", marker.MessageID, "error", err)
		return nil
	}
	b.Log.Info("restart marker edited", "chat_id", marker.ChatID, "msg_id", marker.MessageID)
	return nil
}
