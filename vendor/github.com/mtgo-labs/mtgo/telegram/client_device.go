package telegram

import (
	"time"

	"github.com/mtgo-labs/mtgo/internal/storage"
	"github.com/mtgo-labs/mtgo/telegram/types"
)

// initDeviceStorage persists or loads the device profile to/from storage.
//
// On first run: saves the current DeviceConfig so the device identity
// survives restarts. On subsequent runs: loads the saved profile and
// overrides the config, ensuring the session always presents the same
// device identity to Telegram.
//
// This is a no-op when Storage is nil or doesn't support DeviceStore.
func (c *Client) initDeviceStorage() {
	if c.cfg.Storage == nil {
		return
	}

	ds, ok := c.cfg.Storage.(storage.DeviceStore)
	if !ok {
		return
	}

	key := c.cfg.SessionName
	if key == "" {
		key = "default"
	}

	// Try to load an existing device profile.
	entry, err := ds.LoadDevice(key)
	if err != nil {
		c.Log.Debugf("device storage: load error for %q: %v", key, err)
		return
	}
	if entry != nil {
		// Override config with the persisted device identity.
		c.cfg.Device = DeviceConfig{
			DeviceModel:    entry.DeviceModel,
			SystemVersion:  entry.SystemVersion,
			AppVersion:     entry.AppVersion,
			LangCode:       entry.LangCode,
			SystemLangCode: entry.SystemLangCode,
			LangPack:       entry.LangPack,
			ClientPlatform: types.ClientPlatform(entry.Platform),
		}
		c.Log.Debugf("device storage: loaded profile for %q (%s)", key, entry.DeviceModel)
		return
	}

	// No saved profile — persist the current one.
	now := time.Now().Unix()
	if err := ds.SaveDevice(&storage.DeviceEntry{
		Key:            key,
		DeviceModel:    c.cfg.Device.DeviceModel,
		SystemVersion:  c.cfg.Device.SystemVersion,
		AppVersion:     c.cfg.Device.AppVersion,
		LangCode:       c.cfg.Device.LangCode,
		SystemLangCode: c.cfg.Device.SystemLangCode,
		LangPack:       c.cfg.Device.LangPack,
		Platform:       string(c.cfg.Device.ClientPlatform),
		CreatedAt:      now,
		UpdatedAt:      now,
	}); err != nil {
		c.Log.Debugf("device storage: save error for %q: %v", key, err)
	}
}
