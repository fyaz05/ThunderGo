package telegram

import "github.com/mtgo-labs/mtgo/tg"

// Lifecycle hook callback types. Plugins register these to observe
// client state transitions without coupling the core to plugin logic.

type (
	// UpdateReceivedHook fires for every incoming update batch (tg.UpdatesClass),
	// before handler dispatch. Hooks run synchronously in the update receive
	// goroutine and MUST be non-blocking — offload expensive work to an
	// internal queue or goroutine.
	UpdateReceivedHook func(c *Client, updates tg.UpdatesClass)

	// SessionLoadedHook fires when session data is restored from storage,
	// early in postConnect and before plugins start. Use to load persisted
	// state (e.g. cached update sequence numbers).
	SessionLoadedHook func(c *Client)

	// ConnectedHook fires after the client is fully connected and
	// authenticated, after plugins have started.
	ConnectedHook func(c *Client)

	// ReconnectHook fires after a successful reconnection. Use to trigger
	// gap recovery for updates that may have been missed while disconnected.
	ReconnectHook func(c *Client)
)

// OnUpdateReceived registers a hook fired for every incoming update batch.
// Multiple hooks may be registered; they fire in registration order.
func (c *Client) OnUpdateReceived(hook UpdateReceivedHook) {
	c.hooksMu.Lock()
	c.updateReceivedHooks = append(c.updateReceivedHooks, hook)
	c.hooksMu.Unlock()
}

// OnSessionLoaded registers a hook fired when session data is restored.
func (c *Client) OnSessionLoaded(hook SessionLoadedHook) {
	c.hooksMu.Lock()
	c.sessionLoadedHooks = append(c.sessionLoadedHooks, hook)
	c.hooksMu.Unlock()
}

// OnConnected registers a hook fired after the client connects.
func (c *Client) OnConnected(hook ConnectedHook) {
	c.hooksMu.Lock()
	c.connectedHooks = append(c.connectedHooks, hook)
	c.hooksMu.Unlock()
}

// OnReconnect registers a hook fired after a successful reconnection.
func (c *Client) OnReconnect(hook ReconnectHook) {
	c.hooksMu.Lock()
	c.reconnectHooks = append(c.reconnectHooks, hook)
	c.hooksMu.Unlock()
}

// fireUpdateReceived dispatches the raw update batch to all registered hooks.
// Called from the session receive goroutine — hooks must be fast.
func (c *Client) fireUpdateReceived(updates tg.UpdatesClass) {
	c.hooksMu.RLock()
	hooks := c.updateReceivedHooks
	c.hooksMu.RUnlock()
	for _, h := range hooks {
		h(c, updates)
	}
}

func (c *Client) fireSessionLoaded() {
	c.hooksMu.RLock()
	hooks := c.sessionLoadedHooks
	c.hooksMu.RUnlock()
	for _, h := range hooks {
		h(c)
	}
}

func (c *Client) fireConnected() {
	c.hooksMu.RLock()
	hooks := c.connectedHooks
	c.hooksMu.RUnlock()
	for _, h := range hooks {
		h(c)
	}
}

func (c *Client) fireReconnect() {
	c.hooksMu.RLock()
	hooks := c.reconnectHooks
	c.hooksMu.RUnlock()
	for _, h := range hooks {
		h(c)
	}
}
