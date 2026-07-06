package bot

import (
	"fmt"
	"regexp"
)

// commandNameRe enforces Telegram's command-name format: 1–32 chars of [a-z0-9_].
var commandNameRe = regexp.MustCompile(`^[a-z0-9_]{1,32}$`)

// validateCommandName returns an error if name is not a valid bot command name.
func validateCommandName(name string) error {
	if !commandNameRe.MatchString(name) {
		return fmt.Errorf("invalid command name %q: must match ^[a-z0-9_]{1,32}$", name)
	}
	return nil
}

// registerBuiltinCommands wires built-in commands to the dispatcher.
func (b *Bot) registerBuiltinCommands() {
	all := []*Command{
		// Public commands.
		{Name: "start", Description: "Start the bot and register", Handler: b.handleStart},
		{Name: "help", Description: "Show usage instructions", Handler: b.handleHelp},
		{Name: "ping", Description: "Measure round-trip latency", Handler: b.handlePing},
		{Name: "dc", Description: "Show the data center of a file or user", Handler: b.handleDc},
		{Name: "link", Description: "Generate a link for a media message (group)", Handler: b.handleLink},
		{Name: "about", Description: "About this bot", Handler: b.handleAbout},

		// Owner-only commands (silently ignored for non-owners).
		{Name: "ban", Description: "Ban a user or channel", OwnerOnly: true, Handler: b.handleBan},
		{Name: "unban", Description: "Unban a user or channel", OwnerOnly: true, Handler: b.handleUnban},
		{Name: "broadcast", Description: "Broadcast a message to all users", OwnerOnly: true, Handler: b.handleBroadcast},
		{Name: "authorize", Description: "Add a user to the allowlist", OwnerOnly: true, Handler: b.handleAuthorize},
		{Name: "deauthorize", Description: "Remove a user from the allowlist", OwnerOnly: true, Handler: b.handleDeauthorize},
		{Name: "listauth", Description: "List authorized users", OwnerOnly: true, Handler: b.handleListAuth},
		{Name: "status", Description: "Show bot health (owner)", OwnerOnly: true, Handler: b.handleStatus},
		{Name: "users", Description: "Show total user count (owner)", OwnerOnly: true, Handler: b.handleUsers},
		{Name: "stats", Description: "Show runtime statistics", OwnerOnly: true, Handler: b.handleStats},
		{Name: "log", Description: "Send the current log file (owner)", OwnerOnly: true, Handler: b.handleLog},
		{Name: "restart", Description: "Restart the bot (owner)", OwnerOnly: true, Handler: b.handleRestart},
	}
	for _, c := range all {
		if err := validateCommandName(c.Name); err != nil {
			b.Log.Error("invalid builtin command name; skipping registration", "name", c.Name, "error", err)
			continue
		}
		b.Register(c)
	}
}
