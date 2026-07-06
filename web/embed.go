// Package web embeds the player-page assets into the binary via go:embed.
package web

import "embed"

//go:embed templates
var AssetsFS embed.FS
