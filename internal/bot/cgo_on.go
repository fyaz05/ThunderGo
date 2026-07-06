//go:build cgo

package bot

// cgoEnabled reports whether the binary was built with CGO. Surfaced via /stats.
const cgoEnabled = "enabled"
