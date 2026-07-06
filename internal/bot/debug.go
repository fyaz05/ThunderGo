package bot

import "runtime"

// debugStack returns a goroutine stack trace for panic recovery logging.
func debugStack() []byte {
	buf := make([]byte, 16384)
	for {
		n := runtime.Stack(buf, false)
		if n < len(buf) {
			return buf[:n]
		}
		buf = make([]byte, len(buf)*2)
	}
}
