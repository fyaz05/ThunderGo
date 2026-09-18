package tgconv

import (
	"encoding/binary"
	"fmt"
)

// TL serialization helpers — used by mtcute and MTKruto formats.

// tlBytes encodes data using TL bytes serialization (length-prefixed, 4-byte
// aligned).
func tlBytes(data []byte) []byte {
	length := len(data)
	buf := make([]byte, 0, length+8)

	if length <= 253 {
		buf = append(buf, byte(length))
	} else {
		buf = append(buf, 0xFE)
		buf = append(buf, byte(length), byte(length>>8), byte(length>>16))
	}

	buf = append(buf, data...)

	padLen := (4 - (len(buf) % 4)) % 4
	for range padLen {
		buf = append(buf, 0)
	}

	return buf
}

// readTLBytes reads TL-encoded bytes starting at offset, returning the data
// and the new offset (aligned to 4 bytes).
func readTLBytes(data []byte, off int) ([]byte, int, error) {
	if off >= len(data) {
		return nil, 0, fmt.Errorf("offset %d out of bounds (len %d)", off, len(data))
	}

	startOff := off
	var length int

	if data[off] <= 253 {
		length = int(data[off])
		off++
	} else {
		off++
		if off+3 > len(data) {
			return nil, 0, fmt.Errorf("not enough bytes for TL length prefix")
		}
		length = int(data[off]) | int(data[off+1])<<8 | int(data[off+2])<<16
		off += 3
	}

	if off+length > len(data) {
		return nil, 0, fmt.Errorf("TL bytes extend past payload (need %d, have %d)", length, len(data)-off)
	}

	result := data[off : off+length]
	off += length

	// Advance past padding to next 4-byte boundary from startOff.
	entryEnd := startOff + ((off - startOff) + 3) &^ 3
	if entryEnd > len(data) {
		entryEnd = off
	}

	return result, entryEnd, nil
}

// tlBoolTrue and tlBoolFalse are the TL boolean constants.
const (
	tlBoolTrue  uint32 = 0x997275B5
	tlBoolFalse uint32 = 0xBC799737
)

// tlBool encodes a bool as 4 TL bytes.
func tlBool(v bool) []byte {
	buf := make([]byte, 4)
	if v {
		binary.LittleEndian.PutUint32(buf, tlBoolTrue)
	} else {
		binary.LittleEndian.PutUint32(buf, tlBoolFalse)
	}
	return buf
}

// readTLBool reads a 4-byte TL bool.
func readTLBool(b []byte) bool {
	return binary.LittleEndian.Uint32(b) == tlBoolTrue
}

// isIPv6 reports whether addr contains a colon (heuristic for IPv6).
func isIPv6(addr string) bool {
	for i := 0; i < len(addr); i++ {
		if addr[i] == ':' {
			return true
		}
	}
	return false
}
