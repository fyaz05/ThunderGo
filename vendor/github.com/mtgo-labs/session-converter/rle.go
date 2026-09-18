package tgconv

// RLE encoding used by MTKruto.
//
// Encodes runs of zero bytes as 0x00 <count>. Non-zero bytes pass through
// unchanged. Runs longer than 255 are split into multiple 0x00 <255> pairs.

// rleEncode RLE-encodes data.
func rleEncode(data []byte) []byte {
	result := make([]byte, 0, len(data))
	var n int

	for _, b := range data {
		if b == 0 {
			if n == 255 {
				result = append(result, 0, byte(n))
				n = 1
			} else {
				n++
			}
		} else {
			if n > 0 {
				result = append(result, 0, byte(n))
				n = 0
			}
			result = append(result, b)
		}
	}

	if n > 0 {
		result = append(result, 0, byte(n))
	}

	return result
}

// rleDecode RLE-decodes data.
func rleDecode(data []byte) []byte {
	result := make([]byte, 0, len(data))
	z := false

	for _, b := range data {
		if b == 0 {
			z = true
			continue
		}
		if z {
			for range int(b) {
				result = append(result, 0)
			}
			z = false
		} else {
			result = append(result, b)
		}
	}

	return result
}
