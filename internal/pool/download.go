package pool

import (
	"errors"
	"math"
)

// DownloadChunkAligned is a defensive wrapper around gogram's DownloadChunk.
// It enforces that the chunkSize is a multiple of 4096 and a divisor of 1MiB,
// that start and end offsets are properly aligned to chunkSize, and prevents
// 32-bit int overflow on the offsets.
func (c *Client) DownloadChunkAligned(media any, start, end, chunkSize int) ([]byte, string, error) {
	if chunkSize <= 0 || chunkSize%4096 != 0 || (1<<20)%chunkSize != 0 {
		return nil, "", errors.New("chunkSize must be a divisor of 1MiB and a multiple of 4096")
	}
	if start < 0 || end < 0 {
		return nil, "", errors.New("start and end offsets must be non-negative")
	}
	if start%chunkSize != 0 {
		return nil, "", errors.New("start offset must be a multiple of chunkSize")
	}
	if start > math.MaxInt32-chunkSize {
		return nil, "", errors.New("start offset overflows MaxInt32")
	}

	return c.DownloadChunk(media, start, end, chunkSize)
}
