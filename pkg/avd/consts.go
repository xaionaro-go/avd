// consts.go defines constants used throughout the avd package.

package avd

import "time"

const (
	SizeBuffer = 65536

	// ReorderMaxBufferSize is the maximum number of packets buffered while
	// waiting for out-of-order DTS packets. Higher values tolerate longer
	// bursts of reordering but consume more memory. 10000 packets is
	// roughly 3-4 seconds of buffering at typical multi-stream bitrates.
	ReorderMaxBufferSize = 10000

	// ReorderMaxDTSDifference is the maximum DTS gap between streams before
	// the reorderer flushes anyway. Too low (e.g. 1ms) triggers false
	// positives on every packet since normal inter-frame intervals are
	// ~21ms at 48fps. Too high misses genuine stream restarts. 1s balances
	// tolerance for all standard frame rates (down to 1fps) while still
	// detecting real discontinuities (which are typically seconds-to-minutes).
	ReorderMaxDTSDifference = time.Second

	// ReorderDiscardUnordered controls whether packets arriving after the
	// reorder window has flushed are silently dropped (true) or passed
	// through out-of-order (false). Dropping prevents downstream decoders
	// from receiving stale data at the cost of occasional frame loss.
	ReorderDiscardUnordered = true
)
