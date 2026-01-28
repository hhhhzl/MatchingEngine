package engine

import (
	"strconv"
)

func f64s(v float64) string {
	// Stable decimal encoding for rust_decimal serde (string-based).
	return strconv.FormatFloat(v, 'f', -1, 64)
}

