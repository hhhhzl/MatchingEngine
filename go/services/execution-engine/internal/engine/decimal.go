package engine

import (
	"encoding/json"
	"math"
)

func toFloat(v any) (float64, bool) {
	switch x := v.(type) {
	case float64:
		return x, true
	case int:
		return float64(x), true
	case int64:
		return float64(x), true
	case json.Number:
		f, err := x.Float64()
		return f, err == nil
	default:
		return 0, false
	}
}

// parseDecimal parses proto-like fixed-point objects:
// - Quantity: {"value": int64, "scale": int32}
// - Money:    {"value": int64, "scale": int32, "currency": "..."}
func parseDecimal(obj any) float64 {
	m, ok := obj.(map[string]interface{})
	if !ok {
		return 0
	}
	val, _ := toFloat(m["value"])
	scale, _ := toFloat(m["scale"])
	if scale < 0 {
		return val
	}
	return val / math.Pow10(int(scale))
}

