package main

import (
	"encoding/json"
	"os"
	"testing"
)

func TestSchemasAreValidJSON(t *testing.T) {
	paths := []string{
		"../../../../../marginforest/specs/execution_intent.jsonschema",
		"../../../../../marginforest/specs/order_plan.jsonschema",
	}
	for _, p := range paths {
		raw, err := os.ReadFile(p)
		if err != nil {
			t.Fatalf("failed to read schema %s: %v", p, err)
		}
		var obj map[string]any
		if err := json.Unmarshal(raw, &obj); err != nil {
			t.Fatalf("schema %s is not valid JSON: %v", p, err)
		}
		if obj["$schema"] == nil {
			t.Fatalf("schema %s missing $schema", p)
		}
		if obj["$id"] == nil {
			t.Fatalf("schema %s missing $id", p)
		}
	}
}

