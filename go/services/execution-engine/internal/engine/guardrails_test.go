package engine

import "testing"

func TestGuardrailsDegradeOnSlippage(t *testing.T) {
	gr := newGuardrails("", "", "", "")
	gr.cfg.Thresholds.SlippageP95BpsMax = 1.0
	gr.cfg.Actions.OnSlippage = "degrade"
	gr.cfg.DegradeLadder = []string{"is_min_v1", "pov", "twap"}
	gr.cfg.WindowSize = 10

	intentID := "i1"
	for i := 0; i < 10; i++ {
		gr.observeSlippage(intentID, 5.0)
	}
	act := gr.evaluate(intentID)
	if act == nil || act.Kind != "degrade" {
		t.Fatalf("expected degrade action, got %+v", act)
	}
	if act.DegradeTo == "" {
		t.Fatalf("expected degrade target")
	}
}

