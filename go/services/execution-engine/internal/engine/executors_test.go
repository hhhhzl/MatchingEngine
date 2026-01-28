package engine

import (
	"testing"
	"time"

	"github.com/marginforest/go/services/execution-engine/internal/wire"
)

func TestExecutorsDeterministic(t *testing.T) {
	now := time.Now().UnixNano()
	intent := wire.ExecutionIntent{
		Version:    "v1",
		IntentID:   "i1",
		RunID:      "r1",
		StrategyID: "s1",
		AccountScope: wire.AccountScope{
			Mode:      "single_account",
			AccountID: "paper-acc-001",
		},
		InstrumentID: wire.InstrumentID{Symbol: "AAPL", Venue: "NASDAQ"},
		Side:         "buy",
		Target:       wire.Target{TargetQty: &wire.Quantity{Value: 100, Scale: 0}},
		TimeWindow:   wire.TimeWindow{StartTSNS: now, EndTSNS: now + int64(10*time.Second)},
		Urgency:      "normal",
		Constraints: wire.Constraints{
			MaxParticipation:  0.1,
			MaxSlippageBps:    10,
			AllowMarketOrders: false,
			ReduceOnly:        false,
		},
		Benchmark:  wire.Benchmark{Type: "arrival_price"},
		AlgoParams: map[string]any{},
	}

	it := &intentRuntime{
		Intent:      intent,
		TargetQty:   100,
		CumMDVolume: 500,
		FirstMDTSNS: now,
	}

	execs := defaultExecutors()
	for name, ex := range execs {
		it.Intent.ExecutionAlgo = name
		a := ex.DesiredCumQty(it, now+int64(5*time.Second))
		b := ex.DesiredCumQty(it, now+int64(5*time.Second))
		if a != b {
			t.Fatalf("executor %s not deterministic: %v vs %v", name, a, b)
		}
	}
}

