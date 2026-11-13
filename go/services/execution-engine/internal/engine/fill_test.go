package engine

import "testing"

// Two partial fills of 30 and 70 must sum to 100. Under the old cumulative
// interpretation the second report (70 < 30+70) was read as a total and only
// 40 more was credited.
func TestApplyFillIsIncremental(t *testing.T) {
	it := &intentRuntime{TargetQty: 100}
	ch := &childOrder{Qty: 100}

	applyFill(it, ch, 30)
	applyFill(it, ch, 70)

	if ch.FilledQty != 100 || it.FilledQty != 100 {
		t.Fatalf("child=%v intent=%v, want 100/100", ch.FilledQty, it.FilledQty)
	}
}

func TestApplyFillCapsAtChildQty(t *testing.T) {
	it := &intentRuntime{TargetQty: 100}
	ch := &childOrder{Qty: 100}

	applyFill(it, ch, 100)
	applyFill(it, ch, 100) // duplicated report

	if ch.FilledQty != 100 || it.FilledQty != 100 {
		t.Fatalf("child=%v intent=%v, want 100/100", ch.FilledQty, it.FilledQty)
	}
}

func TestApplyFillIgnoresNonPositive(t *testing.T) {
	it := &intentRuntime{}
	ch := &childOrder{Qty: 10}

	applyFill(it, ch, 0)
	applyFill(it, ch, -5)

	if ch.FilledQty != 0 || it.FilledQty != 0 {
		t.Fatalf("child=%v intent=%v, want 0/0", ch.FilledQty, it.FilledQty)
	}
}
