package matchingengine

import "testing"

func TestEngineSmoke(t *testing.T) {
	e, err := NewEngine([]string{"AAPL"})
	if err != nil {
		t.Fatalf("NewEngine: %v", err)
	}
	defer e.Close()

	// Rest a sell.
	ev1, err := e.SubmitOrderEvents(Order{
		ClientOrderID: "s1",
		Symbol:        "AAPL",
		Side:          1,
		OrderType:     1,
		TimeInForce:   0,
		Price:         100 * DecimalScale,
		Qty:           10 * DecimalScale,
		TimestampNS:   1,
	})
	if err != nil {
		t.Fatalf("SubmitOrderEvents sell: %v", err)
	}
	if len(ev1) == 0 {
		t.Fatalf("expected events")
	}

	// Cross with a buy.
	ev2, err := e.SubmitOrderEvents(Order{
		ClientOrderID: "b1",
		Symbol:        "AAPL",
		Side:          0,
		OrderType:     1,
		TimeInForce:   0,
		Price:         101 * DecimalScale,
		Qty:           10 * DecimalScale,
		TimestampNS:   2,
	})
	if err != nil {
		t.Fatalf("SubmitOrderEvents buy: %v", err)
	}
	if len(ev2) == 0 {
		t.Fatalf("expected events")
	}

	if ev2[0].Seq <= ev1[len(ev1)-1].Seq {
		t.Fatalf("expected monotonic seq")
	}
}

