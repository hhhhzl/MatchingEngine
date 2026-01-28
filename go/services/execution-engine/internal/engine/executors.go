package engine

import (
	"math"
	"time"

	"github.com/marginforest/go/services/execution-engine/internal/wire"
)

// Executor is the plugin interface for execution algorithms.
//
// v1 scope:
// - Deterministic schedule (desired cumulative quantity vs time/market volume).
// - Deterministic plan defaults (price strategy, cancel/replace, fault tolerance).
//
// The engine handles the common concerns:
// - Idempotency, persistence, restart recovery.
// - Rate limiting, kill-switch mode linkage.
// - Child order lifecycle tracking via EXEC_REPORT.
type Executor interface {
	Name() string
	BuildPlan(intent wire.ExecutionIntent, nowNS int64, sliceInterval time.Duration) wire.OrderPlan
	DesiredCumQty(it *intentRuntime, nowNS int64) float64
}

func defaultExecutors() map[string]Executor {
	return map[string]Executor{
		"twap":      twapExecutor{},
		"vwap":      vwapExecutor{},
		"pov":       povExecutor{},
		"is_min_v1": isMinV1Executor{},
	}
}

type twapExecutor struct{}

func (twapExecutor) Name() string { return "twap" }
func (twapExecutor) BuildPlan(intent wire.ExecutionIntent, nowNS int64, sliceInterval time.Duration) wire.OrderPlan {
	intent.ExecutionAlgo = "twap"
	return buildPlan(intent, nowNS, sliceInterval)
}
func (twapExecutor) DesiredCumQty(it *intentRuntime, nowNS int64) float64 {
	start := float64(it.Intent.TimeWindow.StartTSNS)
	end := float64(it.Intent.TimeWindow.EndTSNS)
	if end <= start {
		return it.TargetQty
	}
	t := float64(nowNS)
	progress := (t - start) / (end - start)
	progress = math.Max(0, math.Min(1, progress))
	return it.TargetQty * progress
}

type povExecutor struct{}

func (povExecutor) Name() string { return "pov" }
func (povExecutor) BuildPlan(intent wire.ExecutionIntent, nowNS int64, sliceInterval time.Duration) wire.OrderPlan {
	intent.ExecutionAlgo = "pov"
	return buildPlan(intent, nowNS, sliceInterval)
}
func (povExecutor) DesiredCumQty(it *intentRuntime, _ int64) float64 {
	p := it.Intent.Constraints.MaxParticipation
	if p <= 0 {
		p = 0.05
	}
	return math.Min(it.TargetQty, p*it.CumMDVolume)
}

type vwapExecutor struct{}

func (vwapExecutor) Name() string { return "vwap" }
func (vwapExecutor) BuildPlan(intent wire.ExecutionIntent, nowNS int64, sliceInterval time.Duration) wire.OrderPlan {
	intent.ExecutionAlgo = "vwap"
	return buildPlan(intent, nowNS, sliceInterval)
}
func (vwapExecutor) DesiredCumQty(it *intentRuntime, nowNS int64) float64 {
	start := float64(it.Intent.TimeWindow.StartTSNS)
	end := float64(it.Intent.TimeWindow.EndTSNS)
	if end <= start {
		return it.TargetQty
	}
	if it.FirstMDTSNS == 0 || it.CumMDVolume <= 0 {
		return twapExecutor{}.DesiredCumQty(it, nowNS)
	}
	elapsedSec := (float64(nowNS-it.FirstMDTSNS) / 1e9)
	if elapsedSec <= 0 {
		return twapExecutor{}.DesiredCumQty(it, nowNS)
	}
	rate := it.CumMDVolume / elapsedSec
	totalDurSec := (end - start) / 1e9
	estTotalVol := math.Max(it.CumMDVolume, rate*totalDurSec)
	if estTotalVol <= 0 {
		return twapExecutor{}.DesiredCumQty(it, nowNS)
	}
	return math.Min(it.TargetQty, it.TargetQty*(it.CumMDVolume/estTotalVol))
}

type isMinV1Executor struct{}

func (isMinV1Executor) Name() string { return "is_min_v1" }
func (isMinV1Executor) BuildPlan(intent wire.ExecutionIntent, nowNS int64, sliceInterval time.Duration) wire.OrderPlan {
	intent.ExecutionAlgo = "is_min_v1"
	return buildPlan(intent, nowNS, sliceInterval)
}
func (isMinV1Executor) DesiredCumQty(it *intentRuntime, nowNS int64) float64 {
	start := float64(it.Intent.TimeWindow.StartTSNS)
	end := float64(it.Intent.TimeWindow.EndTSNS)
	if end <= start {
		return it.TargetQty
	}
	t := float64(nowNS)
	progress := (t - start) / (end - start)
	progress = math.Max(0, math.Min(1, progress))
	alpha := 1.0
	switch it.Intent.Urgency {
	case "high":
		alpha = 0.8
	case "urgent":
		alpha = 0.65
	}
	return it.TargetQty * math.Pow(progress, alpha)
}

