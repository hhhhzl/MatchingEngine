package main

import (
	"flag"
	"log"
	"time"

	"github.com/marginforest/go/services/execution-engine/internal/engine"
	"github.com/marginforest/go/services/execution-engine/internal/store"
	"github.com/marginforest/go/services/execution-engine/internal/wire"
)

func main() {
	var (
		gatewayAddr = flag.String("gateway", "127.0.0.1:9000", "Event gateway TCP addr")
		storeDir    = flag.String("store", ".cache/execution_engine", "Local durable store dir")

		intentTopic      = flag.String("intent", "v1.exec.intent", "Input topic for EXEC_INTENT_PUBLISHED")
		planTopic        = flag.String("plan", "v1.exec.order_plan", "Output topic for ORDER_PLAN_UPDATED")
		orderCmdTopic    = flag.String("orders", "v1.order.cmd.request", "Output topic for ORDER_CMD (pre-risk)")
		auditTopic       = flag.String("audit", "v1.audit", "Output topic for AUDIT events")
		execMetricsTopic = flag.String("exec_metrics", "v1.exec.metrics", "Output topic for EXEC_METRIC events (optional)")

		mdTopic       = flag.String("md", "v1.marketdata.tick", "Marketdata topic")
		execTopic     = flag.String("exec", "v1.exec.report", "Execution report topic")
		snapshotTopic = flag.String("snapshot", "v1.snapshot", "Portfolio snapshot topic")
		riskTopic     = flag.String("risk", "v1.risk.event", "Risk events topic")

		runID       = flag.String("run_id", "exec-engine", "Run ID (used when intent.run_id is empty)")
		codeVersion = flag.String("code_version", "dev", "Code version tag in envelopes")

		httpAddr = flag.String("http", "127.0.0.1:9320", "HTTP addr for status/metrics")

		decisionMS = flag.Int("decision_ms", 200, "Decision interval (ms)")
		sliceMS    = flag.Int("slice_ms", 1000, "Default slice interval for TWAP-style plans (ms)")

		killSwitchURL = flag.String("kill_switch", "", "Kill switch base URL (e.g. http://127.0.0.1:9102)")

		obsURL = flag.String("obs_url", "", "Observability service base URL (e.g. http://127.0.0.1:9400)")
		orgID  = flag.String("org_id", "", "Org ID header for observability/release-controller")
		userID = flag.String("user_id", "", "User ID header for observability/release-controller")

		guardrailsPath       = flag.String("guardrails", "", "Path to guardrails_execution.yaml (optional)")
		releaseControllerURL = flag.String("release_controller_url", "", "Release-controller base URL (optional)")

		microSidecar     = flag.String("micro_sidecar", "", "Path to execution-micro sidecar binary (optional)")
		instrumentsPath  = flag.String("instruments", "", "Path to instrument precision YAML (optional)")
		costParamsDir    = flag.String("cost_params_dir", "", "Root dir of cost params store v2 (optional)")
		costParamsVersion = flag.String("cost_params_version", "", "Cost params version to use (optional; default latest)")
		costBucket       = flag.String("cost_bucket", "all", "Cost params bucket key")
	)
	flag.Parse()

	st, err := store.Open(*storeDir)
	if err != nil {
		log.Fatalf("failed to open store: %v", err)
	}
	defer func() { _ = st.Close() }()

	gw := wire.NewGatewayClient(*gatewayAddr)
	cfg := engine.Config{
		RunID:       *runID,
		CodeVersion: *codeVersion,

		IntentTopic:      *intentTopic,
		PlanTopic:        *planTopic,
		OrderCmdTopic:    *orderCmdTopic,
		AuditTopic:       *auditTopic,
		ExecMetricsTopic: *execMetricsTopic,
		ExecReportTopic:  *execTopic,
		MDTopic:          *mdTopic,
		SnapshotTopic:    *snapshotTopic,
		RiskTopic:        *riskTopic,

		HTTPAddr: *httpAddr,

		DecisionInterval: time.Duration(*decisionMS) * time.Millisecond,
		SliceInterval:    time.Duration(*sliceMS) * time.Millisecond,
		KillSwitchURL:    *killSwitchURL,

		ObservabilityURL: *obsURL,
		OrgID:            *orgID,
		UserID:           *userID,

		GuardrailsPath:       *guardrailsPath,
		ReleaseControllerURL: *releaseControllerURL,

		MicroSidecarPath:  *microSidecar,
		InstrumentsPath:   *instrumentsPath,
		CostParamsDir:     *costParamsDir,
		CostParamsVersion: *costParamsVersion,
		CostBucket:        *costBucket,
	}

	eng := engine.New(cfg, gw, st)
	if err := eng.Recover(); err != nil {
		log.Fatalf("failed to recover: %v", err)
	}
	log.Printf("execution-engine starting (gateway=%s, intent=%s, orders=%s)", *gatewayAddr, *intentTopic, *orderCmdTopic)
	eng.Run()
}

