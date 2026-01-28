package engine

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"strings"
	"time"
)

// obsClient is a best-effort asynchronous client for the observability-service HTTP API.
// It MUST NOT block the hot execution path; requests are dropped if the queue is full.
type obsClient struct {
	baseURL string
	orgID   string
	userID  string

	c *http.Client

	ch chan any
}

type obsMetricReq struct {
	MetricName string                 `json:"metric_name"`
	Value      float64                `json:"value"`
	Tags       map[string]string      `json:"tags,omitempty"`
	Timestamp  int64                  `json:"timestamp,omitempty"`
	Metadata   map[string]interface{} `json:"metadata,omitempty"`
}

type obsSpanReq struct {
	TraceID       string                 `json:"trace_id"`
	SpanID        string                 `json:"span_id"`
	ParentSpanID  string                 `json:"parent_span_id,omitempty"`
	SpanKind      string                 `json:"span_kind,omitempty"`
	OperationName string                 `json:"operation_name"`
	StartTime     int64                  `json:"start_time"`
	EndTime       int64                  `json:"end_time"`
	Status        string                 `json:"status,omitempty"`
	Metadata      map[string]interface{} `json:"metadata,omitempty"`
}

func newObsClient(baseURL, orgID, userID string) *obsClient {
	baseURL = strings.TrimRight(baseURL, "/")
	if baseURL == "" || orgID == "" || userID == "" {
		return nil
	}
	oc := &obsClient{
		baseURL: baseURL,
		orgID:   orgID,
		userID:  userID,
		c: &http.Client{
			Timeout: 250 * time.Millisecond,
		},
		ch: make(chan any, 2048),
	}
	go oc.loop()
	return oc
}

func (o *obsClient) loop() {
	for msg := range o.ch {
		switch v := msg.(type) {
		case obsMetricReq:
			_ = o.postJSON("/api/v1/metrics", v)
		case obsSpanReq:
			_ = o.postJSON("/api/v1/traces/spans", v)
		}
	}
}

func (o *obsClient) postJSON(path string, body any) error {
	b, err := json.Marshal(body)
	if err != nil {
		return err
	}
	req, err := http.NewRequestWithContext(context.Background(), http.MethodPost, o.baseURL+path, bytes.NewReader(b))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-Org-ID", o.orgID)
	req.Header.Set("X-User-ID", o.userID)
	resp, err := o.c.Do(req)
	if err != nil {
		return err
	}
	_ = resp.Body.Close()
	return nil
}

func (o *obsClient) RecordMetric(name string, value float64, tags map[string]string, tsNS int64, meta map[string]interface{}) {
	if o == nil {
		return
	}
	select {
	case o.ch <- obsMetricReq{MetricName: name, Value: value, Tags: tags, Timestamp: tsNS, Metadata: meta}:
	default:
		// Drop on overload to protect execution.
	}
}

func (o *obsClient) RecordSpan(traceID, spanID, parentSpanID, op string, startNS, endNS int64, status string, meta map[string]interface{}) {
	if o == nil {
		return
	}
	select {
	case o.ch <- obsSpanReq{
		TraceID:       traceID,
		SpanID:        spanID,
		ParentSpanID:  parentSpanID,
		SpanKind:      "INTERNAL",
		OperationName: op,
		StartTime:     startNS,
		EndTime:       endNS,
		Status:        status,
		Metadata:      meta,
	}:
	default:
	}
}

