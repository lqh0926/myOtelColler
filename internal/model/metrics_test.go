package model

import (
	"testing"
	"time"
)

func TestSupportedEnumsAreValid(t *testing.T) {
	for _, priority := range []Priority{PriorityLow, PriorityHigh} {
		if !priority.Valid() {
			t.Errorf("Priority(%d).Valid() = false", priority)
		}
	}
	if Priority(2).Valid() {
		t.Error("Priority(2).Valid() = true, want false")
	}

	for _, metricType := range []MetricType{MetricTypeGauge, MetricTypeCounter} {
		if !metricType.Valid() {
			t.Errorf("MetricType(%d).Valid() = false", metricType)
		}
	}
	if MetricType(2).Valid() {
		t.Error("MetricType(2).Valid() = true, want false")
	}
}

func TestMetricBatchRepresentsMinimalModel(t *testing.T) {
	timestamp := time.Unix(1_700_000_000, 0)
	batch := MetricBatch{
		Priority:           PriorityHigh,
		ResourceAttributes: Attributes{"service.name": "raft-node"},
		ScopeName:          "raft.telemetry",
		ScopeVersion:       "v1",
		Metrics: []Metric{{
			Name:        "raft_term",
			Description: "Current Raft term",
			Unit:        "1",
			Type:        MetricTypeGauge,
			DataPoints: []DataPoint{{
				Attributes: Attributes{"node": "n1"},
				Timestamp:  timestamp,
				Value:      7,
			}},
		}},
	}

	if got := batch.Metrics[0].DataPoints[0].Value; got != 7 {
		t.Fatalf("Value = %v, want 7", got)
	}
	if got := batch.ResourceAttributes["service.name"]; got != "raft-node" {
		t.Fatalf("resource service.name = %q, want raft-node", got)
	}
	if got := batch.Metrics[0].DataPoints[0].Timestamp; !got.Equal(timestamp) {
		t.Fatalf("Timestamp = %v, want %v", got, timestamp)
	}
}
