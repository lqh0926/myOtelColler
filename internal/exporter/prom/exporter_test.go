package prom

import (
	"context"
	"errors"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/lqh0926/myOtelColler/internal/model"
	"github.com/lqh0926/myOtelColler/internal/pipeline"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
)

var (
	_ pipeline.MetricsConsumer = (*Exporter)(nil)
	_ pipeline.Starter         = (*Exporter)(nil)
	_ pipeline.Shutdowner      = (*Exporter)(nil)
	_ prometheus.Collector     = (*Exporter)(nil)
)

func TestExporter_ExposesGaugeAndCounter(t *testing.T) {
	registry := prometheus.NewRegistry()
	exporter := newTestExporter(t, registry)
	timestamp := time.Unix(100, 0)
	batch := &model.MetricBatch{
		ResourceAttributes: model.Attributes{"service.name": "raft"},
		ScopeName:          "raft.telemetry",
		ScopeVersion:       "v1",
		Metrics: []model.Metric{
			{
				Name:        "raft.current-term",
				Description: "Current Raft term",
				Unit:        "1",
				Type:        model.MetricTypeGauge,
				DataPoints: []model.DataPoint{{
					Attributes: model.Attributes{"node.id": "n1"},
					Timestamp:  timestamp,
					Value:      7,
				}, {
					Attributes: model.Attributes{"node.id": "n2"},
					Timestamp:  timestamp,
					Value:      8,
				}},
			},
			{
				Name:        "raft_elections_total",
				Description: "Raft elections",
				Unit:        "{election}",
				Type:        model.MetricTypeCounter,
				DataPoints: []model.DataPoint{{
					Attributes: model.Attributes{"node.id": "n1"},
					Timestamp:  timestamp,
					Value:      3,
				}},
			},
		},
	}

	if err := exporter.ConsumeMetrics(context.Background(), batch); err != nil {
		t.Fatalf("ConsumeMetrics() error = %v", err)
	}

	want := `
# HELP raft_current_term Current Raft term
# TYPE raft_current_term gauge
raft_current_term{attribute_node_id="n1",otel_scope_name="raft.telemetry",otel_scope_version="v1",resource_service_name="raft"} 7
raft_current_term{attribute_node_id="n2",otel_scope_name="raft.telemetry",otel_scope_version="v1",resource_service_name="raft"} 8
# HELP raft_elections_total Raft elections
# TYPE raft_elections_total counter
raft_elections_total{attribute_node_id="n1",otel_scope_name="raft.telemetry",otel_scope_version="v1",resource_service_name="raft"} 3
`
	if err := testutil.GatherAndCompare(
		registry,
		strings.NewReader(want),
		"raft_current_term",
		"raft_elections_total",
	); err != nil {
		t.Fatalf("GatherAndCompare() error = %v", err)
	}
}

func TestExporter_IgnoresOlderSampleForSameLabels(t *testing.T) {
	registry := prometheus.NewRegistry()
	exporter := newTestExporter(t, registry)

	consumeOne := func(timestamp time.Time, value float64) {
		t.Helper()
		err := exporter.ConsumeMetrics(context.Background(), &model.MetricBatch{
			Metrics: []model.Metric{{
				Name: "temperature",
				Type: model.MetricTypeGauge,
				DataPoints: []model.DataPoint{{
					Timestamp: timestamp,
					Value:     value,
				}},
			}},
		})
		if err != nil {
			t.Fatalf("ConsumeMetrics() error = %v", err)
		}
	}

	consumeOne(time.Unix(200, 0), 20)
	consumeOne(time.Unix(100, 0), 10)

	want := `
# HELP temperature temperature
# TYPE temperature gauge
temperature{otel_scope_name="",otel_scope_version=""} 20
`
	if err := testutil.GatherAndCompare(registry, strings.NewReader(want), "temperature"); err != nil {
		t.Fatalf("GatherAndCompare() error = %v", err)
	}
}

func TestExporter_RejectsSchemaConflictsWithoutPartialUpdate(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*model.Metric)
		match  string
	}{
		{
			name: "type",
			mutate: func(metric *model.Metric) {
				metric.Type = model.MetricTypeCounter
			},
			match: "type",
		},
		{
			name: "description",
			mutate: func(metric *model.Metric) {
				metric.Description = "different"
			},
			match: "description",
		},
		{
			name: "unit",
			mutate: func(metric *model.Metric) {
				metric.Unit = "ms"
			},
			match: "unit",
		},
		{
			name: "label names",
			mutate: func(metric *model.Metric) {
				metric.DataPoints[0].Attributes = model.Attributes{"node": "n1"}
			},
			match: "label names",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			exporter := newTestExporter(t, prometheus.NewRegistry())
			base := model.Metric{
				Name:        "requests",
				Description: "Requests",
				Unit:        "{request}",
				Type:        model.MetricTypeGauge,
				DataPoints:  []model.DataPoint{{Value: 1}},
			}
			if err := exporter.ConsumeMetrics(context.Background(), &model.MetricBatch{
				Metrics: []model.Metric{base},
			}); err != nil {
				t.Fatalf("initial ConsumeMetrics() error = %v", err)
			}

			conflicting := base
			conflicting.DataPoints = append([]model.DataPoint(nil), base.DataPoints...)
			tt.mutate(&conflicting)
			err := exporter.ConsumeMetrics(context.Background(), &model.MetricBatch{
				Metrics: []model.Metric{conflicting},
			})
			if err == nil || !strings.Contains(err.Error(), tt.match) {
				t.Fatalf("ConsumeMetrics() error = %v, want containing %q", err, tt.match)
			}
		})
	}
}

func TestExporter_RejectsSanitizedLabelCollision(t *testing.T) {
	exporter := newTestExporter(t, prometheus.NewRegistry())
	err := exporter.ConsumeMetrics(context.Background(), &model.MetricBatch{
		Metrics: []model.Metric{{
			Name: "metric",
			Type: model.MetricTypeGauge,
			DataPoints: []model.DataPoint{{
				Attributes: model.Attributes{"node.id": "n1", "node-id": "n2"},
			}},
		}},
	})
	if err == nil || !strings.Contains(err.Error(), "collides after sanitization") {
		t.Fatalf("ConsumeMetrics() error = %v, want label collision", err)
	}
}

func TestExporter_RejectsNegativeCounterAndCanceledContext(t *testing.T) {
	exporter := newTestExporter(t, prometheus.NewRegistry())
	err := exporter.ConsumeMetrics(context.Background(), &model.MetricBatch{
		Metrics: []model.Metric{{
			Name:       "requests_total",
			Type:       model.MetricTypeCounter,
			DataPoints: []model.DataPoint{{Value: -1}},
		}},
	})
	if err == nil || !strings.Contains(err.Error(), "negative value") {
		t.Fatalf("ConsumeMetrics() error = %v, want negative counter error", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	err = exporter.ConsumeMetrics(ctx, &model.MetricBatch{})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("ConsumeMetrics() error = %v, want context.Canceled", err)
	}
}

func TestExporter_StartAndShutdown(t *testing.T) {
	exporter, err := New("127.0.0.1:0", prometheus.NewRegistry())
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	if err := exporter.Start(context.Background()); err != nil {
		t.Fatalf("Start() error = %v", err)
	}
	t.Cleanup(func() {
		_ = exporter.Shutdown(context.Background())
	})

	response, err := http.Get("http://" + exporter.Address())
	if err != nil {
		t.Fatalf("GET scrape endpoint: %v", err)
	}
	_, readErr := io.Copy(io.Discard, response.Body)
	closeErr := response.Body.Close()
	if readErr != nil {
		t.Fatalf("read scrape response: %v", readErr)
	}
	if closeErr != nil {
		t.Fatalf("close scrape response: %v", closeErr)
	}
	if response.StatusCode != http.StatusOK {
		t.Fatalf("scrape status = %d, want 200", response.StatusCode)
	}
	if err := exporter.Shutdown(context.Background()); err != nil {
		t.Fatalf("Shutdown() error = %v", err)
	}
	if err := exporter.Shutdown(context.Background()); err != nil {
		t.Fatalf("second Shutdown() error = %v", err)
	}
}

func newTestExporter(t *testing.T, registry *prometheus.Registry) *Exporter {
	t.Helper()
	exporter, err := New("127.0.0.1:0", registry)
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	return exporter
}
