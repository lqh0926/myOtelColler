package otlp

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/lqh0926/myOtelColler/internal/model"
	"github.com/lqh0926/myOtelColler/internal/pipeline"
	collectorMetrics "go.opentelemetry.io/proto/otlp/collector/metrics/v1"
	commonpb "go.opentelemetry.io/proto/otlp/common/v1"
	metricspb "go.opentelemetry.io/proto/otlp/metrics/v1"
	resourcepb "go.opentelemetry.io/proto/otlp/resource/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

var (
	_ collectorMetrics.MetricsServiceServer = (*Receiver)(nil)
	_ pipeline.Starter                      = (*Receiver)(nil)
	_ pipeline.Shutdowner                   = (*Receiver)(nil)
)

type recordingConsumer struct {
	mu      sync.Mutex
	batches []*model.MetricBatch
	err     error
}

func (c *recordingConsumer) ConsumeMetrics(_ context.Context, batch *model.MetricBatch) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.batches = append(c.batches, batch)
	return c.err
}

func (c *recordingConsumer) snapshot() []*model.MetricBatch {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([]*model.MetricBatch(nil), c.batches...)
}

func TestReceiver_ConvertsAndSplitsRequestByResourceAndScope(t *testing.T) {
	consumer := &recordingConsumer{}
	receiver := newTestReceiver(t, model.PriorityLow, consumer)
	ctx := metadata.NewIncomingContext(
		context.Background(),
		metadata.Pairs(PriorityMetadataKey, "HIGH"),
	)
	request := &collectorMetrics.ExportMetricsServiceRequest{
		ResourceMetrics: []*metricspb.ResourceMetrics{{
			Resource: &resourcepb.Resource{Attributes: []*commonpb.KeyValue{
				stringAttribute("service.name", "raft"),
			}},
			ScopeMetrics: []*metricspb.ScopeMetrics{
				{
					Scope: &commonpb.InstrumentationScope{Name: "raft.core", Version: "v1"},
					Metrics: []*metricspb.Metric{{
						Name:        "raft_term",
						Description: "Current term",
						Unit:        "1",
						Data: &metricspb.Metric_Gauge{Gauge: &metricspb.Gauge{
							DataPoints: []*metricspb.NumberDataPoint{
								{
									Attributes:   []*commonpb.KeyValue{stringAttribute("node", "n1")},
									TimeUnixNano: 100,
									Value:        &metricspb.NumberDataPoint_AsDouble{AsDouble: 7.5},
								},
								{
									Attributes:   []*commonpb.KeyValue{stringAttribute("node", "n2")},
									TimeUnixNano: 101,
									Value:        &metricspb.NumberDataPoint_AsInt{AsInt: 8},
								},
							},
						}},
					}},
				},
				{
					Scope: &commonpb.InstrumentationScope{Name: "raft.rpc", Version: "v2"},
					Metrics: []*metricspb.Metric{{
						Name: "raft_elections_total",
						Unit: "{election}",
						Data: &metricspb.Metric_Sum{Sum: &metricspb.Sum{
							AggregationTemporality: metricspb.AggregationTemporality_AGGREGATION_TEMPORALITY_CUMULATIVE,
							IsMonotonic:            true,
							DataPoints: []*metricspb.NumberDataPoint{{
								TimeUnixNano: 102,
								Value:        &metricspb.NumberDataPoint_AsInt{AsInt: 3},
							}},
						}},
					}},
				},
			},
		}},
	}

	if _, err := receiver.Export(ctx, request); err != nil {
		t.Fatalf("Export() error = %v", err)
	}
	batches := consumer.snapshot()
	if len(batches) != 2 {
		t.Fatalf("batch count = %d, want 2", len(batches))
	}
	if batches[0].Priority != model.PriorityHigh || batches[1].Priority != model.PriorityHigh {
		t.Fatalf("priorities = %v, %v; want high", batches[0].Priority, batches[1].Priority)
	}
	if batches[0].ScopeName != "raft.core" || batches[1].ScopeName != "raft.rpc" {
		t.Fatalf("scope names = %q, %q", batches[0].ScopeName, batches[1].ScopeName)
	}
	if got := batches[0].ResourceAttributes["service.name"]; got != "raft" {
		t.Fatalf("resource service.name = %q, want raft", got)
	}
	if got := batches[0].Metrics[0].DataPoints[0].Value; got != 7.5 {
		t.Fatalf("gauge double value = %v, want 7.5", got)
	}
	if got := batches[0].Metrics[0].DataPoints[1].Value; got != 8 {
		t.Fatalf("gauge int value = %v, want 8", got)
	}
	if batches[1].Metrics[0].Type != model.MetricTypeCounter {
		t.Fatalf("sum type = %v, want counter", batches[1].Metrics[0].Type)
	}
	if got := batches[1].Metrics[0].DataPoints[0].Timestamp; !got.Equal(time.Unix(0, 102)) {
		t.Fatalf("counter timestamp = %v, want 102ns", got)
	}
}

func TestReceiver_UsesDefaultPriorityWhenMetadataIsMissing(t *testing.T) {
	consumer := &recordingConsumer{}
	receiver := newTestReceiver(t, model.PriorityHigh, consumer)
	if _, err := receiver.Export(context.Background(), validGaugeRequest()); err != nil {
		t.Fatalf("Export() error = %v", err)
	}
	if got := consumer.snapshot()[0].Priority; got != model.PriorityHigh {
		t.Fatalf("priority = %v, want high", got)
	}
}

func TestReceiver_RejectsUnsupportedInput(t *testing.T) {
	tests := []struct {
		name    string
		request *collectorMetrics.ExportMetricsServiceRequest
		ctx     context.Context
	}{
		{
			name: "histogram",
			request: requestWithMetric(&metricspb.Metric{
				Name: "latency",
				Data: &metricspb.Metric_Histogram{Histogram: &metricspb.Histogram{}},
			}),
		},
		{
			name: "non-monotonic sum",
			request: requestWithMetric(&metricspb.Metric{
				Name: "change",
				Data: &metricspb.Metric_Sum{Sum: &metricspb.Sum{
					AggregationTemporality: metricspb.AggregationTemporality_AGGREGATION_TEMPORALITY_CUMULATIVE,
				}},
			}),
		},
		{
			name: "delta sum",
			request: requestWithMetric(&metricspb.Metric{
				Name: "requests",
				Data: &metricspb.Metric_Sum{Sum: &metricspb.Sum{
					AggregationTemporality: metricspb.AggregationTemporality_AGGREGATION_TEMPORALITY_DELTA,
					IsMonotonic:            true,
				}},
			}),
		},
		{
			name: "non-string attribute",
			request: requestWithMetric(&metricspb.Metric{
				Name: "load",
				Data: &metricspb.Metric_Gauge{Gauge: &metricspb.Gauge{DataPoints: []*metricspb.NumberDataPoint{{
					Attributes: []*commonpb.KeyValue{{
						Key:   "core",
						Value: &commonpb.AnyValue{Value: &commonpb.AnyValue_IntValue{IntValue: 1}},
					}},
					TimeUnixNano: 1,
					Value:        &metricspb.NumberDataPoint_AsDouble{AsDouble: 0.5},
				}}}},
			}),
		},
		{
			name: "scope attributes",
			request: &collectorMetrics.ExportMetricsServiceRequest{ResourceMetrics: []*metricspb.ResourceMetrics{{
				ScopeMetrics: []*metricspb.ScopeMetrics{{
					Scope:   &commonpb.InstrumentationScope{Attributes: []*commonpb.KeyValue{stringAttribute("team", "storage")}},
					Metrics: validGaugeRequest().ResourceMetrics[0].ScopeMetrics[0].Metrics,
				}},
			}}},
		},
		{
			name:    "unknown priority",
			request: validGaugeRequest(),
			ctx: metadata.NewIncomingContext(
				context.Background(),
				metadata.Pairs(PriorityMetadataKey, "urgent"),
			),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			receiver := newTestReceiver(t, model.PriorityLow, &recordingConsumer{})
			ctx := tt.ctx
			if ctx == nil {
				ctx = context.Background()
			}
			_, err := receiver.Export(ctx, tt.request)
			if got := status.Code(err); got != codes.InvalidArgument {
				t.Fatalf("Export() code = %v, want InvalidArgument; error = %v", got, err)
			}
		})
	}
}

func TestReceiver_MapsDownstreamErrors(t *testing.T) {
	tests := []struct {
		name string
		err  error
		code codes.Code
	}{
		{name: "queue full", err: fmt.Errorf("enqueue: %w", pipeline.ErrQueueFull), code: codes.ResourceExhausted},
		{name: "canceled", err: context.Canceled, code: codes.Canceled},
		{name: "internal", err: errors.New("export failed"), code: codes.Internal},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			receiver := newTestReceiver(t, model.PriorityLow, &recordingConsumer{err: tt.err})
			_, err := receiver.Export(context.Background(), validGaugeRequest())
			if got := status.Code(err); got != tt.code {
				t.Fatalf("Export() code = %v, want %v; error = %v", got, tt.code, err)
			}
		})
	}
}

func TestReceiver_GRPCIntegrationAndLifecycle(t *testing.T) {
	consumer := &recordingConsumer{}
	receiver, err := New("127.0.0.1:0", model.PriorityLow, consumer)
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	if err := receiver.Start(context.Background()); err != nil {
		t.Fatalf("Start() error = %v", err)
	}
	t.Cleanup(func() {
		_ = receiver.Shutdown(context.Background())
	})

	connection, err := grpc.NewClient(receiver.Address(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("grpc.NewClient() error = %v", err)
	}
	t.Cleanup(func() { _ = connection.Close() })
	client := collectorMetrics.NewMetricsServiceClient(connection)
	ctx := metadata.AppendToOutgoingContext(context.Background(), PriorityMetadataKey, "high")
	if _, err := client.Export(ctx, validGaugeRequest()); err != nil {
		t.Fatalf("client.Export() error = %v", err)
	}
	if got := consumer.snapshot(); len(got) != 1 || got[0].Priority != model.PriorityHigh {
		t.Fatalf("received batches = %+v, want one high-priority batch", got)
	}
	if err := receiver.Shutdown(context.Background()); err != nil {
		t.Fatalf("Shutdown() error = %v", err)
	}
	if err := receiver.Shutdown(context.Background()); err != nil {
		t.Fatalf("second Shutdown() error = %v", err)
	}
}

func newTestReceiver(t *testing.T, priority model.Priority, consumer pipeline.MetricsConsumer) *Receiver {
	t.Helper()
	receiver, err := New("127.0.0.1:0", priority, consumer)
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	return receiver
}

func validGaugeRequest() *collectorMetrics.ExportMetricsServiceRequest {
	return requestWithMetric(&metricspb.Metric{
		Name: "load",
		Data: &metricspb.Metric_Gauge{Gauge: &metricspb.Gauge{
			DataPoints: []*metricspb.NumberDataPoint{{
				TimeUnixNano: 1,
				Value:        &metricspb.NumberDataPoint_AsDouble{AsDouble: 0.5},
			}},
		}},
	})
}

func requestWithMetric(metric *metricspb.Metric) *collectorMetrics.ExportMetricsServiceRequest {
	return &collectorMetrics.ExportMetricsServiceRequest{
		ResourceMetrics: []*metricspb.ResourceMetrics{{
			ScopeMetrics: []*metricspb.ScopeMetrics{{Metrics: []*metricspb.Metric{metric}}},
		}},
	}
}

func stringAttribute(key, value string) *commonpb.KeyValue {
	return &commonpb.KeyValue{
		Key:   key,
		Value: &commonpb.AnyValue{Value: &commonpb.AnyValue_StringValue{StringValue: value}},
	}
}
