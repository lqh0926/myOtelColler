package otlp

import (
	"context"
	"errors"
	"fmt"
	"net"
	"strings"
	"sync"

	"github.com/lqh0926/myOtelColler/internal/model"
	"github.com/lqh0926/myOtelColler/internal/pipeline"
	collectorMetrics "go.opentelemetry.io/proto/otlp/collector/metrics/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

// PriorityMetadataKey is the gRPC metadata key used for request-level semantic
// priority. Its accepted values are "low" and "high", case-insensitively.
const PriorityMetadataKey = "x-otel-priority"

// Receiver implements the standard OTLP MetricsService over gRPC.
type Receiver struct {
	collectorMetrics.UnimplementedMetricsServiceServer

	address         string
	defaultPriority model.Priority
	consumer        pipeline.MetricsConsumer

	mu       sync.Mutex
	server   *grpc.Server
	listener net.Listener
}

// New creates a receiver. Start must be called before it accepts RPCs.
func New(address string, defaultPriority model.Priority, consumer pipeline.MetricsConsumer) (*Receiver, error) {
	if address == "" {
		return nil, errors.New("OTLP address must not be empty")
	}
	if !defaultPriority.Valid() {
		return nil, fmt.Errorf("invalid default priority %d", defaultPriority)
	}
	if consumer == nil {
		return nil, errors.New("metrics consumer must not be nil")
	}
	return &Receiver{
		address:         address,
		defaultPriority: defaultPriority,
		consumer:        consumer,
	}, nil
}

// Export converts one OTLP request into Resource+Scope batches and passes each
// batch to the downstream consumer in request order.
func (r *Receiver) Export(
	ctx context.Context,
	request *collectorMetrics.ExportMetricsServiceRequest,
) (*collectorMetrics.ExportMetricsServiceResponse, error) {
	priority, err := priorityFromContext(ctx, r.defaultPriority)
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	if request == nil {
		return nil, status.Error(codes.InvalidArgument, "OTLP export request must not be nil")
	}

	batches, err := convertRequest(request, priority)
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	for _, batch := range batches {
		if err := r.consumer.ConsumeMetrics(ctx, batch); err != nil {
			return nil, downstreamStatus(err)
		}
	}
	return &collectorMetrics.ExportMetricsServiceResponse{}, nil
}

func priorityFromContext(ctx context.Context, fallback model.Priority) (model.Priority, error) {
	values := metadata.ValueFromIncomingContext(ctx, PriorityMetadataKey)
	if len(values) == 0 {
		return fallback, nil
	}
	if len(values) != 1 {
		return 0, fmt.Errorf("metadata %q must have exactly one value", PriorityMetadataKey)
	}
	switch strings.ToLower(strings.TrimSpace(values[0])) {
	case "low":
		return model.PriorityLow, nil
	case "high":
		return model.PriorityHigh, nil
	default:
		return 0, fmt.Errorf("metadata %q must be low or high", PriorityMetadataKey)
	}
}

func downstreamStatus(err error) error {
	if errors.Is(err, pipeline.ErrQueueFull) {
		return status.Error(codes.ResourceExhausted, err.Error())
	}
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return status.FromContextError(err).Err()
	}
	return status.Error(codes.Internal, err.Error())
}

// Start opens the listening socket and starts serving. It returns only after the
// socket is ready so it is safe to start this component last.
func (r *Receiver) Start(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return fmt.Errorf("start OTLP receiver: %w", err)
	}

	r.mu.Lock()
	defer r.mu.Unlock()
	if r.server != nil {
		return errors.New("start OTLP receiver: already started")
	}
	listener, err := net.Listen("tcp", r.address)
	if err != nil {
		return fmt.Errorf("listen for OTLP gRPC on %q: %w", r.address, err)
	}
	server := grpc.NewServer()
	collectorMetrics.RegisterMetricsServiceServer(server, r)
	r.listener = listener
	r.server = server
	go func() {
		_ = server.Serve(listener)
	}()
	return nil
}

// Shutdown stops accepting new RPCs, waits for active RPCs within the context
// deadline, then force-stops if the context expires. Repeated calls are safe.
func (r *Receiver) Shutdown(ctx context.Context) error {
	r.mu.Lock()
	server := r.server
	r.server = nil
	r.listener = nil
	r.mu.Unlock()
	if server == nil {
		return nil
	}

	done := make(chan struct{})
	go func() {
		server.GracefulStop()
		close(done)
	}()
	select {
	case <-done:
		return nil
	case <-ctx.Done():
		server.Stop()
		<-done
		return fmt.Errorf("shutdown OTLP receiver: %w", ctx.Err())
	}
}

// Address returns the actual listening address after Start, including the port
// selected by the OS when the configured port was zero.
func (r *Receiver) Address() string {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.listener == nil {
		return r.address
	}
	return r.listener.Addr().String()
}
