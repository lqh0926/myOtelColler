package pipeline

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/lqh0926/myOtelColler/internal/model"
)

type testComponent struct{}

func (testComponent) ConsumeMetrics(context.Context, *model.MetricBatch) error { return nil }
func (testComponent) Start(context.Context) error                              { return nil }
func (testComponent) Shutdown(context.Context) error                           { return nil }

var (
	_ MetricsConsumer = testComponent{}
	_ Starter         = testComponent{}
	_ Shutdowner      = testComponent{}
)

func TestErrQueueFullCanBeIdentifiedWhenWrapped(t *testing.T) {
	err := fmt.Errorf("enqueue high-priority batch: %w", ErrQueueFull)
	if !errors.Is(err, ErrQueueFull) {
		t.Fatalf("errors.Is(%v, ErrQueueFull) = false", err)
	}
}
