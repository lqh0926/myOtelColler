package pipeline

import (
	"context"

	"github.com/lqh0926/myOtelColler/internal/model"
)

// MetricsConsumer accepts ownership of one metric batch.
type MetricsConsumer interface {
	ConsumeMetrics(context.Context, *model.MetricBatch) error
}
