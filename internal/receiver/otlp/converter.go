package otlp

import (
	"errors"
	"fmt"
	"math"
	"time"

	"github.com/lqh0926/myOtelColler/internal/model"
	collectorMetrics "go.opentelemetry.io/proto/otlp/collector/metrics/v1"
	commonpb "go.opentelemetry.io/proto/otlp/common/v1"
	metricspb "go.opentelemetry.io/proto/otlp/metrics/v1"
)

func convertRequest(
	request *collectorMetrics.ExportMetricsServiceRequest,
	priority model.Priority,
) ([]*model.MetricBatch, error) {
	var batches []*model.MetricBatch
	for resourceIndex, resourceMetrics := range request.ResourceMetrics {
		if resourceMetrics == nil {
			return nil, fmt.Errorf("resource_metrics[%d] must not be nil", resourceIndex)
		}
		resourceAttributes, err := convertAttributes(resourceMetrics.GetResource().GetAttributes())
		if err != nil {
			return nil, fmt.Errorf("resource_metrics[%d] attributes: %w", resourceIndex, err)
		}
		for scopeIndex, scopeMetrics := range resourceMetrics.ScopeMetrics {
			if scopeMetrics == nil {
				return nil, fmt.Errorf("resource_metrics[%d].scope_metrics[%d] must not be nil", resourceIndex, scopeIndex)
			}
			if scope := scopeMetrics.Scope; scope != nil && len(scope.Attributes) != 0 {
				return nil, fmt.Errorf(
					"resource_metrics[%d].scope_metrics[%d]: scope attributes are unsupported",
					resourceIndex,
					scopeIndex,
				)
			}

			metrics := make([]model.Metric, 0, len(scopeMetrics.Metrics))
			for metricIndex, otlpMetric := range scopeMetrics.Metrics {
				converted, err := convertMetric(otlpMetric)
				if err != nil {
					return nil, fmt.Errorf(
						"resource_metrics[%d].scope_metrics[%d].metrics[%d]: %w",
						resourceIndex,
						scopeIndex,
						metricIndex,
						err,
					)
				}
				metrics = append(metrics, converted)
			}
			if len(metrics) == 0 {
				continue
			}

			batch := &model.MetricBatch{
				Priority:           priority,
				ResourceAttributes: cloneAttributes(resourceAttributes),
				Metrics:            metrics,
			}
			if scopeMetrics.Scope != nil {
				batch.ScopeName = scopeMetrics.Scope.Name
				batch.ScopeVersion = scopeMetrics.Scope.Version
			}
			batches = append(batches, batch)
		}
	}
	return batches, nil
}

func convertMetric(metric *metricspb.Metric) (model.Metric, error) {
	if metric == nil {
		return model.Metric{}, errors.New("metric must not be nil")
	}
	if metric.Name == "" {
		return model.Metric{}, errors.New("metric name must not be empty")
	}
	if len(metric.Metadata) != 0 {
		return model.Metric{}, fmt.Errorf("metric %q metadata is unsupported", metric.Name)
	}

	converted := model.Metric{
		Name:        metric.Name,
		Description: metric.Description,
		Unit:        metric.Unit,
	}
	var points []*metricspb.NumberDataPoint
	switch data := metric.Data.(type) {
	case *metricspb.Metric_Gauge:
		if data.Gauge == nil {
			return model.Metric{}, fmt.Errorf("metric %q gauge must not be nil", metric.Name)
		}
		converted.Type = model.MetricTypeGauge
		points = data.Gauge.DataPoints
	case *metricspb.Metric_Sum:
		if data.Sum == nil {
			return model.Metric{}, fmt.Errorf("metric %q sum must not be nil", metric.Name)
		}
		if !data.Sum.IsMonotonic {
			return model.Metric{}, fmt.Errorf("metric %q non-monotonic sum is unsupported", metric.Name)
		}
		if data.Sum.AggregationTemporality != metricspb.AggregationTemporality_AGGREGATION_TEMPORALITY_CUMULATIVE {
			return model.Metric{}, fmt.Errorf("metric %q must use cumulative aggregation temporality", metric.Name)
		}
		converted.Type = model.MetricTypeCounter
		points = data.Sum.DataPoints
	case *metricspb.Metric_Histogram:
		return model.Metric{}, fmt.Errorf("metric %q histogram is unsupported", metric.Name)
	case *metricspb.Metric_ExponentialHistogram:
		return model.Metric{}, fmt.Errorf("metric %q exponential histogram is unsupported", metric.Name)
	case *metricspb.Metric_Summary:
		return model.Metric{}, fmt.Errorf("metric %q summary is unsupported", metric.Name)
	case nil:
		return model.Metric{}, fmt.Errorf("metric %q has no data", metric.Name)
	default:
		return model.Metric{}, fmt.Errorf("metric %q has unsupported data type", metric.Name)
	}

	converted.DataPoints = make([]model.DataPoint, 0, len(points))
	for pointIndex, point := range points {
		convertedPoint, err := convertDataPoint(point)
		if err != nil {
			return model.Metric{}, fmt.Errorf("metric %q data_points[%d]: %w", metric.Name, pointIndex, err)
		}
		converted.DataPoints = append(converted.DataPoints, convertedPoint)
	}
	return converted, nil
}

func convertDataPoint(point *metricspb.NumberDataPoint) (model.DataPoint, error) {
	if point == nil {
		return model.DataPoint{}, errors.New("data point must not be nil")
	}
	if len(point.Exemplars) != 0 {
		return model.DataPoint{}, errors.New("exemplars are unsupported")
	}
	attributes, err := convertAttributes(point.Attributes)
	if err != nil {
		return model.DataPoint{}, fmt.Errorf("attributes: %w", err)
	}
	if point.TimeUnixNano > math.MaxInt64 {
		return model.DataPoint{}, fmt.Errorf("timestamp %d exceeds supported range", point.TimeUnixNano)
	}
	if point.TimeUnixNano == 0 {
		return model.DataPoint{}, errors.New("timestamp must not be zero")
	}

	converted := model.DataPoint{
		Attributes: attributes,
		Timestamp:  time.Unix(0, int64(point.TimeUnixNano)),
	}
	switch value := point.Value.(type) {
	case *metricspb.NumberDataPoint_AsDouble:
		converted.Value = value.AsDouble
	case *metricspb.NumberDataPoint_AsInt:
		converted.Value = float64(value.AsInt)
	case nil:
		return model.DataPoint{}, errors.New("numeric value is missing")
	default:
		return model.DataPoint{}, errors.New("numeric value has unsupported type")
	}
	return converted, nil
}

func convertAttributes(attributes []*commonpb.KeyValue) (model.Attributes, error) {
	converted := make(model.Attributes, len(attributes))
	for index, attribute := range attributes {
		if attribute == nil {
			return nil, fmt.Errorf("attribute[%d] must not be nil", index)
		}
		if attribute.Key == "" {
			return nil, fmt.Errorf("attribute[%d] key must not be empty", index)
		}
		if _, exists := converted[attribute.Key]; exists {
			return nil, fmt.Errorf("attribute key %q is duplicated", attribute.Key)
		}
		if attribute.Value == nil {
			return nil, fmt.Errorf("attribute %q value must not be nil", attribute.Key)
		}
		stringValue, ok := attribute.Value.Value.(*commonpb.AnyValue_StringValue)
		if !ok {
			return nil, fmt.Errorf("attribute %q must have a string value", attribute.Key)
		}
		converted[attribute.Key] = stringValue.StringValue
	}
	return converted, nil
}

func cloneAttributes(attributes model.Attributes) model.Attributes {
	clone := make(model.Attributes, len(attributes))
	for key, value := range attributes {
		clone[key] = value
	}
	return clone
}
