package model

import "time"

// Priority describes the semantic importance of every metric in a batch.
type Priority uint8

const (
	// PriorityLow is intended for periodic data that may be shed under load.
	PriorityLow Priority = iota
	// PriorityHigh is intended for event or alert data that should be preserved.
	PriorityHigh
)

// Valid reports whether p is a supported priority.
func (p Priority) Valid() bool {
	return p == PriorityLow || p == PriorityHigh
}

// MetricType identifies the supported metric data semantics.
type MetricType uint8

const (
	// MetricTypeGauge represents an instantaneous value.
	MetricTypeGauge MetricType = iota
	// MetricTypeCounter represents a monotonic cumulative value.
	MetricTypeCounter
)

// Valid reports whether t is a supported metric type.
func (t MetricType) Valid() bool {
	return t == MetricTypeGauge || t == MetricTypeCounter
}

// Attributes is the deliberately restricted first-version attribute model.
// OTLP attributes with non-string values must be rejected or skipped explicitly
// at the receiver boundary.
type Attributes map[string]string

// MetricBatch is the queue's atomic unit. Every metric in a batch has the same
// Resource, Scope, and Priority.
type MetricBatch struct {
	Priority           Priority
	ResourceAttributes Attributes
	ScopeName          string
	ScopeVersion       string
	Metrics            []Metric
}

// Metric is a Gauge or monotonic cumulative Counter and its data points.
type Metric struct {
	Name        string
	Description string
	Unit        string
	Type        MetricType
	DataPoints  []DataPoint
}

// DataPoint is one numeric sample. Value is always float64 so the pipeline does
// not carry transport-specific integer and floating-point variants.
type DataPoint struct {
	Attributes Attributes
	Timestamp  time.Time
	Value      float64
}
