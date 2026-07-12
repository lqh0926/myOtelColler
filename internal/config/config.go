// Package config defines the collector's small, explicit configuration surface.
package config

import (
	"errors"
	"fmt"
	"time"
)

const (
	defaultOTLPAddress       = ":4317"
	defaultPrometheusAddress = ":9464"
	defaultQueueCapacity     = 1_000
	defaultHighWatermark     = 800
	defaultLowWatermark      = 500
	defaultShutdownTimeout   = 10 * time.Second
)

// Priority is the request-level priority used by the queue processor.
// It intentionally lives in config until the internal model is introduced.
type Priority string

const (
	PriorityLow  Priority = "low"
	PriorityHigh Priority = "high"
)

// Config contains the complete first-version collector configuration.
type Config struct {
	OTLPAddress       string
	PrometheusAddress string
	Queue             QueueConfig
	ShutdownTimeout   time.Duration
	DefaultPriority   Priority
}

// QueueConfig controls the bounded queue and its hysteresis thresholds.
type QueueConfig struct {
	Capacity      int
	HighWatermark int
	LowWatermark  int
}

// Default returns a runnable local-development configuration.
func Default() Config {
	return Config{
		OTLPAddress:       defaultOTLPAddress,
		PrometheusAddress: defaultPrometheusAddress,
		Queue: QueueConfig{
			Capacity:      defaultQueueCapacity,
			HighWatermark: defaultHighWatermark,
			LowWatermark:  defaultLowWatermark,
		},
		ShutdownTimeout: defaultShutdownTimeout,
		DefaultPriority: PriorityLow,
	}
}

// Validate rejects configurations that cannot satisfy the queue invariants.
func (c Config) Validate() error {
	if c.OTLPAddress == "" {
		return errors.New("OTLP address must not be empty")
	}
	if c.PrometheusAddress == "" {
		return errors.New("Prometheus address must not be empty")
	}
	if c.Queue.Capacity <= 0 {
		return errors.New("queue capacity must be greater than zero")
	}
	if c.Queue.LowWatermark < 0 ||
		c.Queue.LowWatermark >= c.Queue.HighWatermark ||
		c.Queue.HighWatermark > c.Queue.Capacity {
		return fmt.Errorf(
			"queue watermarks must satisfy 0 <= low < high <= capacity: low=%d high=%d capacity=%d",
			c.Queue.LowWatermark,
			c.Queue.HighWatermark,
			c.Queue.Capacity,
		)
	}
	if c.ShutdownTimeout <= 0 {
		return errors.New("shutdown timeout must be greater than zero")
	}
	if c.DefaultPriority != PriorityLow && c.DefaultPriority != PriorityHigh {
		return fmt.Errorf("unsupported default priority %q", c.DefaultPriority)
	}
	return nil
}
