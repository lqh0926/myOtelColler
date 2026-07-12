package config

import "testing"

func TestDefaultIsValid(t *testing.T) {
	if err := Default().Validate(); err != nil {
		t.Fatalf("Default().Validate() returned an error: %v", err)
	}
}

func TestValidateRejectsInvalidWatermarks(t *testing.T) {
	tests := []struct {
		name  string
		queue QueueConfig
	}{
		{name: "negative low", queue: QueueConfig{Capacity: 10, LowWatermark: -1, HighWatermark: 8}},
		{name: "equal watermarks", queue: QueueConfig{Capacity: 10, LowWatermark: 8, HighWatermark: 8}},
		{name: "high above capacity", queue: QueueConfig{Capacity: 10, LowWatermark: 5, HighWatermark: 11}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := Default()
			cfg.Queue = tt.queue
			if err := cfg.Validate(); err == nil {
				t.Fatal("Validate() returned nil for invalid queue configuration")
			}
		})
	}
}
