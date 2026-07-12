// Command collector assembles and runs the OTLP metrics collector.
package main

import (
	"fmt"
	"os"

	"github.com/lqh0926/myOtelColler/internal/config"
)

func main() {
	cfg := config.Default()
	if err := cfg.Validate(); err != nil {
		fmt.Fprintf(os.Stderr, "invalid collector configuration: %v\n", err)
		os.Exit(1)
	}

	// Component assembly is added after the pipeline contracts are defined.
}
