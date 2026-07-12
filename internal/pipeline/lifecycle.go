package pipeline

import "context"

// Starter is implemented by components that need explicit initialization.
type Starter interface {
	Start(context.Context) error
}

// Shutdowner is implemented by components that need graceful shutdown.
type Shutdowner interface {
	Shutdown(context.Context) error
}
