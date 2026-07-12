package pipeline

import "errors"

// ErrQueueFull means a batch was not accepted because the bounded queue had no
// item eligible for eviction. Callers may identify wrapped instances with
// errors.Is.
var ErrQueueFull = errors.New("queue full")
