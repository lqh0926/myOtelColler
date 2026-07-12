// Package pipeline defines consumer, lifecycle, and error contracts.
//
// A call to MetricsConsumer.ConsumeMetrics transfers ownership of the batch to
// the consumer unconditionally, whether the call returns nil or an error. The
// caller must not read or modify the batch after the call begins. This permits a
// queue processor to retain the pointer without copying it.
//
// A nil ConsumeMetrics result means that the consumer either accepted the batch
// or completed an explicitly configured policy drop. It does not promise that
// the batch has already been exported or persisted.
package pipeline
