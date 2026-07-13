// Package prom exports the latest values from the internal metrics model using
// the Prometheus exposition format.
//
// OTLP monotonic cumulative counters are exposed as their latest cumulative
// value; this exporter does not add deltas. A lower value with a newer timestamp
// is therefore exposed as a counter reset. For duplicate label sets, an older
// non-zero timestamp cannot overwrite a newer one, while equal or missing
// timestamps use last-write-wins semantics.
package prom
