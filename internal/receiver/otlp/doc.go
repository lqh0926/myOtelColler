// Package otlp receives OTLP metrics over gRPC and converts them to the internal
// transport-independent model.
//
// PriorityMetadataKey carries the project-specific request priority. This
// extension does not modify the standard OTLP MetricsService RPC.
package otlp
