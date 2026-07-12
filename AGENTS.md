# Repository Guidelines

## Project Structure & Module Organization

This repository is currently in the planning stage; `otel-collector-plan.md` is the source of truth for scope and milestones. The intended system is a small Go pipeline: OTLP gRPC receiver → processors/backpressure → Prometheus exporter. As implementation begins, keep binaries under `cmd/`, reusable packages under `internal/` (for example, `internal/receiver`, `internal/pipeline`, and `internal/exporter`), and tests beside the code they cover. Put deployment files at the repository root or under `deploy/`, and Grafana dashboards under `deploy/grafana/`.

Do not expand the project into a general plugin framework or add unrelated receivers and exporters.

## Build, Test, and Development Commands

The Go module and build scripts have not yet been created. Once present, prefer standard commands:

- `go run ./cmd/collector` runs the collector locally.
- `go build ./...` compiles every package.
- `go test ./...` runs the full test suite.
- `go test -race ./...` checks concurrent queue and pipeline code for races.
- `go fmt ./...` formats Go sources; `go vet ./...` reports common defects.
- `docker compose up --build` should eventually start the collector, Prometheus, and Grafana stack.

Update this section when the repository introduces a `Makefile` or different entrypoint.

## Coding Style & Naming Conventions

Use idiomatic Go and accept `gofmt` output without manual alignment. Package names should be short, lowercase, and singular. Exported identifiers use `PascalCase`; unexported identifiers use `camelCase`. Name files by responsibility, such as `receiver.go`, `bounded_queue.go`, and `bounded_queue_test.go`. Keep interfaces small and define them near their consumers. Wrap errors with context using `%w`, and avoid hidden global state.

## Testing Guidelines

Use Go's `testing` package and table-driven tests. Test files must end in `_test.go`, with cases named for behavior (`TestQueue_DropsLowestPriority`). Prioritize boundary conditions, high/low-watermark hysteresis, drop policies, slow exporters, cancellation, and race safety. Add an end-to-end test proving that an OTLP metric becomes Prometheus output.

## Commit & Pull Request Guidelines

No commit history is available yet, so use concise imperative subjects, optionally scoped, such as `pipeline: add consumer interface`. Keep commits focused. Pull requests should describe the change, design tradeoffs, test commands and results, and any configuration changes. Link relevant issues; include dashboard screenshots when visualization changes.

## Design Ownership

Pipeline interfaces and backpressure behavior are owner-led design areas. Discuss alternatives and tradeoffs before implementing them. Scaffolding, protocol integration, exporters, and deployment files may be contributed directly, but must remain understandable and documented.
