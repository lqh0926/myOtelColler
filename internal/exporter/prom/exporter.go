package prom

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/lqh0926/myOtelColler/internal/model"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

const (
	scopeNameLabel    = "otel_scope_name"
	scopeVersionLabel = "otel_scope_version"
)

// Exporter retains the latest sample for every metric and label set. It is both
// a pipeline consumer and a Prometheus collector.
type Exporter struct {
	address  string
	registry *prometheus.Registry

	mu       sync.RWMutex
	families map[string]*metricFamily

	lifecycleMu sync.Mutex
	server      *http.Server
	listener    net.Listener
}

type metricFamily struct {
	desc        *prometheus.Desc
	help        string
	unit        string
	metricType  model.MetricType
	labelNames  []string
	samplesByID map[string]sample
}

type sample struct {
	labelValues []string
	timestamp   int64
	hasTime     bool
	value       float64
}

type normalizedSample struct {
	name        string
	help        string
	unit        string
	metricType  model.MetricType
	labelNames  []string
	labelValues []string
	sampleID    string
	timestamp   int64
	hasTime     bool
	value       float64
}

// New creates and registers an exporter. A nil registry creates an isolated
// registry, which avoids accidental registration in Prometheus's global state.
func New(address string, registry *prometheus.Registry) (*Exporter, error) {
	if address == "" {
		return nil, errors.New("Prometheus address must not be empty")
	}
	if registry == nil {
		registry = prometheus.NewRegistry()
	}

	exporter := &Exporter{
		address:  address,
		registry: registry,
		families: make(map[string]*metricFamily),
	}
	if err := registry.Register(exporter); err != nil {
		return nil, fmt.Errorf("register Prometheus exporter: %w", err)
	}
	return exporter, nil
}

// ConsumeMetrics validates a complete batch before updating any stored sample.
// It does not retain the batch or any maps or slices owned by the caller.
func (e *Exporter) ConsumeMetrics(ctx context.Context, batch *model.MetricBatch) error {
	if err := ctx.Err(); err != nil {
		return fmt.Errorf("consume metrics: %w", err)
	}
	if batch == nil {
		return errors.New("consume metrics: batch must not be nil")
	}

	normalized, err := normalizeBatch(batch)
	if err != nil {
		return err
	}

	e.mu.Lock()
	defer e.mu.Unlock()

	if err := e.validateSchemas(normalized); err != nil {
		return err
	}
	for _, incoming := range normalized {
		family := e.families[incoming.name]
		if family == nil {
			family = &metricFamily{
				desc: prometheus.NewDesc(
					incoming.name,
					incoming.help,
					incoming.labelNames,
					nil,
				),
				help:        incoming.help,
				unit:        incoming.unit,
				metricType:  incoming.metricType,
				labelNames:  append([]string(nil), incoming.labelNames...),
				samplesByID: make(map[string]sample),
			}
			e.families[incoming.name] = family
		}

		current, exists := family.samplesByID[incoming.sampleID]
		if exists && current.hasTime && incoming.hasTime && incoming.timestamp < current.timestamp {
			continue
		}
		family.samplesByID[incoming.sampleID] = sample{
			labelValues: append([]string(nil), incoming.labelValues...),
			timestamp:   incoming.timestamp,
			hasTime:     incoming.hasTime,
			value:       incoming.value,
		}
	}
	return nil
}

func (e *Exporter) validateSchemas(samples []normalizedSample) error {
	pending := make(map[string]normalizedSample)
	for _, incoming := range samples {
		if family := e.families[incoming.name]; family != nil {
			if err := schemaConflict(
				incoming.name,
				family.metricType,
				family.help,
				family.unit,
				family.labelNames,
				incoming,
			); err != nil {
				return err
			}
			continue
		}
		if first, exists := pending[incoming.name]; exists {
			if err := schemaConflict(
				incoming.name,
				first.metricType,
				first.help,
				first.unit,
				first.labelNames,
				incoming,
			); err != nil {
				return err
			}
			continue
		}
		pending[incoming.name] = incoming
	}
	return nil
}

func schemaConflict(
	name string,
	metricType model.MetricType,
	help string,
	unit string,
	labelNames []string,
	incoming normalizedSample,
) error {
	if metricType != incoming.metricType {
		return fmt.Errorf("metric %q conflicts with existing type", name)
	}
	if help != incoming.help {
		return fmt.Errorf("metric %q conflicts with existing description", name)
	}
	if unit != incoming.unit {
		return fmt.Errorf("metric %q conflicts with existing unit", name)
	}
	if !equalStrings(labelNames, incoming.labelNames) {
		return fmt.Errorf("metric %q conflicts with existing label names", name)
	}
	return nil
}

func normalizeBatch(batch *model.MetricBatch) ([]normalizedSample, error) {
	var normalized []normalizedSample
	for _, metric := range batch.Metrics {
		if !metric.Type.Valid() {
			return nil, fmt.Errorf("metric %q has unsupported type %d", metric.Name, metric.Type)
		}
		name, err := sanitizeMetricName(metric.Name)
		if err != nil {
			return nil, err
		}
		help := metric.Description
		if help == "" {
			help = name
		}

		for _, point := range metric.DataPoints {
			if metric.Type == model.MetricTypeCounter && point.Value < 0 {
				return nil, fmt.Errorf("counter metric %q has negative value %v", metric.Name, point.Value)
			}
			labelNames, labelValues, err := normalizeLabels(batch, point)
			if err != nil {
				return nil, fmt.Errorf("metric %q: %w", metric.Name, err)
			}
			timestamp := point.Timestamp.UnixNano()
			hasTime := !point.Timestamp.IsZero()
			normalized = append(normalized, normalizedSample{
				name:        name,
				help:        help,
				unit:        metric.Unit,
				metricType:  metric.Type,
				labelNames:  labelNames,
				labelValues: labelValues,
				sampleID:    sampleID(labelValues),
				timestamp:   timestamp,
				hasTime:     hasTime,
				value:       point.Value,
			})
		}
	}
	return normalized, nil
}

func normalizeLabels(batch *model.MetricBatch, point model.DataPoint) ([]string, []string, error) {
	labels := map[string]string{
		scopeNameLabel:    batch.ScopeName,
		scopeVersionLabel: batch.ScopeVersion,
	}
	for key, value := range batch.ResourceAttributes {
		if err := addLabel(labels, "resource_"+key, value); err != nil {
			return nil, nil, err
		}
	}
	for key, value := range point.Attributes {
		if err := addLabel(labels, "attribute_"+key, value); err != nil {
			return nil, nil, err
		}
	}

	names := make([]string, 0, len(labels))
	for name := range labels {
		names = append(names, name)
	}
	sort.Strings(names)
	values := make([]string, len(names))
	for i, name := range names {
		values[i] = labels[name]
	}
	return names, values, nil
}

func addLabel(labels map[string]string, rawName, value string) error {
	name := sanitizeLabelName(rawName)
	if _, exists := labels[name]; exists {
		return fmt.Errorf("label %q collides after sanitization as %q", rawName, name)
	}
	labels[name] = value
	return nil
}

func sanitizeMetricName(name string) (string, error) {
	if name == "" {
		return "", errors.New("metric name must not be empty")
	}
	return sanitizeIdentifier(name, true), nil
}

func sanitizeLabelName(name string) string {
	return sanitizeIdentifier(name, false)
}

func sanitizeIdentifier(value string, allowColon bool) string {
	var builder strings.Builder
	for index, character := range value {
		valid := character == '_' || isASCIILetter(character) ||
			(index > 0 && isASCIIDigit(character)) ||
			(allowColon && character == ':')
		if valid {
			builder.WriteRune(character)
		} else {
			builder.WriteByte('_')
		}
	}
	return builder.String()
}

func isASCIILetter(character rune) bool {
	return character >= 'a' && character <= 'z' || character >= 'A' && character <= 'Z'
}

func isASCIIDigit(character rune) bool {
	return character >= '0' && character <= '9'
}

func sampleID(values []string) string {
	var builder strings.Builder
	for _, value := range values {
		builder.WriteString(strconv.Itoa(len(value)))
		builder.WriteByte(':')
		builder.WriteString(value)
	}
	return builder.String()
}

func equalStrings(left, right []string) bool {
	if len(left) != len(right) {
		return false
	}
	for index := range left {
		if left[index] != right[index] {
			return false
		}
	}
	return true
}

// Describe implements prometheus.Collector.
func (e *Exporter) Describe(descriptions chan<- *prometheus.Desc) {
	e.mu.RLock()
	defer e.mu.RUnlock()
	for _, family := range e.families {
		descriptions <- family.desc
	}
}

// Collect implements prometheus.Collector.
func (e *Exporter) Collect(metrics chan<- prometheus.Metric) {
	e.mu.RLock()
	defer e.mu.RUnlock()
	for _, family := range e.families {
		valueType := prometheus.GaugeValue
		if family.metricType == model.MetricTypeCounter {
			valueType = prometheus.CounterValue
		}
		for _, current := range family.samplesByID {
			metric, err := prometheus.NewConstMetric(
				family.desc,
				valueType,
				current.value,
				current.labelValues...,
			)
			if err != nil {
				metrics <- prometheus.NewInvalidMetric(family.desc, err)
				continue
			}
			metrics <- metric
		}
	}
}

// Start begins serving the registry. It returns after the listening socket is
// ready, so upstream components can safely start afterward.
func (e *Exporter) Start(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return fmt.Errorf("start Prometheus exporter: %w", err)
	}

	e.lifecycleMu.Lock()
	defer e.lifecycleMu.Unlock()
	if e.server != nil {
		return errors.New("start Prometheus exporter: already started")
	}
	listener, err := net.Listen("tcp", e.address)
	if err != nil {
		return fmt.Errorf("listen for Prometheus scrapes on %q: %w", e.address, err)
	}
	server := &http.Server{
		Handler:           promhttp.HandlerFor(e.registry, promhttp.HandlerOpts{}),
		ReadHeaderTimeout: 5 * time.Second,
	}
	e.listener = listener
	e.server = server
	go func() {
		_ = server.Serve(listener)
	}()
	return nil
}

// Shutdown gracefully stops the scrape endpoint. Calling it before Start or
// more than once is safe.
func (e *Exporter) Shutdown(ctx context.Context) error {
	e.lifecycleMu.Lock()
	server := e.server
	e.server = nil
	e.listener = nil
	e.lifecycleMu.Unlock()
	if server == nil {
		return nil
	}
	if err := server.Shutdown(ctx); err != nil {
		return fmt.Errorf("shutdown Prometheus exporter: %w", err)
	}
	return nil
}

// Address returns the actual listening address after Start, including the port
// selected by the OS when the configured port was zero.
func (e *Exporter) Address() string {
	e.lifecycleMu.Lock()
	defer e.lifecycleMu.Unlock()
	if e.listener == nil {
		return e.address
	}
	return e.listener.Addr().String()
}
