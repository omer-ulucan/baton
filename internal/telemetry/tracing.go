package telemetry

import (
	"context"
	"os"

	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracehttp"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.43.0"
	"go.opentelemetry.io/otel/trace"
	"go.opentelemetry.io/otel/trace/noop"
)

// Propagator carries W3C trace context across HTTP calls and into tasks.
var Propagator propagation.TextMapPropagator = propagation.TraceContext{}

// Tracing is a tracer provider and how to flush it.
type Tracing struct {
	Provider trace.TracerProvider
	Shutdown func(context.Context) error
}

// NewTracing returns an OTLP/HTTP-exporting provider when
// OTEL_EXPORTER_OTLP_ENDPOINT (or the traces-specific variable) is set,
// and a no-op provider otherwise. The exporter and sampler read the
// standard OTEL_* environment variables.
func NewTracing(ctx context.Context, service string, getenv func(string) string) (Tracing, error) {
	if getenv("OTEL_EXPORTER_OTLP_ENDPOINT") == "" && getenv("OTEL_EXPORTER_OTLP_TRACES_ENDPOINT") == "" {
		return Tracing{Provider: noop.NewTracerProvider(), Shutdown: func(context.Context) error { return nil }}, nil
	}
	exp, err := otlptracehttp.New(ctx)
	if err != nil {
		return Tracing{}, err
	}
	host, _ := os.Hostname()
	res := resource.NewSchemaless(semconv.ServiceName(service), semconv.ServiceInstanceID(host))
	tp := sdktrace.NewTracerProvider(sdktrace.WithBatcher(exp), sdktrace.WithResource(res))
	return Tracing{Provider: tp, Shutdown: tp.Shutdown}, nil
}

// TraceParent is the W3C traceparent of the span in ctx ("" if none).
func TraceParent(ctx context.Context) string {
	c := propagation.MapCarrier{}
	Propagator.Inject(ctx, c)
	return c["traceparent"]
}

// WithTraceParent returns ctx carrying a remote parent span context.
func WithTraceParent(ctx context.Context, traceparent string) context.Context {
	if traceparent == "" {
		return ctx
	}
	return Propagator.Extract(ctx, propagation.MapCarrier{"traceparent": traceparent})
}
