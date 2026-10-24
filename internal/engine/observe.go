package engine

import (
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"

	"github.com/omer-ulucan/baton/internal/decision"
)

// Observer receives engine events for metrics. Calls happen after the
// transaction that caused them committed.
type Observer interface {
	Claimed(latency time.Duration)
	TaskFinished(queue, outcome string, d time.Duration) // succeeded | failed | expired | released
	RetryScheduled(queue string)
	LeaseExpired(queue string)
	WorkflowFinished(status string)
	Fenced()
}

type noopObserver struct{}

func (noopObserver) Claimed(time.Duration)                      {}
func (noopObserver) TaskFinished(string, string, time.Duration) {}
func (noopObserver) RetryScheduled(string)                      {}
func (noopObserver) LeaseExpired(string)                        {}
func (noopObserver) WorkflowFinished(string)                    {}
func (noopObserver) Fenced()                                    {}

func decisionAttrs(kind decision.Kind, d decision.Decision) trace.SpanStartEventOption {
	attrs := []attribute.KeyValue{
		attribute.String("decision.kind", string(kind)), attribute.String("decision.policy", d.Policy),
		attribute.String("decision.label", d.Label), attribute.Float64("decision.confidence", d.Confidence),
	}
	if d.Fallback != "" {
		attrs = append(attrs, attribute.String("decision.fallback", d.Fallback))
	}
	if d.ModelRevision != "" {
		attrs = append(attrs, attribute.String("decision.model_revision", d.ModelRevision))
	}
	return trace.WithAttributes(attrs...)
}
