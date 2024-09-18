package tracer // this can be changed to any name

import (
	"context"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.17.0"
)

// InitTracer initializes the OpenTelemetry tracer
func InitTracer(serviceName string) func(context.Context) error {
	// Initialize the exporter
	exporter, err := otlptracegrpc.New(context.Background())
	if err != nil {
		panic(err)
	}

	attr := resource.WithAttributes(
		// Set the deployment environment semantic attribute
		semconv.DeploymentEnvironmentKey.String("production"), // You can change this value to "development" or "staging" or you can get the value from the environment variables
		// You can add more attributes here
	)

	// Create a new resource with the attributes
	resources, err := resource.New(context.Background(),
		resource.WithFromEnv(),
		resource.WithTelemetrySDK(),
		resource.WithProcess(),
		resource.WithOS(),
		resource.WithContainer(),
		resource.WithHost(),
		attr)

	if err != nil {
		panic(err)
	}

	// Create a new tracer provider with the exporter and resource
	tp := sdktrace.NewTracerProvider(
		// Batch the traces to the exporter using BatchSpanProcessor
		sdktrace.WithBatcher(exporter),
		sdktrace.WithResource(resources),
	)

	otel.SetTracerProvider(tp)
	otel.SetTextMapPropagator(propagation.NewCompositeTextMapPropagator(propagation.TraceContext{}, propagation.Baggage{}))

	return tp.Shutdown
}
