"""Opt-in OTLP tracing shared by the API and listener processes."""

import os

from opentelemetry import trace
from opentelemetry.exporter.otlp.proto.grpc.trace_exporter import OTLPSpanExporter
from opentelemetry.sdk.resources import Resource
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor


def configure_tracing(service_name: str) -> TracerProvider | None:
    """Export spans when an OTLP endpoint is configured; otherwise do no work."""
    if not os.environ.get("OTEL_EXPORTER_OTLP_ENDPOINT"):
        return None

    provider = TracerProvider(resource=Resource.create({"service.name": service_name}))
    provider.add_span_processor(BatchSpanProcessor(OTLPSpanExporter()))
    trace.set_tracer_provider(provider)
    return provider
