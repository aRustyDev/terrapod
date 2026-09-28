"""Trace export remains opt-in and uses distinct process identities."""

from terrapod import tracing


def test_tracing_is_idle_without_endpoint(monkeypatch):
    monkeypatch.delenv("OTEL_EXPORTER_OTLP_ENDPOINT", raising=False)
    monkeypatch.setattr(
        tracing,
        "OTLPSpanExporter",
        lambda: (_ for _ in ()).throw(AssertionError("exporter created")),
    )

    assert tracing.configure_tracing("terrapod-api") is None


def test_tracing_configures_otlp_export(monkeypatch):
    monkeypatch.setenv("OTEL_EXPORTER_OTLP_ENDPOINT", "http://127.0.0.1:4317")
    seen = {}

    class Provider:
        def __init__(self, resource):
            seen["resource"] = resource

        def add_span_processor(self, processor):
            seen["processor"] = processor

    monkeypatch.setattr(tracing, "TracerProvider", Provider)
    monkeypatch.setattr(tracing, "OTLPSpanExporter", lambda: "exporter")
    monkeypatch.setattr(tracing, "BatchSpanProcessor", lambda exporter: ("batch", exporter))
    monkeypatch.setattr(
        tracing.trace, "set_tracer_provider", lambda provider: seen.update(global_provider=provider)
    )

    provider = tracing.configure_tracing("terrapod-listener")

    assert provider is seen["global_provider"]
    assert seen["resource"].attributes["service.name"] == "terrapod-listener"
    assert seen["processor"] == ("batch", "exporter")
