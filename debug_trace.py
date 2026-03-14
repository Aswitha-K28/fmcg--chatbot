import os
import time
from opentelemetry import trace
from opentelemetry.exporter.otlp.proto.http.trace_exporter import OTLPSpanExporter
from opentelemetry.sdk.resources import Resource
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor

# Manually setup a simple trace to test Phoenix connectivity
os.environ["PHOENIX_COLLECTOR_HTTP_ENDPOINT"] = "http://localhost:6006/v1/traces"
endpoint = "http://localhost:6006/v1/traces"

print(f"Testing connectivity to {endpoint}...")

resource = Resource(attributes={"service.name": "debug-trace-service"})
provider = TracerProvider(resource=resource)
exporter = OTLPSpanExporter(endpoint=endpoint)
processor = BatchSpanProcessor(exporter)
provider.add_span_processor(processor)
trace.set_tracer_provider(provider)

tracer = trace.get_tracer(__name__)

with tracer.start_as_current_span("debug-span") as span:
    span.set_attribute("debug.message", "This is a test trace from Antigravity")
    print("Created debug span.")

print("Exporting...")
processor.force_flush()
time.sleep(2)
print("Done. Please refresh Phoenix UI.")
