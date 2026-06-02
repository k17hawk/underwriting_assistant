import hashlib
from datetime import datetime
from typing import Union, Optional
from opentelemetry import trace
from opentelemetry.trace import Status, StatusCode

from .models import UnderwritingSubmission
from .parser import LLMParser
from .storage import MongoArtifactStore, MongoIdempotencyStore
from .streamer import KafkaHandoff

class ExtractorHandoff:
    def __init__(self, parser: LLMParser = None):
        self.parser = parser or LLMParser()
        self.artifact_store = MongoArtifactStore()
        self.idempotency = MongoIdempotencyStore()
        self.streamer = KafkaHandoff()
        self.tracer = trace.get_tracer("extractor.handoff")

    def _compute_hash(self, data: bytes) -> str:
        return hashlib.sha256(data).hexdigest()

    def extract(self, raw_data: Union[bytes, str], correlation_id: Optional[str] = None) -> UnderwritingSubmission:
        if isinstance(raw_data, str):
            raw_data = raw_data.encode("utf-8")
        file_hash = self._compute_hash(raw_data)
        correlation_id = correlation_id or file_hash[:16]

        with self.tracer.start_as_current_span("extractor.handoff") as span:
            span.set_attribute("correlation_id", correlation_id)

            if self.idempotency.is_processed(correlation_id):
                span.set_attribute("idempotency_skipped", True)
                # Optionally return existing artifact
                existing = self.artifact_store.get_artifact(correlation_id)
                if existing:
                    return UnderwritingSubmission(**existing)
                raise RuntimeError(f"Already processed but artifact missing: {correlation_id}")

            # Input validation
            if len(raw_data) > 10 * 1024 * 1024:
                raise ValueError("File too large (>10MB)")

            # Parse with DeepSeek
            raw_text = raw_data.decode("utf-8", errors="replace")
            extracted_dict = self.parser.parse(raw_text)

            # Build final artifact dict
            extracted_dict["correlation_id"] = correlation_id
            extracted_dict["schema_version"] = "v3"
            extracted_dict["timestamp_extracted"] = datetime.utcnow()
            extracted_dict["source_file_hash"] = file_hash

            # Validate with Pydantic
            try:
                artifact = UnderwritingSubmission(**extracted_dict)
                is_valid = True
            except Exception as e:
                is_valid = False
                # Store invalid artifact for debugging
                self.artifact_store.store_artifact(correlation_id, extracted_dict, is_valid=False)
                raise RuntimeError(f"Schema validation failed: {e}")

            # Store valid artifact in MongoDB
            self.artifact_store.store_artifact(correlation_id, artifact.model_dump(), is_valid=True)

            # Send Kafka handoff message
            self.streamer.send(correlation_id, schema_version="v3")

            # Mark idempotency
            self.idempotency.mark_completed(correlation_id)

            span.set_status(Status(StatusCode.OK))
            return artifact