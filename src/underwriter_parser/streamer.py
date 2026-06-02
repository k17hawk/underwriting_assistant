import os
import json
from datetime import datetime

from kafka import KafkaProducer
from tenacity import retry, stop_after_attempt, wait_exponential

class KafkaHandoff:
    def __init__(self, bootstrap_servers: str = None, topic: str = None):
        self.bootstrap_servers = bootstrap_servers or os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
        self.topic = topic or os.getenv("KAFKA_HANDOFF_TOPIC", "underwriting-handoff")
        self.producer = KafkaProducer(
            bootstrap_servers=self.bootstrap_servers,
            value_serializer=lambda v: json.dumps(v).encode("utf-8")
        )

    @retry(stop=stop_after_attempt(5), wait=wait_exponential(multiplier=0.5, min=0.5, max=8))
    def send(self, artifact_id: str, schema_version: str = "v3"):
        message = {
            "artifact_id": artifact_id,
            "timestamp_extracted": datetime.utcnow().isoformat(),
            "schema_version": schema_version,
            # No S3 key – artifacts are fetched from MongoDB by the analyzer
        }
        self.producer.send(self.topic, key=artifact_id.encode(), value=message)
        self.producer.flush()