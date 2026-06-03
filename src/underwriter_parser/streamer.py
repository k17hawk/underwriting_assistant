import os
import json
from datetime import datetime
from confluent_kafka import Producer

from tenacity import retry, stop_after_attempt, wait_exponential

class KafkaHandoff:
    def __init__(self, bootstrap_servers: str = None, topic: str = None):
        self.bootstrap_servers = bootstrap_servers or os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
        self.topic = topic or os.getenv("KAFKA_HANDOFF_TOPIC", "underwriting-handoff")
        conf = {'bootstrap.servers': self.bootstrap_servers}
        self.producer = Producer(conf)

    def _delivery_report(self, err, msg):
        if err is not None:
            print(f"Message delivery failed: {err}")
        else:
            print(f"Message delivered to {msg.topic()} [{msg.partition()}]")

    @retry(stop=stop_after_attempt(5), wait=wait_exponential(multiplier=0.5, min=0.5, max=8))
    def send(self, artifact_id: str, schema_version: str = "v3"):
        message = {
            "artifact_id": artifact_id,
            "timestamp_extracted": datetime.utcnow().isoformat(),
            "schema_version": schema_version,
        }
        self.producer.produce(
            self.topic,
            key=artifact_id.encode('utf-8'),
            value=json.dumps(message).encode('utf-8'),
            callback=self._delivery_report
        )
        self.producer.flush()