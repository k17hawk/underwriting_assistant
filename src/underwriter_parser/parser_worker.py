# parser_worker.py
import os
import json
import time
import signal
from pathlib import Path
from typing import Optional, Dict, Any
from datetime import datetime
from confluent_kafka import Consumer, Producer
from opentelemetry import trace
from opentelemetry.trace import Status, StatusCode
import fitz

from src.underwriter_parser.entity import config
from underwriter_parser.mongodb_storage import MongoDBSubmissionStore
from src.underwriter_parser.models import UnderwritingSubmission

class ParserWorker:
    """Async worker that consumes from Kafka parser-input topic."""
    
    def __init__(self):
        self.artifact_dir = Path(config.storage.artifact_dir)
        self.running = True
        
        # Initialize MongoDB
        self.db_store = MongoDBSubmissionStore()
        
        # Initialize Kafka Consumer
        consumer_conf = {       
            'bootstrap.servers': config.kafka.bootstrap_servers,
            'group.id': 'parser-worker-group',
            'auto.offset.reset': 'earliest',
            'enable.auto.commit': True,
            'auto.commit.interval.ms': 5000,
            'max.poll.interval.ms': config.parsing.max_retries * 60000,  # Timeout for processing
        }
        self.consumer = Consumer(consumer_conf)
        self.consumer.subscribe([config.kafka.parser_input_topic])
        
        # Initialize Kafka Producer for output
        producer_conf = {
            'bootstrap.servers': config.kafka.bootstrap_servers,
            'client.id': 'parser-worker-producer'
        }
        self.producer = Producer(producer_conf)
        
        # DeepSeek config
        self.deepseek_config = config.deepseek
        
        self.tracer = trace.get_tracer("parser_worker")
        
        # Setup signal handlers for graceful shutdown
        signal.signal(signal.SIGINT, self._shutdown)
        signal.signal(signal.SIGTERM, self._shutdown)
    
    def _shutdown(self, signum, frame):
        print("Shutting down worker...")
        self.running = False
    
    def _delivery_report(self, err, msg):
        if err is not None:
            print(f"Output message delivery failed: {err}")
    
    def call_deepseek(self, raw_text: str) -> Dict[str, Any]:
        """Call DeepSeek API once (no retry)."""
        import requests
        
        system_prompt = self._load_system_prompt()
        user_prompt = f"Document content:\n{raw_text}\n\nExtract the submission data."
        
        try:
            response = requests.post(
                self.deepseek_config.endpoint,
                headers={"Authorization": f"Bearer {self.deepseek_config.api_key}"},
                json={
                    "model": self.deepseek_config.model,
                    "messages": [
                        {"role": "system", "content": system_prompt},
                        {"role": "user", "content": user_prompt}
                    ],
                    "temperature": self.deepseek_config.temperature,
                    "response_format": {"type": "json_object"}
                },
                timeout=self.deepseek_config.timeout_seconds
            )
            response.raise_for_status()
            
            result = response.json()
            extracted = json.loads(result["choices"][0]["message"]["content"])
            
            return {
                "status": "SUCCESS",
                "error_message": None,
                "data": extracted
            }
            
        except requests.exceptions.Timeout:
            return {
                "status": "TIMEOUT",
                "error_message": "DeepSeek API call timed out",
                "data": None
            }
        except requests.exceptions.RequestException as e:
            if hasattr(e, 'response') and e.response:
                if 400 <= e.response.status_code < 500:
                    error_type = "CLIENT_ERROR"
                else:
                    error_type = "SERVER_ERROR"
            else:
                error_type = "SERVER_ERROR"
            
            return {
                "status": error_type,
                "error_message": str(e),
                "data": None
            }
        except json.JSONDecodeError as e:
            return {
                "status": "CLIENT_ERROR",
                "error_message": f"Failed to parse DeepSeek response: {e}",
                "data": None
            }
        except Exception as e:
            return {
                "status": "SERVER_ERROR",
                "error_message": f"Unexpected error: {e}",
                "data": None
            }
    
    def _load_system_prompt(self) -> str:
        """Load system prompt for DeepSeek."""
        prompt_version = config.parsing.prompt_version
        prompt_file = Path(f"/src/underwriter_parser/prompts/system_prompt_{prompt_version}.txt")
        
        if not prompt_file.exists():
            # Fallback to the one in your codebase
            try:
                from src.underwriter_parser.parser import load_system_prompt
                return load_system_prompt(prompt_version)
            except:
                return """You are an expert insurance underwriting assistant.
Extract data from the submission document and output a JSON object that exactly matches the UnderwritingSubmission v3 schema.
Required fields are marked [R] in the schema. Use reasonable defaults for optional fields if missing.
Return ONLY valid JSON, no extra text."""
        
        with open(prompt_file, 'r') as f:
            return f.read()
    
    def extract_text_from_pdf(self, pdf_path: Path) -> str:
        """Extract text from PDF using PyMuPDF."""
        doc = fitz.open(pdf_path)
        text = ""
        for page in doc:
            text += page.get_text()
        doc.close()
        
        return text
    
    def validate_and_store_artifact(self, correlation_id: str, extracted_dict: Dict[str, Any]) -> bool:
        """Validate extracted data against Pydantic schema and store."""
        try:
            extracted_dict["correlation_id"] = correlation_id
            extracted_dict["schema_version"] = config.parsing.prompt_version
            extracted_dict["timestamp_extracted"] = datetime.now().isoformat()
            
            artifact = UnderwritingSubmission(**extracted_dict)
            
            self.db_store.store_artifact(
                correlation_id,
                artifact.model_dump(),
                is_valid=True
            )
            
            return True
            
        except Exception as e:
            self.db_store.store_artifact(
                correlation_id,
                extracted_dict,
                is_valid=False
            )
            return False
    
    def save_parsed_json(self, correlation_id: str, data: Dict[str, Any]) -> Path:
        """Save parsed JSON to artifact directory."""
        artifact_path = self.artifact_dir / correlation_id
        artifact_path.mkdir(exist_ok=True)
        
        json_path = artifact_path / f"{correlation_id}.json"
        with open(json_path, 'w') as f:
            json.dump(data, f, indent=2)
        
        return json_path
    
    def send_to_kafka_handoff(self, correlation_id: str):
        """Send parsed result to Kafka handoff topic for downstream processing."""
        from src.underwriter_parser.streamer import KafkaHandoff
        
        kafka = KafkaHandoff(
            bootstrap_servers=config.kafka.bootstrap_servers,
            topic=config.kafka.handoff_topic
        )
        kafka.send(correlation_id, config.parsing.prompt_version)
    
    def send_output_to_kafka(self, correlation_id: str, status: str,
                             error_message: Optional[str] = None,
                             parsed_json_path: Optional[str] = None):
        """Send result to Kafka parser-output topic."""
        message = {
            "correlation_id": correlation_id,
            "status": status,
            "timestamp": datetime.now().isoformat()
        }
        
        if error_message:
            message["error_message"] = error_message
        
        if parsed_json_path:
            message["parsed_json_path"] = parsed_json_path
        
        self.producer.produce(
            config.kafka.parser_output_topic,
            key=correlation_id.encode('utf-8'),
            value=json.dumps(message).encode('utf-8'),
            callback=self._delivery_report
        )
        self.producer.flush()
    
    def process_message(self, message: Dict[str, Any]):
        """Process a single message from Kafka."""
        correlation_id = message["correlation_id"]
        file_path = message["file_path"]
        
        with self.tracer.start_as_current_span("parser_worker.process") as span:
            span.set_attribute("correlation_id", correlation_id)
            
            try:
                print(f"📥 Processing message for: {correlation_id}")
                
                pdf_path = Path(file_path)
                if not pdf_path.exists():
                    raise FileNotFoundError(f"PDF not found: {pdf_path}")
                
                # Extract text from PDF
                print(f"📄 Extracting text from PDF: {pdf_path}")
                raw_text = self.extract_text_from_pdf(pdf_path)
                
                if not raw_text or len(raw_text.strip()) == 0:
                    error_message = "No text extracted from PDF"
                    print(f"⚠️ {error_message}")
                    self.db_store.update_submission_status(
                        correlation_id,
                        "FAILED",
                        error_type="EMPTY_RESULT",
                        error_message=error_message
                    )
                    self.send_output_to_kafka(correlation_id, "EMPTY_RESULT", error_message=error_message)
                    return
                
                # Call DeepSeek API
                print(f"🤖 Calling DeepSeek API for: {correlation_id}")
                result = self.call_deepseek(raw_text)
                
                status = result["status"]
                error_message = result.get("error_message")
                parsed_json_path = None
                
                if status == "SUCCESS":
                    print(f"✅ DeepSeek extraction successful")
                    
                    # Validate and store artifact - THIS UPDATES MONGODB
                    is_valid = self.validate_and_store_artifact(correlation_id, result["data"])
                    
                    if is_valid:
                        json_path = self.save_parsed_json(correlation_id, result["data"])
                        parsed_json_path = str(json_path)
                        
                        # Update MongoDB status - THIS UPDATES MONGODB
                        print(f"📊 Updating MongoDB status to COMPLETED")
                        self.db_store.update_submission_status(
                            correlation_id,
                            "COMPLETED",
                            parsed_json_path=parsed_json_path
                        )
                        
                        # Send to Kafka handoff topic
                        self.send_to_kafka_handoff(correlation_id)
                        
                        # Mark as processed (idempotency) - THIS UPDATES MONGODB
                        self.db_store.mark_completed(correlation_id)
                        print(f"✅ Completed processing for: {correlation_id}")
                    else:
                        status = "VALIDATION_FAILED"
                        error_message = "Schema validation failed"
                        print(f"⚠️ Validation failed for: {correlation_id}")
                        self.db_store.update_submission_status(
                            correlation_id,
                            "FAILED",
                            error_type=status,
                            error_message=error_message
                        )
                else:
                    print(f"❌ DeepSeek failed: {status} - {error_message}")
                    self.db_store.update_submission_status(
                        correlation_id,
                        "FAILED",
                        error_type=status,
                        error_message=error_message
                    )
                
                self.send_output_to_kafka(
                    correlation_id,
                    status,
                    error_message=error_message,
                    parsed_json_path=parsed_json_path
                )
                
            except Exception as e:
                error_message = f"Unexpected error: {str(e)}"
                print(f"❌ Unexpected error: {error_message}")
                self.db_store.update_submission_status(
                    correlation_id,
                    "FAILED",
                    error_type="UNEXPECTED",
                    error_message=error_message
                )
                self.send_output_to_kafka(correlation_id, "UNEXPECTED", error_message=error_message)
                
    def run(self):
        """Main worker loop - poll Kafka for messages."""
        print(f"Parser worker started, consuming from {config.kafka.parser_input_topic}...")
        
        try:
            while self.running:
                msg = self.consumer.poll(1.0)  # 1 second timeout
                
                if msg is None:
                    continue
                
                if msg.error():
                    print(f"Consumer error: {msg.error()}")
                    continue
                
                try:
                    message = json.loads(msg.value().decode('utf-8'))
                    self.process_message(message)
                except Exception as e:
                    print(f"Error processing message: {e}")
                    # Message will be re-consumed after group rebalance or if we don't commit
                    
        except KeyboardInterrupt:
            print("Interrupted by user")
        finally:
            self.consumer.close()
            print("Consumer closed")

if __name__ == "__main__":
    worker = ParserWorker()
    worker.run()