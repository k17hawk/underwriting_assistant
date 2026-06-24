# orchestrator.py
import os
import json
import uuid
from datetime import datetime
from pathlib import Path
from typing import Dict, Any, Optional
from confluent_kafka import Producer
from opentelemetry import trace
from opentelemetry.trace import Status, StatusCode

from src.underwriter_parser.entity.config import config 
from src.underwriter_parser.pre_checker import FilePrecheck
from src.underwriter_parser.mongodb_storage import MongoDBSubmissionStore

class Orchestrator:
    """Handles submission intake and Kafka publishing."""
    
    def __init__(self):
        self.artifact_dir = Path(config.storage.artifact_dir)
        self.artifact_dir.mkdir(parents=True, exist_ok=True)
        
        self.db_store = MongoDBSubmissionStore()
        
        kafka_conf = {
            'bootstrap.servers': config.kafka.bootstrap_servers,
            'client.id': 'orchestrator'
        }
        self.producer = Producer(kafka_conf)
        self.tracer = trace.get_tracer("orchestrator")
    
    def generate_correlation_id(self) -> str:
        timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
        uuid_segment = str(uuid.uuid4())[:8]
        return f"{timestamp}-{uuid_segment}"
    
    def save_file_to_artifact(self, correlation_id: str, file_content: bytes, 
                             original_filename: str = "document.pdf", 
                             is_converted: bool = False) -> Path:
        """Save file to local artifact directory."""
        artifact_path = self.artifact_dir / correlation_id
        artifact_path.mkdir(exist_ok=True)
        
        # ✅ If converted from image, save as PDF
        if is_converted:
            filename = "original.pdf"
        else:
            # Keep original filename but ensure it's safe
            filename = Path(original_filename).name
        
        file_path = artifact_path / filename
        with open(file_path, 'wb') as f:
            f.write(file_content)
        
        return file_path
    
    def send_to_kafka(self, correlation_id: str, file_path: Path, 
                     retry_count: int = 0, original_filename: Optional[str] = None):
        """Send message to Kafka parser-input topic."""
        span = trace.get_current_span()
        trace_id = span.get_span_context().trace_id.to_bytes(16, 'big').hex()
        
        message = {
            "correlation_id": correlation_id,
            "file_path": str(file_path),  # This is now the PDF path
            "original_filename": original_filename,
            "retry_count": retry_count,
            "traceparent": f"00-{trace_id}-0000000000000000-01"
        }
        
        def delivery_report(err, msg):
            if err is not None:
                print(f"❌ Message delivery failed: {err}")
            else:
                print(f"✅ Message delivered to {msg.topic()} [{msg.partition()}]")
        
        self.producer.produce(
            config.kafka.parser_input_topic,
            key=correlation_id.encode('utf-8'),
            value=json.dumps(message).encode('utf-8'),
            callback=delivery_report
        )
        self.producer.flush()
        
        print(f"📤 Sent to Kafka: {message}")
    
    def process_submission(self, file_content: bytes, 
                      original_filename: str = "document.pdf") -> Dict[str, Any]:
        """
        Orchestrate the submission intake process.
        Returns: correlation_id and initial status
        """
        print(f"🔍 orchestrator.process_submission called: {original_filename}, size={len(file_content)}")
        
        with self.tracer.start_as_current_span("orchestrator.process") as span:
            print("🔍 Phase 0: Starting synchronous pre-check...")
            
            try:
                is_valid, error_msg, processed_content = FilePrecheck.validate_pdf(file_content)
                print(f"✅ Pre-check result: is_valid={is_valid}, error_msg={error_msg}")
                
                if not is_valid:
                    span.set_status(Status(StatusCode.ERROR, error_msg))
                    raise ValueError(f"Pre-check failed: {error_msg}")
                
                print("✅ Pre-check passed")
                
                # Phase 1: Submission intake
                correlation_id = self.generate_correlation_id()
                span.set_attribute("correlation_id", correlation_id)
                
                print(f"📝 Phase 1: Generating correlation_id: {correlation_id}")
                
                is_converted = (processed_content != file_content)
                print(f"📝 is_converted: {is_converted}")
                
                file_path = self.save_file_to_artifact(
                    correlation_id, 
                    processed_content,
                    original_filename=original_filename,
                    is_converted=is_converted
                )
                print(f"💾 Saved file to: {file_path}")
                
                # Get page count
                import fitz
                doc = fitz.open(stream=processed_content, filetype="pdf")
                page_count = len(doc)
                doc.close()
                print(f"📄 PDF has {page_count} pages")
                
                print(f"📊 Saving to MongoDB...")
                record = self.db_store.create_submission_record(
                    correlation_id, 
                    str(file_path),
                    original_filename=original_filename,
                    page_count=page_count
                )
                print(f"✅ MongoDB record created: {record['correlation_id']}")
                
                # Send to Kafka
                print(f"📤 Sending to Kafka...")
                self.send_to_kafka(correlation_id, file_path, original_filename=original_filename)
                
                span.set_status(Status(StatusCode.OK))
                
                result = {
                    "correlation_id": correlation_id,
                    "status": record["status"],
                    "file_path": str(file_path),
                    "original_filename": original_filename,
                    "is_converted": is_converted,
                    "page_count": page_count,
                    "message": "Submission accepted and queued for parsing"
                }
                print(f"✅ Returning result: {result}")
                return result
                
            except Exception as e:
                print(f"❌ Error in orchestrator.process_submission: {e}")
                import traceback
                traceback.print_exc()
                raise