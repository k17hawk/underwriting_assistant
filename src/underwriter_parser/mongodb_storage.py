# mongodb_storage.py
import os
from datetime import datetime
from typing import Optional, Dict, Any
from tenacity import retry, stop_after_attempt, wait_exponential

from src.underwriter_parser.entity.config import config
from mongodb_connections import mongo_connection
class MongoDBSubmissionStore:
    def __init__(self):
        self.client = mongo_connection.client
        self.database_name = config.mongodb.database
        
        self.db = self.client[self.database_name]
        self.submissions = self.db[config.mongodb.files_collection]
        self.artifacts = self.db[config.mongodb.artifacts_collection]
        self.idempotency = self.db[config.mongodb.idempotency_collection]
        
        self._create_indexes()
    
    def _create_indexes(self):
        """Create necessary indexes."""
        # ✅ _id index is created automatically by MongoDB
        # Only create indexes on other fields
        
        # Submissions collection indexes
        self.submissions.create_index("correlation_id", unique=True)
        self.submissions.create_index("status")
        self.submissions.create_index("created_at")
        
        # Artifacts collection indexes
        self.artifacts.create_index("correlation_id", unique=True)
        
        # Idempotency collection - _id already has index, no need to create
    
    @retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=1, max=10))
    def create_submission_record(self, correlation_id: str, file_path: str, 
                                original_filename: str = None) -> Dict[str, Any]:
        """Create initial submission record."""
        record = {
            "correlation_id": correlation_id,
            "status": "PARSING",
            "retry_count": 0,
            "error_type": None,
            "error_message": None,
            "file_path": file_path,
            "original_filename": original_filename,
            "parsed_json_path": None,
            "created_at": datetime.utcnow(),
            "updated_at": datetime.utcnow()
        }
        
        result = self.submissions.insert_one(record)
        print(f"✅ Submission record inserted with ID: {result.inserted_id}")
        return record
    
    @retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=1, max=10))
    def update_submission_status(self, correlation_id: str, status: str,
                                error_type: Optional[str] = None,
                                error_message: Optional[str] = None,
                                parsed_json_path: Optional[str] = None,
                                increment_retry: bool = False):
        """Update submission record status."""
        update_data = {
            "status": status,
            "updated_at": datetime.utcnow()
        }
        
        if error_type is not None:
            update_data["error_type"] = error_type
        
        if error_message is not None:
            update_data["error_message"] = error_message
        
        if parsed_json_path is not None:
            update_data["parsed_json_path"] = parsed_json_path
        
        update_operation = {"$set": update_data}
        
        if increment_retry:
            update_operation["$inc"] = {"retry_count": 1}
        
        result = self.submissions.update_one(
            {"correlation_id": correlation_id},
            update_operation
        )
        
        if result.modified_count > 0:
            print(f"✅ Updated submission {correlation_id} status to {status}")
        else:
            print(f"⚠️ No changes made to submission {correlation_id}")
    
    @retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=1, max=10))
    def get_submission_record(self, correlation_id: str) -> Optional[Dict[str, Any]]:
        """Get submission record by correlation_id."""
        return self.submissions.find_one({"correlation_id": correlation_id})
    
    @retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=1, max=10))
    def store_artifact(self, correlation_id: str, artifact_dict: dict, is_valid: bool = True):
        """Store parsed artifact in artifacts collection."""
        doc = {
            "correlation_id": correlation_id,
            "artifact": artifact_dict,
            "valid": is_valid,
            "schema_version": config.parsing.prompt_version,
            "stored_at": datetime.utcnow()
        }
        
        result = self.artifacts.update_one(
            {"correlation_id": correlation_id},
            {"$set": doc},
            upsert=True
        )
        
        if result.upserted_id:
            print(f"✅ Artifact inserted with ID: {result.upserted_id}")
        else:
            print(f"✅ Artifact updated for correlation_id: {correlation_id}")
    
    @retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=1, max=10))
    def get_artifact(self, correlation_id: str) -> Optional[Dict[str, Any]]:
        """Get artifact by correlation_id from artifacts collection."""
        doc = self.artifacts.find_one({"correlation_id": correlation_id})
        return doc.get("artifact") if doc else None
    
    @retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=1, max=10))
    def is_processed(self, correlation_id: str) -> bool:
        """Check if correlation_id has been processed (idempotency)."""
        return self.idempotency.find_one({"_id": correlation_id}) is not None
    
    @retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=1, max=10))
    def mark_completed(self, correlation_id: str):
        """Mark correlation_id as processed (idempotency)."""
        result = self.idempotency.update_one(
            {"_id": correlation_id},
            {"$set": {"processed_at": datetime.utcnow()}},
            upsert=True
        )
        print(f"✅ Idempotency marked for: {correlation_id}")