# mongodb_storage.py
import os
from datetime import datetime
from typing import Optional, Dict, Any
from tenacity import retry, stop_after_attempt, wait_exponential

from src.underwriter_parser.entity.config import config
from mongodb_connections import mongo_connection
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type
import pymongo.errors
from datetime import date 
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
                            original_filename: str = None,
                            page_count: int = 0) -> Dict[str, Any]:
        """Create initial submission record."""
        record = {
            "correlation_id": correlation_id,
            "status": "PARSING",
            "retry_count": 0,
            "error_type": None,
            "error_message": None,
            "file_path": file_path,
            "original_filename": original_filename,
            "page_count": page_count,
            "parsed_json_path": None,
            "created_at": datetime.now(),
            "updated_at": datetime.now()
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
    

    def store_deepseek_response(self, correlation_id: str, deepseek_response: dict, is_valid: bool = True):
        try:
            from datetime import date

            def clean_and_convert(obj):
                """Clean keys and convert date/datetime objects."""
                if isinstance(obj, datetime):
                    return obj.isoformat()
                if isinstance(obj, date):
                    return obj.isoformat()
                if isinstance(obj, dict):
                    return {
                        k.replace('.', '_').replace('$', '_'): clean_and_convert(v)
                        for k, v in obj.items()
                    }
                if isinstance(obj, list):
                    return [clean_and_convert(item) for item in obj]
                return obj

            cleaned_response = clean_and_convert(deepseek_response)

            doc = {
                "correlation_id": correlation_id,
                "deepseek_response": cleaned_response,
                "valid": is_valid,
                "stored_at": datetime.utcnow()
            }

            self.artifacts.update_one(
                {"correlation_id": correlation_id},
                {"$set": doc},
                upsert=True
            )
            print(f"✅ Full DeepSeek response stored in MongoDB")

        except Exception as e:
            print(f"❌ Failed to store DeepSeek response: {e}")
            try:
                import json
                doc = {
                    "correlation_id": correlation_id,
                    "deepseek_response_json": json.dumps(deepseek_response, default=str),
                    "valid": is_valid,
                    "stored_at": datetime.utcnow()
                }
                self.artifacts.update_one(
                    {"correlation_id": correlation_id},
                    {"$set": doc},
                    upsert=True
                )
                print(f"✅ Full DeepSeek response stored as JSON string")
            except Exception as e2:
                print(f"❌ Fallback storage failed: {e2}")      
    @retry(
     stop=stop_after_attempt(3), 
     wait=wait_exponential(multiplier=1, min=1, max=10),
     retry=retry_if_exception_type((pymongo.errors.OperationFailure, pymongo.errors.ServerSelectionTimeoutError))
   )
   
    def store_artifact(self, correlation_id: str, artifact_dict: dict, is_valid: bool = True):
        """Store parsed artifact in artifacts collection."""
        try:
            def clean_keys(obj):
                """Replace dots and dollar signs in dictionary keys."""
                if isinstance(obj, dict):
                    new_dict = {}
                    for key, value in obj.items():
                        new_key = key.replace('.', '_').replace('$', '_')
                        new_dict[new_key] = clean_keys(value)
                    return new_dict
                elif isinstance(obj, list):
                    return [clean_keys(item) for item in obj]
                else:
                    return obj
            
            cleaned_artifact = clean_keys(artifact_dict)
            
            def convert_datetime(obj):
                
                if isinstance(obj, datetime):
                    return obj.isoformat()
                if isinstance(obj, date): 
                    return obj.isoformat()
                if isinstance(obj, dict):
                    return {k: convert_datetime(v) for k, v in obj.items()}
                if isinstance(obj, list):
                    return [convert_datetime(item) for item in obj]
                return obj
            
            cleaned_artifact = convert_datetime(cleaned_artifact)
            
            doc = {
                "correlation_id": correlation_id,
                "artifact": cleaned_artifact,
                "valid": is_valid,
                "schema_version": config.parsing.prompt_version,
                "stored_at": datetime.utcnow()
            }
            
            # ✅ Use replace_one with upsert instead of update_one
            result = self.artifacts.replace_one(
                {"correlation_id": correlation_id},
                doc,
                upsert=True
            )
            
            if result.upserted_id:
                print(f"✅ Artifact inserted with ID: {result.upserted_id}")
            else:
                print(f"✅ Artifact updated for correlation_id: {correlation_id}")
            
            return result
            
        except Exception as e:
            print(f"❌ Failed to store artifact: {e}")
            # Try fallback: store as JSON string
            try:
                import json
                artifact_json = json.dumps(artifact_dict, default=str)
                
                doc = {
                    "correlation_id": correlation_id,
                    "artifact_json": artifact_json,
                    "valid": is_valid,
                    "schema_version": config.parsing.prompt_version,
                    "stored_at": datetime.utcnow()
                }
                
                self.artifacts.replace_one(
                    {"correlation_id": correlation_id},
                    doc,
                    upsert=True
                )
                print(f"✅ Artifact stored as JSON string fallback")
            except Exception as e2:
                print(f"❌ Fallback storage also failed: {e2}")
                raise
    
    @retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=1, max=10))
    def get_artifact(self, correlation_id: str) -> Optional[Dict[str, Any]]:
        """Get artifact by correlation_id from artifacts collection."""
        doc = self.artifacts.find_one({"correlation_id": correlation_id})
        if doc:
            if "artifact_data" in doc:
                import json
                return json.loads(doc["artifact_data"])
            elif "artifact" in doc:
                return doc["artifact"]
        return None
    
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