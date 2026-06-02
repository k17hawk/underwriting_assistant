import os
from datetime import datetime
from typing import Optional

from pymongo import MongoClient
from tenacity import retry, stop_after_attempt, wait_exponential

class MongoArtifactStore:
    def __init__(self, uri: str = None, db_name: str = None, collection_name: str = None):
        self.uri = uri or os.getenv("MONGODB_URI", "mongodb://localhost:27017")
        self.db_name = db_name or os.getenv("MONGODB_DATABASE", "underwriting")
        self.collection_name = collection_name or os.getenv("MONGODB_ARTIFACTS_COLLECTION", "artifacts")
        self.client = MongoClient(self.uri)
        self.db = self.client[self.db_name]
        self.collection = self.db[self.collection_name]

    @retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=1, max=10))
    def store_artifact(self, correlation_id: str, artifact_dict: dict, is_valid: bool = True):
        doc = {
            "_id": correlation_id,
            "correlation_id": correlation_id,
            "artifact": artifact_dict,
            "valid": is_valid,
            "stored_at": datetime.utcnow()
        }
        self.collection.update_one({"_id": correlation_id}, {"$set": doc}, upsert=True)

    @retry(stop=stop_after_attempt(3))
    def get_artifact(self, correlation_id: str) -> Optional[dict]:
        doc = self.collection.find_one({"_id": correlation_id})
        return doc["artifact"] if doc else None

class MongoIdempotencyStore:
    def __init__(self, uri: str = None, db_name: str = None, collection_name: str = None):
        self.uri = uri or os.getenv("MONGODB_URI")
        self.db_name = db_name or os.getenv("MONGODB_DATABASE")
        self.collection_name = collection_name or os.getenv("MONGODB_IDEMPOTENCY_COLLECTION")
        self.client = MongoClient(self.uri)
        self.db = self.client[self.db_name]
        self.collection = self.db[self.collection_name]

    def is_processed(self, correlation_id: str) -> bool:
        return self.collection.find_one({"_id": correlation_id}) is not None

    def mark_completed(self, correlation_id: str):
        self.collection.update_one(
            {"_id": correlation_id},
            {"$set": {"processed_at": datetime.utcnow()}},
            upsert=True
        )