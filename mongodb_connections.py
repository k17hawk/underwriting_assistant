# mongodb_connections.py
import os
import certifi
import pymongo
from typing import Optional
from dotenv import load_dotenv

load_dotenv()

class MongoDBConnection:
    """Singleton MongoDB connection manager with TLS certificate."""
    
    _instance: Optional['MongoDBConnection'] = None
    _client: Optional[pymongo.MongoClient] = None
    
    def __new__(cls):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
        return cls._instance
    
    def __init__(self):
        if self._client is None:
            self._connect()
    
    def _connect(self):
        """Establish MongoDB connection with TLS certificate."""
        mongo_uri = os.getenv("MONGODB_URI")
        if not mongo_uri:
            raise ValueError("MONGODB_URI environment variable is not set")
        
        try:
            # Get certificate for TLS
            ca = certifi.where()
            
            # Connect with TLS certificate - Option A: Using tlsCAFile
            self._client = pymongo.MongoClient(
                mongo_uri,
                tlsCAFile=ca,
                tls=True,
                tlsAllowInvalidCertificates=True,  # ⚠️ For development only
                serverSelectionTimeoutMS=30000,  # 30 seconds timeout
                connectTimeoutMS=30000,
                socketTimeoutMS=30000,
            )
            
            # Test connection
            self._client.admin.command('ping')
            print("✅ MongoDB connection established successfully")
            
        except Exception as e:
            # Try alternative connection method
            try:
                print("⚠️ First connection attempt failed, trying alternative...")
                
                # Option B: Without explicit tlsCAFile
                self._client = pymongo.MongoClient(
                    mongo_uri,
                    tls=True,
                    tlsAllowInvalidCertificates=True,
                    serverSelectionTimeoutMS=30000,
                    connectTimeoutMS=30000,
                    socketTimeoutMS=30000,
                )
                
                self._client.admin.command('ping')
                print("✅ MongoDB connection established successfully (alternative)")
                
            except Exception as e2:
                raise ConnectionError(f"Failed to connect to MongoDB: {e2}")
    
    @property
    def client(self) -> pymongo.MongoClient:
        """Get MongoDB client."""
        if self._client is None:
            self._connect()
        return self._client
    
    def get_database(self, db_name: str):
        """Get database instance."""
        return self.client[db_name]
    
    def get_collection(self, db_name: str, collection_name: str):
        """Get collection instance."""
        return self.client[db_name][collection_name]


mongo_connection = MongoDBConnection()