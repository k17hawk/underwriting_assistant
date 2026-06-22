# config.py
import os
import yaml
from pathlib import Path
from typing import Optional, Dict, Any
from pydantic import BaseModel, Field
from dotenv import load_dotenv

# Load .env first
load_dotenv()

class StorageConfig(BaseModel):
    artifact_dir: str = "./artifacts"
    max_file_size_mb: int = 10
    
    @property
    def max_file_size_bytes(self) -> int:
        return self.max_file_size_mb * 1024 * 1024

class KafkaConfig(BaseModel):
    bootstrap_servers: str = "localhost:9092"
    parser_input_topic: str = "parser-input"
    parser_output_topic: str = "parser-output"
    handoff_topic: str = "underwriting-topic"

class DeepSeekConfig(BaseModel):
    api_key: str
    model: str = "deepseek-chat"
    endpoint: str = "https://api.deepseek.com/v1/chat/completions"
    timeout_seconds: int = 60
    temperature: float = 0.0

class MongoDBConfig(BaseModel):
    uri: str
    database: str = "underwriting_assistant"
    artifacts_collection: str = "artifacts"
    idempotency_collection: str = "idempotency"
    files_collection: str = "raw_files"

class ParsingConfig(BaseModel):
    max_retries: int = 3
    retry_backoff_seconds: int = 2
    prompt_version: str = "v3"

class AppConfig(BaseModel):
    app_name: str = "underwriting-parser"
    environment: str = "development"
    storage: StorageConfig = Field(default_factory=StorageConfig)
    kafka: KafkaConfig = Field(default_factory=KafkaConfig)
    deepseek: DeepSeekConfig
    mongodb: MongoDBConfig
    parsing: ParsingConfig = Field(default_factory=ParsingConfig)

class ConfigLoader:
    _instance: Optional['ConfigLoader'] = None
    _config: Optional[AppConfig] = None
    
    def __new__(cls):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
        return cls._instance
    
    @classmethod
    def load(cls, config_path: str = "config.yaml") -> AppConfig:
        if cls._config is None:
            # Load YAML config
            config_path = os.getenv("CONFIG_PATH", config_path)
            raw_config = {}
            
            if os.path.exists(config_path):
                with open(config_path, 'r') as f:
                    raw_config = yaml.safe_load(f) or {}
            
            # Override with environment variables (from .env)
            raw_config = cls._merge_env_vars(raw_config)
            
            cls._config = AppConfig(**raw_config)
        
        return cls._config
    
    @classmethod
    def _merge_env_vars(cls, config: Dict[str, Any]) -> Dict[str, Any]:
        """Merge environment variables into config."""
        # DeepSeek
        if os.getenv("DEEPSEEK_API_KEY"):
            config.setdefault("deepseek", {})["api_key"] = os.getenv("DEEPSEEK_API_KEY")
        if os.getenv("DEEPSEEK_MODEL"):
            config.setdefault("deepseek", {})["model"] = os.getenv("DEEPSEEK_MODEL")
        if os.getenv("DEEPSEEK_ENDPOINT"):
            config.setdefault("deepseek", {})["endpoint"] = os.getenv("DEEPSEEK_ENDPOINT")
        
        # MongoDB
        if os.getenv("MONGODB_URI"):
            config.setdefault("mongodb", {})["uri"] = os.getenv("MONGODB_URI")
        if os.getenv("MONGODB_DATABASE"):
            config.setdefault("mongodb", {})["database"] = os.getenv("MONGODB_DATABASE")
        if os.getenv("MONGODB_ARTIFACTS_COLLECTION"):
            config.setdefault("mongodb", {})["artifacts_collection"] = os.getenv("MONGODB_ARTIFACTS_COLLECTION")
        if os.getenv("MONGODB_IDEMPOTENCY_COLLECTION"):
            config.setdefault("mongodb", {})["idempotency_collection"] = os.getenv("MONGODB_IDEMPOTENCY_COLLECTION")
        if os.getenv("MONGODB_FILES_COLLECTION"):
            config.setdefault("mongodb", {})["files_collection"] = os.getenv("MONGODB_FILES_COLLECTION")
        
        # Kafka
        if os.getenv("KAFKA_BOOTSTRAP_SERVERS"):
            config.setdefault("kafka", {})["bootstrap_servers"] = os.getenv("KAFKA_BOOTSTRAP_SERVERS")
        if os.getenv("KAFKA_HANDOFF_TOPIC"):
            config.setdefault("kafka", {})["handoff_topic"] = os.getenv("KAFKA_HANDOFF_TOPIC")
        
        # Prompt version
        if os.getenv("PROMPT_VERSION"):
            config.setdefault("parsing", {})["prompt_version"] = os.getenv("PROMPT_VERSION")
        
        # Max file size
        if os.getenv("MAX_FILE_SIZE_MB"):
            config.setdefault("storage", {})["max_file_size_mb"] = int(os.getenv("MAX_FILE_SIZE_MB"))
        
        return config

# Singleton instance
config = ConfigLoader.load()