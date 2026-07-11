from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent

KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"
KAFKA_TOPIC_SYSTEM_LOGS = "system-logs"
KAFKA_CONSUMER_GROUP = "rag-ingestion-group"

CHROMA_PERSIST_DIR = str(PROJECT_ROOT / "data" / "chroma")
CHROMA_COLLECTION_NAME = "system-logs"

EMBEDDING_MODEL_NAME = "all-MiniLM-L6-v2"
