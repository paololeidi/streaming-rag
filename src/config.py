import os
from pathlib import Path

from dotenv import load_dotenv

load_dotenv()

PROJECT_ROOT = Path(__file__).resolve().parent.parent

# --- Kafka ---
KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"
KAFKA_TOPIC_SYSTEM_LOGS = "system-logs"
KAFKA_CONSUMER_GROUP = "rag-ingestion-group"

# --- ChromaDB ---
CHROMA_PERSIST_DIR = str(PROJECT_ROOT / "data" / "chroma")
CHROMA_COLLECTION_NAME = "system-logs"

# --- Embeddings ---
EMBEDDING_MODEL_NAME = "all-MiniLM-L6-v2"

# --- Ollama / LLM ---
OLLAMA_MODEL = os.getenv("OLLAMA_MODEL", "llama3.2:3b")
OLLAMA_BASE_URL = os.getenv("OLLAMA_BASE_URL", "http://localhost:11434")
# How long Ollama keeps the model in RAM after the last request.
# "30m" prevents cold-start reloads. Set "0" to unload immediately.
OLLAMA_KEEP_ALIVE = os.getenv("OLLAMA_KEEP_ALIVE", "30m")

# --- LangSmith ---
# Tracing is activated automatically by LangChain when these env vars are set.
# Set LANGCHAIN_TRACING_V2=true and LANGCHAIN_API_KEY in your .env file.
LANGCHAIN_PROJECT = os.getenv("LANGCHAIN_PROJECT", "ai-project")
