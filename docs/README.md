# Project Documentation

Technical documentation for the Streaming RAG learning project.

## Contents

| Document | Description |
|----------|-------------|
| [Phase 1: Ingestion & Streaming](./phase-1-ingestion.md) | Kafka setup, producer/consumer design, offset strategy, and how to run the stack |
| [Phase 2: Vector Processing](./phase-2-vector-processing.md) | ChromaDB setup, chunking, embeddings, and async ingestion pipeline |
| [Data Contract](./data-contract.md) | `SystemLog` schema, validation rules, and serialization format |

## Quick start

```bash
# 1. Start Kafka (from project root)
docker compose -f infrastructure/kafka/docker-compose.yml up -d

# 2. One-time setup: create a virtual environment and install dependencies (from project root)
python -m venv .venv
pip install -r requirements.txt
```

### Run producer and consumer

Open **separate terminals** for the producer and consumer. Activate the venv in each one before running a script — each new terminal starts with system Python and does not have the project dependencies.

The consumer embeds each log and writes to ChromaDB (`data/chroma/`). The first run downloads the Hugging Face model (~80MB).

```bash
# Terminal 2 — Producer (from project root)
source .venv/Scripts/activate   # Git Bash; required in every terminal
cd src
python producer.py

# Terminal 3 — Consumer (from project root)
source .venv/Scripts/activate   # Git Bash; required in every terminal
cd src
python consumer.py
```

Your prompt should show `(.venv)` when the environment is active.

**PowerShell equivalent:**

```powershell
.\.venv\Scripts\Activate.ps1
cd src
python producer.py   # or consumer.py
```

## Source layout

```
src/
  config.py          # Shared Kafka, Chroma, and embedding settings
  model/
    models.py        # Pydantic data contract (SystemLog)
  chunking.py        # Log-to-chunk transformation
  embeddings.py      # Hugging Face embedder
  vector_store.py    # ChromaDB wrapper
  producer.py        # Mock log generator
  consumer.py        # Async ingestion pipeline with manual offset commits

data/
  chroma/            # Persistent vector store (gitignored)

infrastructure/kafka/
  docker-compose.yml   # Local KRaft broker

docs/             # This folder
requirements.txt  # Pinned Python dependencies
```
