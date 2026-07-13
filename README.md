# End-to-End Streaming RAG

Cloud-native, event-driven Streaming RAG for real-time log ingestion, vectorization, and intelligent incident analysis via an agentic API.

## Overview

This project ingests application logs via Kafka, embeds them into a vector database (ChromaDB), and exposes an agentic FastAPI backed by a LangGraph ReAct agent for root-cause analysis and system troubleshooting.

## Project status

| Phase | Status |
|-------|--------|
| 1 — Ingestion & Streaming | Complete |
| 2 — Vector Processing | Complete |
| 3 — Agentic RAG & API | Complete |
| 4 — Cloud-Native Deployment | Planned |

See [roadmap.md](roadmap.md) for the full development plan.

---

## Quick start

### 1. Infrastructure

```powershell
# Start Kafka
docker compose -f infrastructure/kafka/docker-compose.yml up -d
```

### 2. Python environment

```powershell
python -m venv .venv
.\.venv\Scripts\Activate.ps1   # PowerShell
pip install -r requirements.txt
```

### 3. Configuration

```powershell
Copy-Item .env.example .env
```

Open `.env` and set your values:

```env
LANGCHAIN_TRACING_V2=true
LANGCHAIN_API_KEY=<your_langsmith_key>   # optional — omit to disable tracing
LANGCHAIN_PROJECT=ai-project

OLLAMA_MODEL=llama3.2:3b   # must support tool/function calling
OLLAMA_KEEP_ALIVE=30m      # keep model in RAM between requests
OLLAMA_BASE_URL=http://localhost:11434
```

### 4. Ollama

Install Ollama from [ollama.com/download](https://ollama.com/download/windows), then pull the recommended model:

```powershell
ollama pull llama3.2:3b
```

`llama3.2:3b` uses ~2 GB of RAM and fully supports tool/function calling. For an even lighter option use `qwen2.5:3b` (also ~2 GB).

Ollama runs as a background service automatically after installation.

### 5. Seed the vector store

Run the producer and consumer to ingest logs into ChromaDB:

```powershell
# terminal 1
.venv\Scripts\python src\producer.py

# terminal 2
.venv\Scripts\python src\consumer.py
# let it run for ~30 seconds, then Ctrl+C both
```

### 6. Start the API

```powershell
.venv\Scripts\uvicorn api.main:app --reload --app-dir src
```

The API is now live at `http://localhost:8000`.

---

## Using the API

### Interactive docs

Open [http://localhost:8000/docs](http://localhost:8000/docs) for the Swagger UI.

### `POST /api/v1/query`

Send a natural-language question about your system logs:

```powershell
Invoke-RestMethod -Method POST http://localhost:8000/api/v1/query `
  -ContentType "application/json" `
  -Body '{"prompt": "what errors occurred recently?", "k": 5}'
```

**Request:**

```json
{
  "prompt": "which service is generating the most errors?",
  "k": 5
}
```

**Response:**

```json
{
  "answer": "The payment-service has generated the most errors...",
  "sources": [
    {
      "service_name": "payment-service",
      "log_level": "ERROR",
      "timestamp": "2026-07-13T13:05:22+00:00",
      "excerpt": "[ERROR] payment-service @ 2026-07-13T13:05:22\nDatabase connection timeout"
    }
  ]
}
```

### `GET /health`

```powershell
Invoke-RestMethod http://localhost:8000/health
# → { status: ok }
```

---

## Architecture

```
Kafka Producer → Kafka → Consumer → HuggingFace Embedder → ChromaDB
                                                                │
                                                    POST /api/v1/query
                                                                │
                                               LangGraph ReAct Agent
                                               ├── vector_search tool
                                               └── log_stats tool
                                                                │
                                                      Final answer + sources
```

The agent uses `create_react_agent` (LangGraph) with a local Ollama LLM. It reasons over two tools:

- **`vector_search`** — semantic similarity search against the live ChromaDB store
- **`log_stats`** — live count/distribution queries filtered by log level, service, and time window

---

## Documentation

| Doc | Description |
|-----|-------------|
| [docs/phase-1-ingestion.md](docs/phase-1-ingestion.md) | Kafka setup, producer, consumer |
| [docs/phase-2-vector-processing.md](docs/phase-2-vector-processing.md) | Embeddings, ChromaDB, chunking |
| [docs/phase-3-agentic-rag.md](docs/phase-3-agentic-rag.md) | Agent, tools, API reference |
| [docs/README.md](docs/README.md) | Full architecture and setup |
