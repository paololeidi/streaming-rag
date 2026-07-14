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
| 3.6 — MCP Integration | Planned |
| 4 — Cloud-Native Deployment | Planned |

See [roadmap.md](roadmap.md) for the full development plan.

---

## Quick start

> **Shell:** all commands below use **Git Bash** (forward-slash paths). If you are using PowerShell, replace `/` with `\` and `cp` with `Copy-Item`.

### 1. Infrastructure

```bash
# Start Kafka
docker compose -f infrastructure/kafka/docker-compose.yml up -d
```

### 2. Python environment

```bash
python -m venv .venv
source .venv/Scripts/activate
pip install -r requirements.txt
```

### 3. Configuration

```bash
cp .env.example .env
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

```bash
ollama pull llama3.2:3b
```

`llama3.2:3b` uses ~2 GB of RAM and fully supports tool/function calling. For an even lighter option use `qwen2.5:3b` (also ~2 GB).

Ollama runs as a background service automatically after installation.

### 5. Seed the vector store

Run the producer and consumer to ingest logs into ChromaDB:

```bash
# terminal 1
.venv/Scripts/python src/producer.py

# terminal 2
.venv/Scripts/python src/consumer.py
# let it run for ~30 seconds, then Ctrl+C both
```

### 6. Start the API

```bash
.venv/Scripts/uvicorn api.main:app --reload --app-dir src
```

The API is now live at `http://localhost:8000`.

---

## Using the API

### Interactive docs

Open [http://localhost:8000/docs](http://localhost:8000/docs) for the Swagger UI.

### `POST /api/v1/query`

Send a natural-language question about your system logs:

```bash
curl -s -X POST http://localhost:8000/api/v1/query \
  -H "Content-Type: application/json" \
  -d '{"prompt": "what errors occurred recently?", "k": 5}'
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

```bash
curl http://localhost:8000/health
# → {"status":"ok"}
```

---

## Architecture

```
                        ┌─────────────────────────────────────────┐
                        │           Data Pipeline                 │
                        │                                         │
  Kafka Producer ──► Kafka ──► Consumer ──► HuggingFace Embedder ──► ChromaDB
                                                                        │
                        └───────────────────────────────────────────────┘
                                                                        │
                   ┌────────────────────────────────────────────────────┘
                   │
                   ▼
        ┌─────────────────────────────────────────────────────┐
        │                  Agentic Layer                      │
        │                                                     │
        │   POST /api/v1/query  ──►  LangGraph ReAct Agent    │
        │                           ├── vector_search tool    │
        │                           └── log_stats tool        │
        │                                    │                │
        │              ┌─────────────────────┘                │
        │              │  MCP Client                          │
        │              └──► external MCP servers (optional)   │
        └──────────────────────────┬──────────────────────────┘
                                   │
                        ┌──────────▼──────────┐
                        │     MCP Server      │
                        │  (fastmcp)          │
                        │  ├── vector_search  │
                        │  └── log_stats      │
                        └──────────┬──────────┘
                                   │
               ┌───────────────────┼───────────────────┐
               ▼                   ▼                   ▼
        Claude Desktop       Other agents        Future clients
```

**Data pipeline:** Kafka streams live logs → embedded by HuggingFace (`all-MiniLM-L6-v2`) → stored in ChromaDB for semantic retrieval.

**Agentic layer:** A LangGraph ReAct agent backed by a local Ollama LLM reasons over two tools to answer natural-language questions about system health. As an MCP client it can also load tools from external MCP servers dynamically at startup.

**MCP server:** Exposes `vector_search` and `log_stats` via the Model Context Protocol, making the live log context available to any MCP-compatible client — Claude Desktop, other agents, or future integrations — without going through the FastAPI layer.

---

## Documentation

| Doc | Description |
|-----|-------------|
| [docs/phase-1-ingestion.md](docs/phase-1-ingestion.md) | Kafka setup, producer, consumer |
| [docs/phase-2-vector-processing.md](docs/phase-2-vector-processing.md) | Embeddings, ChromaDB, chunking |
| [docs/phase-3-agentic-rag.md](docs/phase-3-agentic-rag.md) | Agent, tools, API reference |

---

## Development approach

This project was built with [Cursor](https://cursor.com) as the primary IDE, using AI-assisted development throughout. The workflow treats the AI as a collaborative engineering tool: it accelerates implementation and surfaces alternatives, while architectural decisions, design trade-offs, and code review remain the developer's responsibility.

This includes: choosing when to use semantic search vs. metadata filtering, structuring the ReAct agent's tool selection logic, deciding on chunking strategy for streaming data, and making the judgment calls that determine whether generated code is actually correct — not just syntactically valid.
