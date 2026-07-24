# End-to-End Streaming RAG

Cloud-native, event-driven Streaming RAG for real-time log ingestion, vectorization, and intelligent incident analysis — exposed via an agentic API and an MCP server for interoperability with any AI client.

## Overview

Diagnosing incidents in distributed systems means manually trawling through thousands of log lines across multiple services, correlating timestamps, and pattern-matching errors under pressure. This project replaces that workflow with a natural-language interface: you ask *"what caused the payment service to crash in the last hour?"* and an AI agent reasons over your live log data to give a grounded, source-cited answer.

The system is built end-to-end: logs flow from a Kafka stream into a vector database in real time, and a LangGraph ReAct agent decides at query time whether to search semantically or query aggregate statistics — then synthesizes a response from real context, not from model memory. The agent is also exposed as an MCP server, making its capabilities available to any MCP-compatible client (Cursor, Claude Desktop, other agents) as a first-class interoperable service.

## Project status


| Phase                       | Status                                    |
| --------------------------- | ----------------------------------------- |
| 1 — Ingestion & Streaming   | Complete                                  |
| 2 — Vector Processing       | Complete                                  |
| 3 — Agentic RAG & API       | Complete                                  |
| 4 — MCP Integration         | Complete                                  |
| 5 — Cloud-Native Deployment | In progress (Compose done; K8s/Helm next) |


See [roadmap.md](roadmap.md) for the full development plan.

---



## Architecture

```
 ┌──────────────────────── Data Pipeline ─────────────────────────┐
 │                                                                │
 │  Producer ──► Kafka ──► Consumer ──► HF Embedder ──► ChromaDB  │
 │                                                                │
 └────────────────────────────────────────────┬───────────────────┘
                                              │
 ┌──────────────────────── Agentic Layer ─────▼────────────────────┐
 │                                                                 │
 │  POST /api/v1/query                                             │
 │         │                                                       │
 │         ▼                                                       │
 │  LangGraph ReAct Agent  (Ollama · llama3.2:3b)                  │
 │         ├── vector_search ──► ChromaDB  (native)                │
 │         ├── log_stats    ──► ChromaDB  (native)                 │
 │         └── [MCP tools]  ──► MCP Server  (optional, via SSE)    │
 │                                                                 │
 └─────────────────────────────┬───────────────────────────────────┘
                               │
            ┌──────────────────▼──────────────────┐
            │             MCP Server              │
            │  (src/mcp_server/server.py)         │
            │  ├── vector_search                  │
            │  └── log_stats                      │
            └──────────────────┬──────────────────┘
                               │  stdio / SSE
        ┌──────────────────────┼──────────────────────┐
        ▼                      ▼                      ▼
   Cursor IDE            Claude Desktop          Other agents
```

**Data pipeline:** application logs are produced to Kafka, consumed in real time, embedded with HuggingFace `all-MiniLM-L6-v2`, and stored in ChromaDB for semantic retrieval.

**Agentic layer:** a LangGraph ReAct agent backed by a local Ollama LLM answers natural-language questions by dynamically choosing between semantic search and statistical queries over the live log store. As an MCP client it can also load tools from external MCP servers at startup without code changes.

**MCP server:** wraps `vector_search` and `log_stats` in the Model Context Protocol, turning the live log context into an interoperable service that any MCP-compatible client (Cursor, Claude Desktop, other agents) can discover and call — without going through the FastAPI layer. The agent also acts as an MCP client, able to load tools from any external MCP server at startup via `MCP_SERVER_URL`.

---



## Requirements

**Compose quick start** (recommended):


| Tool                                                              | Purpose                         | Notes                                                                    |
| ----------------------------------------------------------------- | ------------------------------- | ------------------------------------------------------------------------ |
| [Docker Desktop](https://www.docker.com/products/docker-desktop/) | Runs the full stack via Compose | Allocate **~8 GB+ RAM** to Docker (Ollama + embeddings + Kafka + Chroma) |


**Local (non-Docker) development** additionally needs:


| Tool                                              | Purpose                               |
| ------------------------------------------------- | ------------------------------------- |
| [Python 3.11+](https://www.python.org/downloads/) | Producer, consumer, API, MCP          |
| [Ollama](https://ollama.com/download)             | Local LLM (Compose runs this for you) |


Optional: **LangSmith API key** — omit `LANGCHAIN_API_KEY` in `.env` to disable tracing.

---



## Quick start (Docker Compose)

Bring up Kafka, Chroma, Ollama, producer, consumer, API, and MCP with one command. No local Python venv, host Ollama install, or manual seeding required.

> **Working directory:** project root (`AI-project/`).



### 1. Configuration (optional)

```bash
cp .env.example .env
```

Edit `.env` only if you want LangSmith tracing or a different `OLLAMA_MODEL`. Compose wires Kafka, Chroma, and Ollama URLs for you.

### 2. Start the stack

```bash
docker compose up --build
```

First run builds the app image (includes the embedding model) and pulls `llama3.2:3b` into the Ollama volume — that can take several minutes. Later starts reuse cached images and the `ollama_data` volume.

When ready:


| Service       | URL                                                          |
| ------------- | ------------------------------------------------------------ |
| API + Swagger | [http://localhost:8000/docs](http://localhost:8000/docs)     |
| Health        | [http://localhost:8000/health](http://localhost:8000/health) |
| MCP (SSE)     | [http://localhost:8001/sse](http://localhost:8001/sse)       |


Stop with `Ctrl+C`, or run detached: `docker compose up --build -d`.

---



## Using the API



### Interactive docs

Open [http://localhost:8000/docs](http://localhost:8000/docs) for the Swagger UI.

### `POST /api/v1/query`

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



## MCP Integration



### Compose stack (SSE)

With `docker compose up`, the MCP server listens on SSE. In **Cursor Settings → MCP**:

```json
{
  "mcpServers": {
    "log-analysis": {
      "url": "http://localhost:8001/sse"
    }
  }
}
```

Validate in the Cursor agent panel:

1. **"What tools do you have available?"** — confirm `vector_search` and `log_stats`.
2. **"How many errors were logged in the last hour?"** — calls `log_stats`.
3. **"Find logs related to database connection failures"** — calls `vector_search`.



### Local stdio (non-Docker)

If you run the MCP server on the host instead of Compose, use absolute paths:

```json
{
  "mcpServers": {
    "log-analysis": {
      "command": "C:\\Users\\Utente\\Documents\\Projects\\AI-project\\.venv\\Scripts\\python.exe",
      "args": ["C:\\Users\\Utente\\Documents\\Projects\\AI-project\\src\\mcp_server\\server.py"],
      "cwd": "C:\\Users\\Utente\\Documents\\Projects\\AI-project"
    }
  }
}
```

---



## Local (non-Docker) development

Use this path when iterating on Python code without rebuilding images. Commands below use **Git Bash** (forward-slash paths).

### 1. Kafka only

```bash
docker compose -f infrastructure/kafka/docker-compose.yml up -d
```

For Chroma in server mode locally, also run a Chroma container (or leave `CHROMA_HOST` unset to use embedded `data/chroma/`).

### 2. Python environment

```bash
python -m venv .venv
source .venv/Scripts/activate
pip install -r requirements.txt
cp .env.example .env
```



### 3. Ollama on the host

```bash
ollama pull llama3.2:3b
```



### 4. Producer + consumer

```bash
# terminal 1
.venv/Scripts/python src/producer.py

# terminal 2
.venv/Scripts/python src/consumer.py
```



### 5. API

```bash
.venv/Scripts/uvicorn api.main:app --reload --app-dir src
```

---



## Documentation


| Doc                                                                    | Description                                   |
| ---------------------------------------------------------------------- | --------------------------------------------- |
| [docs/phase-1-ingestion.md](docs/phase-1-ingestion.md)                 | Kafka setup, producer, consumer               |
| [docs/phase-2-vector-processing.md](docs/phase-2-vector-processing.md) | Embeddings, vector database, chunking         |
| [docs/phase-3-agentic-rag.md](docs/phase-3-agentic-rag.md)             | Agentic RAG, tools, API reference             |
| [docs/phase-4-mcp-integration.md](docs/phase-4-mcp-integration.md)     | MCP server, MCP client, Cursor setup          |
| [docs/phase-5-cloud-native.md](docs/phase-5-cloud-native.md)           | Docker image, Compose stack, Chroma dual mode |


---



## Development approach

This project was built with [Cursor](https://cursor.com) as the primary IDE, using AI-assisted development throughout. The workflow treats the AI as a collaborative engineering tool: it accelerates implementation and surfaces alternatives, while architectural decisions, design trade-offs, and code review remain the developer's responsibility.

This includes: choosing when to use semantic search vs. metadata filtering, structuring the ReAct agent's tool selection logic, deciding on chunking strategy for streaming data, and making the judgment calls that determine whether generated code is actually correct — not just syntactically valid.