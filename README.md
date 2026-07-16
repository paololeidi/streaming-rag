# End-to-End Streaming RAG

Cloud-native, event-driven Streaming RAG for real-time log ingestion, vectorization, and intelligent incident analysis — exposed via an agentic API and an MCP server for interoperability with any AI client.

## Overview

Diagnosing incidents in distributed systems means manually trawling through thousands of log lines across multiple services, correlating timestamps, and pattern-matching errors under pressure. This project replaces that workflow with a natural-language interface: you ask *"what caused the payment service to crash in the last hour?"* and an AI agent reasons over your live log data to give a grounded, source-cited answer.

The system is built end-to-end: logs flow from a Kafka stream into a vector database in real time, and a LangGraph ReAct agent decides at query time whether to search semantically or query aggregate statistics — then synthesizes a response from real context, not from model memory. The agent is also exposed as an MCP server, making its capabilities available to any MCP-compatible client (Cursor, Claude Desktop, other agents) as a first-class interoperable service.

## Project status


| Phase                       | Status   |
| --------------------------- | -------- |
| 1 — Ingestion & Streaming   | Complete |
| 2 — Vector Processing       | Complete |
| 3 — Agentic RAG & API       | Complete |
| 4 — MCP Integration         | Complete |
| 5 — Cloud-Native Deployment | Planned  |


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
                               │  stdio
        ┌──────────────────────┼──────────────────────┐
        ▼                      ▼                      ▼
   Cursor IDE            Claude Desktop          Other agents
```

**Data pipeline:** application logs are produced to Kafka, consumed in real time, embedded with HuggingFace `all-MiniLM-L6-v2`, and stored in ChromaDB for semantic retrieval.

**Agentic layer:** a LangGraph ReAct agent backed by a local Ollama LLM answers natural-language questions by dynamically choosing between semantic search and statistical queries over the live log store. As an MCP client it can also load tools from external MCP servers at startup without code changes.

**MCP server:** wraps `vector_search` and `log_stats` in the Model Context Protocol, turning the live log context into an interoperable service that any MCP-compatible client (Cursor, Claude Desktop, other agents) can discover and call — without going through the FastAPI layer. The agent also acts as an MCP client, able to load tools from any external MCP server at startup via `MCP_SERVER_URL`.

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



## MCP Integration



### 7. Connect the MCP server to Cursor

Open **Cursor Settings → MCP** and add the following server entry:

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

> Both `command` and the script path in `args` must be absolute paths. On Windows, using a relative script path causes Cursor to resolve it against its own working directory rather than the project root.

Cursor spawns the MCP server automatically (stdio transport). You should see `log-analysis` appear as a connected server with a green indicator.

### 8. Validate the MCP integration

In the Cursor agent panel, run these three checks:

1. **"What tools do you have available?"** — confirm `vector_search` and `log_stats` are listed.
2. **"How many errors were logged in the last hour?"** — agent calls `log_stats`, returns a live count broken down by service.
3. **"Find logs related to database connection failures"** — agent calls `vector_search`, returns semantically matched log entries with service names and timestamps.

---



## Documentation


| Doc                                                                    | Description                          |
| ---------------------------------------------------------------------- | ------------------------------------ |
| [docs/phase-1-ingestion.md](docs/phase-1-ingestion.md)                 | Kafka setup, producer, consumer      |
| [docs/phase-2-vector-processing.md](docs/phase-2-vector-processing.md) | Embeddings, ChromaDB, chunking       |
| [docs/phase-3-agentic-rag.md](docs/phase-3-agentic-rag.md)             | Agent, tools, API reference          |
| [docs/phase-4-mcp-integration.md](docs/phase-4-mcp-integration.md)     | MCP server, MCP client, Cursor setup |


---



## Development approach

This project was built with [Cursor](https://cursor.com) as the primary IDE, using AI-assisted development throughout. The workflow treats the AI as a collaborative engineering tool: it accelerates implementation and surfaces alternatives, while architectural decisions, design trade-offs, and code review remain the developer's responsibility.

This includes: choosing when to use semantic search vs. metadata filtering, structuring the ReAct agent's tool selection logic, deciding on chunking strategy for streaming data, and making the judgment calls that determine whether generated code is actually correct — not just syntactically valid.