# End-to-End Streaming RAG

Cloud-native, event-driven Streaming RAG for real-time log ingestion, vectorization, and intelligent incident analysis — exposed via an agentic API and an MCP server for interoperability with any AI client.

## Overview

Diagnosing incidents in distributed systems means manually trawling through thousands of log lines across multiple services, correlating timestamps, and pattern-matching errors under pressure. This project replaces that workflow with a natural-language interface: you ask *"what caused the payment service to crash in the last hour?"* and an AI agent reasons over your live log data to give a grounded, source-cited answer.

The system is built end-to-end: logs flow from a Kafka stream into a vector database in real time, and a LangGraph ReAct agent decides at query time whether to search semantically or query aggregate statistics — then synthesizes a response from real context, not from model memory. The agent is also exposed as an MCP server, making its capabilities available to any MCP-compatible client (Cursor, Claude Desktop, other agents) as a first-class interoperable service.

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



## Quick start

The fastest way to spin up the complete end-to-end stack (Kafka KRaft, ChromaDB, Ollama with `llama3.2:3b`, Producer, Consumer, FastAPI app, and MCP server) is with Docker Compose:

```bash
# 1. Prepare environment configuration (optional)
cp .env.example .env

# 2. Start the full stack
docker compose up --build
```

When initialization completes:

| Service | Endpoint | Description |
|---|---|---|
| **API & Swagger UI** | [http://localhost:8000/docs](http://localhost:8000/docs) | Interactive OpenAPI documentation & query interface |
| **Health Check** | [http://localhost:8000/health](http://localhost:8000/health) | API liveness & readiness endpoint |
| **MCP Server (SSE)** | [http://localhost:8001/sse](http://localhost:8001/sse) | Model Context Protocol SSE transport for AI clients |

Stop the stack at any time with `Ctrl+C`, or run detached: `docker compose up --build -d`.

---

## 📖 Step-by-Step Guide: Build, Run & Test

For full instructions on building the system, running individual components (producer, consumer, API, MCP server), and testing tools, the agent, and endpoints:

👉 **[docs/run-and-test-guide.md](docs/run-and-test-guide.md)**

### What the guide covers:
- **Building the Project:**
  - Local Python virtual environment setup (`requirements.txt` and `requirements-dev.txt`).
  - Container image builds using multi-stage `Dockerfile`.
  - Local Kubernetes deployments on Kind via Helm (`charts/streaming-rag/`).
- **Running Components Individually:**
  - Starting local Kafka KRaft and Ollama (`llama3.2:3b`).
  - Running the mock log producer (`src/producer.py`).
  - Running the real-time ingestion consumer and vector pipeline (`src/consumer.py`).
  - Launching the FastAPI server (`src/api/main.py`).
  - Launching the MCP server in SSE or stdio mode (`src/mcp_server/server.py`).
- **Testing the API, Agent & Tools:**
  - Testing REST endpoints (`POST /api/v1/query`) and real-time token streaming (`POST /api/v1/query/stream` via SSE).
  - Invoking the LangGraph ReAct agent programmatically in Python.
  - Testing the `vector_search` and `log_stats` tools directly and via MCP clients.
  - Executing automated tests with strict test coverage enforcement (`pytest --cov-fail-under=90`).
  - Running code quality gates (`ruff check`, `ruff format`, `mypy`).

---

## Documentation

| Doc | Description |
|---|---|
| [docs/technical-guide.md](docs/technical-guide.md) | **Comprehensive system architecture & technical implementation guide** |
| [docs/run-and-test-guide.md](docs/run-and-test-guide.md) | **Step-by-step build, run, and testing playbook** |
| [docs/data-contract.md](docs/data-contract.md) | Pydantic log schema & serialization data contract |
| [docs/improvements.md](docs/improvements.md) | Production improvements & enterprise backlog |
| [roadmap.md](roadmap.md) | Development roadmap & completed phase milestones |
| [charts/streaming-rag/README.md](charts/streaming-rag/README.md) | Helm values, Kind build/load/install |


---



## Development approach

This project was developed through pair programming and active collaboration with AI coding assistants. The workflow treats AI as a collaborative engineering partner: it accelerates implementation and surfaces alternatives, while architectural decisions, design trade-offs, and code review remain the developer's responsibility.

This includes: choosing when to use semantic search vs. metadata filtering, structuring the ReAct agent's tool selection logic, deciding on chunking strategy for streaming data, and making the judgment calls that determine whether generated code is actually correct — not just syntactically valid.