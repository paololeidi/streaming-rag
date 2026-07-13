# Phase 3 — Agentic RAG & API

## Goal

Expose a reasoning LangGraph ReAct agent over a FastAPI HTTP interface. The agent answers natural-language questions about system health by dynamically choosing between two tools that query the live ChromaDB vector store built in Phase 2.

---

## Architecture

```
POST /api/v1/query
        │
        ▼
  FastAPI (src/api/main.py)
        │  QueryRequest { prompt, k }
        ▼
  LangGraph ReAct Agent (src/agent/graph.py)
        │
        ├──► vector_search(query, k)
        │         └── HuggingFaceEmbedder → ChromaDB.query_similar()
        │
        └──► log_stats(log_level, service_name, minutes)
                  └── ChromaDB.get() + metadata filtering
        │
        ▼ final AIMessage
  QueryResponse { answer, sources[] }
```

---

## New Files

| File | Purpose |
|---|---|
| `src/agent/__init__.py` | Package marker |
| `src/agent/tools.py` | `@tool vector_search` and `@tool log_stats` |
| `src/agent/prompts.py` | System prompt for the SRE analyst persona |
| `src/agent/graph.py` | `create_react_agent` wiring with ChatOllama |
| `src/api/__init__.py` | Package marker |
| `src/api/schemas.py` | `QueryRequest` / `QueryResponse` / `LogSource` models |
| `src/api/routes.py` | `POST /api/v1/query` endpoint |
| `src/api/main.py` | FastAPI app with lifespan model warm-up |
| `.env.example` | Template for environment variables |

---

## Configuration

Copy `.env.example` to `.env` and fill in your values:

```bash
LANGCHAIN_TRACING_V2=true
LANGCHAIN_API_KEY=<your_langsmith_key>
LANGCHAIN_PROJECT=ai-project

OLLAMA_MODEL=llama3.2:3b       # must support tool/function calling
OLLAMA_KEEP_ALIVE=30m          # keep model in RAM between requests
OLLAMA_BASE_URL=http://localhost:11434
```

> **Ollama model requirements**: The agent uses LangChain tool calling, which requires a model that supports the OpenAI-compatible function-call format. Confirmed working: `llama3.2:3b` (~2 GB RAM, recommended), `llama3.1` (4.7 GB RAM), `qwen2.5:3b` (~2 GB RAM).
>
> Set `OLLAMA_KEEP_ALIVE=0` to unload the model immediately after each request (saves RAM between uses at the cost of a reload on the next request).

---

## Running the API

```bash
# 1. Ensure Ollama is running and the model is pulled
ollama pull llama3.2:3b

# 2. Start the FastAPI server (from the repo root)
uvicorn api.main:app --reload --app-dir src

# 3. Open the interactive docs
http://localhost:8000/docs
```

---

## API Reference

### `POST /api/v1/query`

**Request body:**

```json
{
  "prompt": "What caused the payment service to crash in the last hour?",
  "k": 5
}
```

| Field | Type | Default | Description |
|---|---|---|---|
| `prompt` | `string` | required | Natural-language question |
| `k` | `int` | `5` | Max log entries to retrieve (1–20) |

**Response body:**

```json
{
  "answer": "The payment-service crashed due to a NullPointerException...",
  "sources": [
    {
      "service_name": "payment-service",
      "log_level": "ERROR",
      "timestamp": "2026-07-12T14:03:22+00:00",
      "excerpt": "[ERROR] payment-service @ 2026-07-12T14:03:22..."
    }
  ]
}
```

### `POST /api/v1/query/stream`

Streams the agent's answer token-by-token using **Server-Sent Events (SSE)**. The connection stays open while the agent reasons and calls tools; text tokens are pushed to the client as they are generated.

Each frame:
```
data: <token text>
```

Final frame when complete:
```
data: [DONE]
```

Same request body as `/query`. Use this endpoint in chat-style UIs to avoid blocking until the full answer is ready.

```bash
curl -X POST http://localhost:8000/api/v1/query/stream \
  -H "Content-Type: application/json" \
  -d '{"prompt": "are there any error logs?", "k": 5}'
```

### `GET /health`

Returns `{"status": "ok"}` — useful for container liveness probes.

---

## Tools

### `vector_search(query, k=5)`

Embeds `query` with the same `all-MiniLM-L6-v2` model used during ingestion, then calls `ChromaDB.query_similar()` to return the top-k semantically similar log entries.

**Best for:** root-cause analysis, error pattern matching, "find logs similar to X".

### `log_stats(log_level=None, service_name=None, minutes=60)`

Retrieves all matching log metadata from ChromaDB and applies a time-window filter using the stored `timestamp` field. Returns counts grouped by level and service.

**Best for:** trend questions, volume queries, "how many errors in the last N minutes?".

---

## LangSmith Observability

When `LANGCHAIN_TRACING_V2=true` and `LANGCHAIN_API_KEY` are set, every agent run is automatically traced to LangSmith. Each trace shows:

- Full ReAct reasoning loop (thought → tool call → observation → answer)
- Token usage per LLM call
- Tool inputs and outputs
- End-to-end latency

View traces at [https://smith.langchain.com](https://smith.langchain.com) under the configured project name.

---

## Design Decisions

- **`create_react_agent`** from `langgraph.prebuilt` was chosen over a custom `StateGraph` for simplicity. It implements a standard ReAct loop and is production-ready with full LangSmith tracing support.
- **Singleton graph** (`agent_graph` in `graph.py`) is instantiated once at import time and shared across all FastAPI requests, avoiding repeated model-loading overhead.
- **Embedding warm-up** in the FastAPI `lifespan` hook ensures the first real request does not pay the sentence-transformer model-loading cost.
- **Source extraction** is performed post-hoc by parsing `ToolMessage` content with a regex matching the `vector_search` output format, keeping the agent's output format clean and unstructured.
- **Model choice (`llama3.2:3b`)**: preferred over the 7B `llama3.1` on constrained hardware — ~2 GB RAM vs ~4.7 GB, ~3x faster token generation on CPU, with equivalent tool-calling support.
- **`keep_alive`**: set to `"30m"` on `ChatOllama` so Ollama keeps the model in RAM between requests. Without this, Ollama unloads the model after 5 minutes of inactivity, causing a ~5 minute cold-start on the next request.
- **Tool output truncation**: `vector_search` caps each retrieved document at 400 characters. This limits the token count fed to the LLM per tool call, which directly reduces generation latency on local hardware.
