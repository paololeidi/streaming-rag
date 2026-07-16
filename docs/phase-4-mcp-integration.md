# Phase 4 — MCP Integration

## Goal

Make the system interoperable: expose the live log tools through the Model Context Protocol so any MCP-compatible client (Cursor, Claude Desktop, other agents) can query the live log context directly, and extend the LangGraph agent so it can load tools from any running MCP server at startup without code changes.

---

## What is MCP?

The **Model Context Protocol** is an open standard (published by Anthropic, now broadly adopted) that decouples AI clients from tool providers using a client–server architecture. It is deliberately modelled after the Language Server Protocol (LSP) — the same idea that allows VS Code to support any language without hardcoding language-specific features.

Before MCP, every AI application reinvented its own plugin or tool-calling format. MCP is the common interface: a server declares its capabilities (tools, resources, prompts) once, and any conformant client can discover and use them without knowing anything about the server's internals.

### The two transport modes

**stdio** — the client spawns the server as a child process and communicates over stdin/stdout. Zero network configuration required. This is what Cursor and Claude Desktop use.

**SSE (Server-Sent Events)** — the server runs as an HTTP service. The client connects to a URL (`/sse`). Used for programmatic access when client and server are separate processes and cannot share stdio.

---



## Architecture

```
 ┌─────────────────────────────── Agentic Layer ──────────────────────────────┐
 │                                                                            │
 │  POST /api/v1/query                                                        │
 │         │                                                                  │
 │         ▼                                                                  │
 │  LangGraph ReAct Agent (src/agent/graph.py)                                │
 │         ├── vector_search ──────────────────────────────► ChromaDB         │
 │         ├── log_stats    ───────────────────────────────► ChromaDB         │
 │         └── [MCP tools]  ── MultiServerMCPClient (SSE) ───────────┐        │
 │                                                                   │        │
 └───────────────────────────────────────────────────────────────────┼────────┘
                                                                     │ SSE
                                                                     ▼
 ┌────────────────────── MCP Server (src/mcp_server/server.py) ───────────────┐
 │                                                                            │
 │  vector_search ──► HuggingFaceEmbedder ──► ChromaDB                        │
 │  log_stats     ──────────────────────────► ChromaDB                        │
 │                                                                            │
 └──────────────┬──────────────────────────────────────────────┬──────────────┘
                │ stdio                                        │ stdio
                ▼                                             ▼
          Cursor IDE                                    other MCP clients
```

---



## Architectural decisions and motivations



### Why `fastmcp` over the raw MCP SDK

The official `mcp` Python SDK is correct and complete, but verbose: you manually construct JSON schemas for every tool, handle the protocol handshake, and wire up transport machinery by hand. `fastmcp` is a high-level wrapper — the same relationship as FastAPI to raw ASGI. It reads type hints and docstrings to auto-generate schemas, and `mcp.run()` handles the rest. The result is tool definitions that look identical in structure to the LangChain `@tool` versions, which makes the codebase easier to follow.

### Why the tools are re-implemented rather than imported from `agent/tools.py`

`agent/tools.py` decorates functions with LangChain's `@tool`, which wraps them in a `StructuredTool` object — a LangChain-specific type that `fastmcp` cannot use. Reusing those wrapped objects would mean coupling the MCP layer to LangChain's internals.

The solution is to share the underlying data-access logic (`embeddings.py`, `vector_store.py`) and wrap it independently in each layer: `@tool` for LangChain, `@mcp.tool()` for fastmcp. There is no logic duplication — both files call the same `query_similar()` and `get_collection()` functions.

### Why stdio is the default transport

stdio is universally supported by every major MCP client (Cursor, Claude Desktop, Continue.dev) with zero network configuration: the client spawns the server process and communicates over stdin/stdout. SSE is offered via the `--sse` flag for the one case that requires it — when the LangGraph agent (a long-running process) needs to connect to the server over a URL.

### Why the module-level singleton in `graph.py` was replaced with an async factory

The old design created the compiled graph at import time:

```python
agent_graph = create_react_agent(...)  # runs at import
```

This is fine when the tool list is static. With `MultiServerMCPClient` it breaks: the client is an async context manager that must stay open for the duration of the app — its `__aexit__` tears down the MCP connection. The only place in FastAPI that manages async resource lifecycles correctly is the `lifespan` context manager. Moving graph construction into `build_agent_graph()` (an async factory) and calling it from `lifespan` gives the correct lifecycle: the MCP client opens at startup, the graph is built with its tools, and both are released cleanly at shutdown.

Storing the compiled graph in `app.state` is the idiomatic FastAPI pattern for startup-built shared resources. Routes access it via `request.app.state.agent_graph`.

### Why the embedding model is loaded lazily

The MCP server loads the `HuggingFaceEmbedder` (sentence-transformers `all-MiniLM-L6-v2`) on the first call to `vector_search` rather than at import time. The reason is the MCP handshake timing: Cursor spawns the server process and immediately attempts the protocol handshake. Loading the model at import time takes several seconds, during which the server cannot respond — Cursor sees silence, times out, and reports a connection failure.

Lazy initialisation via a `_get_embedder()` helper solves this cleanly: the server process starts in milliseconds, the handshake completes, and the model loads in the background the first time a tool is actually called. The one-time latency on the first `vector_search` invocation is acceptable; a broken connection is not.

### Why MCP tools are additive, not replacing native tools

`vector_search` and `log_stats` remain as native LangChain tools in the agent regardless of whether `MCP_SERVER_URL` is set. This means the FastAPI backend works fully without any MCP server running — it degrades gracefully. MCP tools extend the capability set at configuration time; they are useful when pointing the agent at an external MCP server that exposes capabilities the native tools do not have.

---



## New and modified files


| Action | File                         | Change                                                               |
| ------ | ---------------------------- | -------------------------------------------------------------------- |
| Create | `src/mcp_server/__init__.py` | Package marker                                                       |
| Create | `src/mcp_server/server.py`   | fastmcp server exposing both tools                                   |
| Modify | `requirements.txt`           | Added `fastmcp==2.14.7`, `langchain-mcp-adapters==0.2.2`             |
| Modify | `src/config.py`              | Added `MCP_SERVER_URL`, `MCP_SERVER_PORT`                            |
| Modify | `.env.example`               | Documented MCP env vars                                              |
| Modify | `src/agent/graph.py`         | Singleton → `async build_agent_graph(extra_tools)` factory           |
| Modify | `src/api/main.py`            | Lifespan builds graph + optional MCP client; version bumped to 0.4.0 |
| Modify | `src/api/routes.py`          | Routes use `request.app.state.agent_graph`                           |


---



## Running the MCP server



### stdio mode (for Cursor and other desktop clients)

```bash
.venv/Scripts/python src/mcp_server/server.py
```

The server speaks MCP over stdin/stdout. You do not run this manually — Cursor spawns it automatically based on the config below.

### SSE mode (for the LangGraph agent MCP client)

```bash
.venv/Scripts/python src/mcp_server/server.py --sse
# Listening on http://0.0.0.0:8001/sse
```

The port is controlled by `MCP_SERVER_PORT` in `.env` (default `8001`).

---



## Tool reference



### `vector_search`

Semantic similarity search over the ChromaDB log store.


| Parameter | Type  | Default | Description                                        |
| --------- | ----- | ------- | -------------------------------------------------- |
| `query`   | `str` | —       | Natural-language description of what to search for |
| `k`       | `int` | `5`     | Maximum number of results to return                |


**Returns:** formatted list of matching log entries, each showing log level, service name, timestamp, and a 400-character excerpt.

**Example input:**

```json
{ "query": "database connection timeout", "k": 3 }
```

**Example output:**

```
[1] ERROR | payment-service @ 2026-07-15T14:32:11+00:00
[ERROR] payment-service — Database connection timeout after 30s

[2] ERROR | order-service @ 2026-07-15T14:31:58+00:00
[ERROR] order-service — Failed to acquire DB connection from pool
```

---



### `log_stats`

Count and distribution query over ChromaDB metadata, filtered by time window.


| Parameter      | Type  | Default | Description                          |
| -------------- | ----- | ------- | ------------------------------------ |
| `log_level`    | `str  | None`   | `None`                               |
| `service_name` | `str  | None`   | `None`                               |
| `minutes`      | `int` | `60`    | Look-back window in minutes from now |


**Returns:** total entry count, breakdown by log level, breakdown by service.

**Example input:**

```json
{ "log_level": "ERROR", "minutes": 30 }
```

**Example output:**

```
Log statistics for the last 30 minute(s):
  Total entries: 47
  By level: ERROR: 47
  By service:
    payment-service: 23
    order-service: 14
    auth-service: 10
```

---



## Cursor validation



### 1. Configure Cursor

Open **Cursor Settings → MCP** and add the following entry (or edit `mcp.json` directly):

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

> **Windows note:** both `command` and the script path in `args` must be **absolute paths**. Using a relative path for the script causes Cursor to resolve it against its own working directory (your user home) rather than the project root, resulting in a "file not found" error. The `cwd` field is still required so the server can resolve its sibling `src/` imports correctly.

Cursor will spawn the server on demand (stdio transport). ChromaDB must have data in it — run the producer and consumer first if you haven't already.

### 2. Validation checklist

In the Cursor agent panel, confirm the following:

- [x] Both `vector_search` and `log_stats` appear in the tool list with their descriptions and parameter schemas.
- [x] "How many errors were logged in the last hour?" → agent calls `log_stats`, returns a live count broken down by service.
- [x] "Find logs related to database connection failures" → agent calls `vector_search`, returns semantically matched entries with service names and timestamps.

---



## MCP client configuration (LangGraph agent)

To let the LangGraph agent load tools from the MCP server at startup, start the server in SSE mode first, then set `MCP_SERVER_URL` in `.env`:

```bash
# Terminal 1 — MCP server in SSE mode
.venv/Scripts/python src/mcp_server/server.py --sse

# Terminal 2 — FastAPI backend (reads MCP_SERVER_URL from .env)
.venv/Scripts/uvicorn api.main:app --reload --app-dir src
```

`.env`:

```env
MCP_SERVER_URL=http://localhost:8001/sse
```

At startup the lifespan hook will print:

```
Connecting to MCP server at http://localhost:8001/sse ...
Loaded 2 tool(s) from MCP server.
Agent ready (native + MCP tools).
```

Leave `MCP_SERVER_URL` empty (the default) to run the agent with native tools only — the FastAPI backend is fully functional without any MCP server running.