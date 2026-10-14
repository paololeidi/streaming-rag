# End-to-End Streaming RAG

> ☕ **Looking for the Java version?** Check out the Java implementation [here](https://github.com/paololeidi/streaming-rag-java).

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



## Quick start (Recommended: Agent-Assisted)

The easiest and fastest way to set up, launch, and verify the project is to let your **AI Coding Assistant / Agent** (Cursor, Claude Code, Copilot, Antigravity, Aider, etc.) run it autonomously using the **[AGENT_GUIDE.md](AGENT_GUIDE.md)** runbook.

Simply prompt your assistant:
> *"Start the application following the agent guide"*

The agent will ask you whether you prefer **Path A (Full Docker Compose - zero local setup)** or **Path B (Local Python hybrid development)**, and will then autonomously verify prerequisites, launch services, seed initial logs, execute health checks and test queries, and deliver working endpoints.

### Prerequisites
Make sure the following tools are available on your system before startup:
- **Docker & Docker Compose** (running daemon, required for containers / Kafka KRaft broker).
- **Ollama** running with the model pulled: `ollama pull llama3.2:3b` (required for local LLM inference in hybrid mode).
- **Python 3.11+ / 3.12** & virtual environment (required only if choosing local hybrid development).

---

## 🛠️ Manual Setup Guide

If you prefer to configure, build, and run the stack manually step-by-step (Docker Compose, individual Python services, Kafka, Kubernetes/Helm on Kind, or automated test suites):

👉 **[docs/run-and-test-guide.md](docs/run-and-test-guide.md)**

---

## Documentation

| Doc | Description |
|---|---|
| [docs/technical-guide.md](docs/technical-guide.md) | **Comprehensive system architecture & technical implementation guide** |
| [docs/run-and-test-guide.md](docs/run-and-test-guide.md) | **Step-by-step build, run, and testing playbook** |
| [AGENT_GUIDE.md](AGENT_GUIDE.md) | **AI Agent & Coding Assistant Runbook (Automated onboarding & testing)** |
| [docs/data-contract.md](docs/data-contract.md) | Pydantic log schema & serialization data contract |
| [docs/improvements.md](docs/improvements.md) | Production improvements & enterprise backlog |
| [charts/streaming-rag/README.md](charts/streaming-rag/README.md) | Helm values, Kind build/load/install |


---



## Development approach

This project was developed through pair programming and active collaboration with AI coding assistants. The workflow treats AI as a collaborative engineering partner: it accelerates implementation and surfaces alternatives, while architectural decisions, design trade-offs, and code review remain the developer's responsibility.

This includes: choosing when to use semantic search vs. metadata filtering, structuring the ReAct agent's tool selection logic, deciding on chunking strategy for streaming data, and making the judgment calls that determine whether generated code is actually correct — not just syntactically valid.