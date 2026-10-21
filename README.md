# End-to-End Streaming RAG

[![Python](https://img.shields.io/badge/Python-3.11%2B-blue.svg)](https://www.python.org/)
[![FastAPI](https://img.shields.io/badge/FastAPI-0.115-teal.svg)](https://fastapi.tiangolo.com/)
[![LangGraph](https://img.shields.io/badge/LangGraph-ReAct-purple.svg)](https://github.com/langchain-ai/langgraph)
[![ChromaDB](https://img.shields.io/badge/ChromaDB-0.6.3-orange.svg)](https://www.trychroma.com/)
[![Kafka](https://img.shields.io/badge/Apache%20Kafka-KRaft-red.svg)](https://kafka.apache.org/)
[![MCP](https://img.shields.io/badge/MCP-FastMCP-purple.svg)](https://modelcontextprotocol.io/)

> ☕ **Looking for the Java version?** Check out the Java implementation [here](https://github.com/paololeidi/streaming-rag-java).

Cloud-native, event-driven Streaming RAG for real-time log ingestion, vectorization, and intelligent incident analysis — exposed via an agentic API and an MCP server for interoperability with any AI client.

## Overview

Diagnosing incidents in distributed systems means manually trawling through thousands of log lines across multiple services, correlating timestamps, and pattern-matching errors under pressure. This project replaces that workflow with a natural-language interface: you ask *"what caused the payment service to crash in the last hour?"* and an AI agent reasons over your live log data to give a grounded, source-cited answer with event IDs and similarity confidence scores.

The system is built end-to-end: multi-archetype synthetic logs flow from a Kafka stream (with Dead Letter Queue resilience on `system-logs-dlq`) into a vector database in real time, and a LangGraph ReAct agent decides at query time whether to search semantically or query aggregate statistics — then synthesizes a response from real context, not from model memory. The agent is also exposed as an MCP server, making its capabilities available to any MCP-compatible client (Cursor, Claude Desktop, other agents) as a first-class interoperable service.

---




## Architecture

```
 ┌──────────────────────── Data Pipeline ─────────────────────────┐
 │                                                                │
 │  Producer ──► Kafka ──► Consumer ──► HF Embedder ──► ChromaDB  │
 │  (burst/stream)         │ (retries + DLQ: system-logs-dlq)     │
 └────────────────────────────────────────────┬───────────────────┘
                                              │
 ┌──────────────────────── Agentic Layer ─────▼────────────────────┐
 │                                                                 │
 │  POST /api/v1/query                                             │
 │         │                                                       │
 │         ▼                                                       │
 │  LangGraph ReAct Agent  (Ollama · llama3.2:3b)                  │
 │         ├── vector_search ──► ChromaDB  (native + score/id)     │
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

**Data pipeline:** realistic microservice logs (`payment-gateway`, `user-auth`, `inventory-service`, `frontend-bff`) are produced to Kafka via continuous stream or programmatic burst (`--burst <count>`), consumed in real time with exponential backoff retries and Dead Letter Queue (`system-logs-dlq`) protection, embedded with HuggingFace `all-MiniLM-L6-v2`, and stored in ChromaDB for semantic retrieval.

**Agentic layer:** a LangGraph ReAct agent backed by a local Ollama LLM answers natural-language questions by dynamically choosing between semantic search and statistical queries over the live log store, returning cited `LogSource` records enriched with `event_id` and similarity `score`. As an MCP client it can also load tools from external MCP servers at startup without code changes.

**MCP server:** wraps `vector_search` and `log_stats` in the Model Context Protocol, turning the live log context into an interoperable service that any MCP-compatible client (Cursor, Claude Desktop, other agents) can discover and call — without going through the FastAPI layer. The agent also acts as an MCP client, able to load tools from any external MCP server at startup via `MCP_SERVER_URL`.

---



## Quick start (Recommended: Agent-Assisted)

The easiest and fastest way to set up, launch, and verify the project is to let your **AI Coding Assistant / Agent** (Cursor, Claude Code, Copilot, Antigravity, Aider, etc.) run it autonomously using the **[AGENT_GUIDE.md](AGENT_GUIDE.md)** runbook.

Simply prompt your assistant:
> *"Start the application following the agent guide"*

The agent will ask you whether you prefer **Path A (Full Docker Compose - zero local setup)** or **Path B (Local Python hybrid development)**, and will then autonomously verify prerequisites, launch services, seed initial logs, execute health checks and test queries, and deliver working endpoints.

### Prerequisites

What you need depends on the path you choose:

| Requirement | Path A: Full Docker Compose | Path B: Local Python hybrid |
|---|:---:|:---:|
| **Docker + Docker Compose** (daemon running) | ✅ Required | ✅ Required (Kafka + Chroma only) |
| **Ollama** with llama3.2:3b pulled | ➖ Not needed (runs in a container; model pulled by ollama-init) | ✅ Required (ollama pull llama3.2:3b) |
| **Python 3.11+ / 3.12** | ➖ Not needed (built inside the container) | ✅ Required |
| **Virtual Environment & Dependencies** | ➖ Not needed | ✅ Required (pip install -r requirements.txt) |

**Path A therefore needs only Docker with Compose**, plus **enough memory for the container engine:** the model, Kafka, Chroma, and Python services run together, so allocate more than the 2 GB default some Docker/Podman VMs use (4 GB+ recommended, ~8 GB for Kind).

> **Podman users:** Podman works too, but podman compose requires a Compose provider (docker-compose or podman-compose) to be installed.

---

## ⚖️ Dual-Implementation Architectural Comparison

| Dimension | Python Implementation (`streaming-rag`) | Java Implementation ([`streaming-rag-java`](https://github.com/paololeidi/streaming-rag-java)) |
|---|---|---|
| **Runtime & Language** | Python 3.11+ / FastAPI / Uvicorn | Java 21 LTS / Spring Boot 3.3.4 (Virtual Threads) |
| **Concurrency Model** | `asyncio` Single-threaded Event Loop | Lightweight Virtual Threads (Project Loom) |
| **Kafka Integration** | `confluent-kafka` (KRaft, Manual Commits, Retries + DLQ `system-logs-dlq`) | Native `spring-kafka` (KRaft, Manual Acks, `DefaultErrorHandler` + DLQ) |
| **Log Generator** | Multi-archetype Faker generator + `--burst` API | Multi-archetype Datafaker generator + Burst API |
| **Agentic Framework** | LangGraph (StateGraph ReAct) | LangChain4j (`@AiService` + `@Tool`) |
| **Embeddings Execution** | SentenceTransformers (Python process) | **In-Process ONNX Runtime** (inside JVM) |
| **Vector Store** | ChromaDB (In-Process / Embedded in Python) | **Apache Lucene 9 (In-Process / Embedded in Java)** |
| **Source Citations** | `LogSource` with `event_id` & normalized similarity `score` | `LogSource` with `eventId` & cosine similarity `score` |
| **MCP Integration** | FastMCP Python server | Spring SSE & Stdio JSON-RPC 2.0 MCP Server |
| **Packaging & Deploy** | Docker Compose / Helm / Kind | Multi-Stage Dockerfile / Compose / Helm |

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
| [docs/architecture-decisions.md](docs/architecture-decisions.md) | Architecture Decision Records (ADRs) |
| [docs/data-contract.md](docs/data-contract.md) | Pydantic log schema & serialization data contract |
| [docs/improvements.md](docs/improvements.md) | Production improvements & enterprise backlog |
| [charts/streaming-rag/README.md](charts/streaming-rag/README.md) | Helm values, Kind build/load/install |


---



## Development approach

This project was developed through pair programming and active collaboration with AI coding assistants. The workflow treats AI as a collaborative engineering partner: it accelerates implementation and surfaces alternatives, while architectural decisions, design trade-offs, and code review remain the developer's responsibility.

This includes: choosing when to use semantic search vs. metadata filtering, structuring the ReAct agent's tool selection logic, deciding on chunking strategy for streaming data, and making the judgment calls that determine whether generated code is actually correct — not just syntactically valid.
