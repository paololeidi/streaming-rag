# End-to-End Streaming RAG: Intelligent Incident & Log Analysis

## 📌 Project Overview

This project implements a Cloud-Native, Event-Driven Streaming RAG (Retrieval-Augmented Generation) architecture. It is designed to ingest real-time application logs, vectorize them on the fly, and expose an Agentic API capable of dynamically querying the live context to perform root-cause analysis and system troubleshooting.

---

## 🚀 Phase 1: Ingestion & Streaming (The Data Engine)

**Goal:** Establish a robust, high-throughput message broker and simulate a real-time stream of system events.

- [x] **1.1 Kafka KRaft Setup:** Configure and deploy a local Kafka cluster without Zookeeper using Docker Compose.
- [x] **1.2 Data Contract Definition:** Define strictly typed log schemas using Pydantic (e.g., timestamps, log levels, service names, stack traces).
- [x] **1.3 Python Producer:** Develop a mock service generating realistic application logs and pushing them to Kafka topics with proper partitioning keys.
- [x] **1.4 Python Consumer:** Implement a real-time consumer listener capable of reading the data stream and handling offsets reliably.

---



## 🧠 Phase 2: Vector Processing (The AI Core)

**Goal:** Transform the raw, real-time log stream into spatial embeddings and persist them in a Vector Database for similarity search.

- [x] **2.1 Vector Database Setup:** Deploy a local Vector DB instance (e.g., ChromaDB, Qdrant, or Milvus).
- [x] **2.2 Chunking Strategy:** Define how streaming text should be chunked/batched based on time windows or token limits to optimize retrieval.
- [x] **2.3 Embedding Generation:** Integrate a Text Embedding model (Hugging Face local or OpenAI API) to vectorize incoming Kafka messages.
- [x] **2.4 Asynchronous Ingestion:** Update the Kafka consumer to asynchronously write embeddings and metadata into the Vector DB without blocking the event loop.

---



## 🤖 Phase 3: Agentic RAG & API (The Intelligent Interface)

**Goal:** Build a reasoning LLM Agent capable of answering user queries by deciding when to query the streaming vector context vs. when to use other tools.

- [x] **3.1 LangSmith Observability:** Configured LangSmith auto-instrumentation via environment variables (`LANGCHAIN_TRACING_V2`, `LANGCHAIN_API_KEY`). Every agent run is traced automatically — full ReAct loop, token usage, tool I/O, and latency visible in the LangSmith UI.
- [x] **3.2 FastAPI Backend:** Async FastAPI app (`src/api/`) with a lifespan hook for embedding model warm-up, Swagger UI at `/docs`, and a `/health` liveness endpoint.
- [x] **3.3 Agent Architecture (LangGraph):** ReAct agent built with `langgraph.prebuilt.create_react_agent` and `ChatOllama` (local model via Ollama). Singleton graph shared across requests. Model: `llama3.2:3b` (~2 GB RAM, tool-calling capable).
- [x] **3.4 Tool Implementation:**
  - `vector_search`*:* Embeds the query with `all-MiniLM-L6-v2` and retrieves semantically similar log entries from ChromaDB. Output truncated to 400 chars/entry to limit LLM context size.
  - `log_stats`*:* Queries ChromaDB metadata for live count/distribution breakdowns by log level and service, filtered by a configurable time window.
- [x] **3.5 Prompt Engineering:** SRE analyst system prompt with explicit tool-selection guidance, source citation requirements, and a hard hallucination guardrail.

**Performance optimisations applied:**

- `keep_alive=30m` on `ChatOllama` eliminates the Ollama cold-start reload penalty.
- `POST /api/v1/query/stream` SSE endpoint streams tokens to the client as they are generated.
- Switched from `llama3.1` (4.9 GB) to `llama3.2:3b` (2.0 GB) — ~3x faster on CPU-only hardware.

---



## 🔌 Phase 4: MCP Integration (Interoperability Layer)

**Goal:** Implement both sides of the Model Context Protocol to make the system interoperable with any MCP-compatible client and extensible via external MCP servers.

- [x] **4.1 MCP Server — expose tools:** Build `src/mcp_server/server.py` using `fastmcp`. Re-expose `vector_search` and `log_stats` as MCP tools so any external client (Cursor, Claude Desktop, other agents) can query the live log context without going through the FastAPI layer.
- [x] **4.2 Cursor validation:** Connect Cursor to the local MCP server via MCP Settings. Verify end-to-end: a natural-language question in Cursor → MCP tool call → ChromaDB → grounded answer.
- [ ] **4.3 MCP Client — dynamic tool loading in the agent:** Integrate `langchain-mcp-adapters` into `graph.py` to let the LangGraph agent load tools from any running MCP server at startup, alongside the existing native tools. The plumbing is implemented and verified: setting `MCP_SERVER_URL` causes the agent to connect, fetch the tool list, and build the graph with those tools at startup. The full value of this pattern materialises in a future phase when the agent is pointed at an *external* MCP server (e.g. GitHub, Slack, a custom internal service) — at that point it gains new capabilities purely through configuration, with no code changes.

---



## ☁️ Phase 5: Cloud-Native Deployment & LLMOps (The Infrastructure)

**Goal:** Package the entire system into production-ready containers so a tester can bring up the full stack with a single Compose command — no local Python venv, manual Kafka compose, or hand-seeded producer/consumer — then deploy the same architecture to Kubernetes via Helm.

**Strategy:** Keep app images lean (producer, consumer, FastAPI, MCP server only). Run Ollama as a **peer Compose/K8s service** (official `ollama/ollama` image + persistent volume for models), not baked into the app Dockerfiles. Apps reach it over the Docker/K8s network via `OLLAMA_BASE_URL`.

- [x] **5.1 Dockerization:** Write optimized, multi-stage `Dockerfile`s for the Producer, Consumer, FastAPI backend, and MCP server (application code only — no LLM runtime in these images).
- [x] **5.2 Unified Compose:** Create a master `docker-compose.yml` that starts the full local stack: Kafka, Vector DB, Ollama (with model pull/init and a volume for persisted models), Producer, Consumer, FastAPI, and MCP Server. Wire `OLLAMA_BASE_URL=http://ollama:11434` (or equivalent) so the agent talks to the Ollama service on the Compose network. Target UX: `docker compose up` replaces the multi-step Quick start for local testing.
- [ ] **5.3 Kubernetes Manifests:** Translate the architecture into standard K8s resources (Deployments, Services, ConfigMaps, Secrets), including an Ollama Deployment/Service and a PVC for model storage.
- [ ] **5.4 Helm Chart Creation:** Package the manifests into a custom Helm Chart for parameterized, declarative deployments (model name, resource limits, optional GPU, etc.).
- [ ] **5.5 Local K8s Deployment:** Deploy the entire stack to a local cluster (Minikube/Kind) and validate end-to-end functionality.