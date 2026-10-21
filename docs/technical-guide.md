# Streaming RAG: System Architecture & Technical Implementation Guide

A unified, comprehensive reference detailing the engineering motivations, architectural trade-offs, and implementation specifications of the **End-to-End Streaming RAG** platform.

---

## 📋 Table of Contents

1. [Executive Summary & Problem Space](#1-executive-summary--problem-space)
2. [End-to-End System Architecture](#2-end-to-end-system-architecture)
3. [Layer 1 — Ingestion & Streaming (Kafka KRaft)](#3-layer-1--ingestion--streaming-kafka-kraft)
   - [Kafka KRaft Broker Architecture](#kafka-kraft-broker-architecture)
   - [Typed Log Contract (`SystemLog`)](#typed-log-contract-systemlog)
   - [Producer & Partition Key Strategy](#producer--partition-key-strategy)
   - [Consumer Group, Retry Resilience & Dead Letter Queue (DLQ)](#consumer-group-retry-resilience--dead-letter-queue-dlq)
4. [Layer 2 — Vector Processing & Semantic Storage (ChromaDB)](#4-layer-2--vector-processing--semantic-storage-chromadb)
   - [Embedding Model Selection](#embedding-model-selection)
   - [Log Chunking & Synthesis](#log-chunking--synthesis)
   - [ChromaDB Architecture: Embedded vs Client-Server](#chromadb-architecture-embedded-vs-client-server)
   - [Idempotent Upserts](#idempotent-upserts)
5. [Layer 3 — Agentic Reasoning & API (LangGraph & FastAPI)](#5-layer-3--agentic-reasoning--api-langgraph--fastapi)
   - [ReAct Agent Architecture](#react-agent-architecture)
   - [Local LLM Engine (ChatOllama)](#local-llm-engine-chatollama)
   - [Native Agent Tools (`vector_search` & `log_stats`)](#native-agent-tools-vector_search--log_stats)
   - [SRE Persona Prompt & Hallucination Guardrails](#sre-persona-prompt--hallucination-guardrails)
   - [FastAPI HTTP Layer & Real-Time Token Streaming (SSE)](#fastapi-http-layer--real-time-token-streaming-sse)
   - [LangSmith Observability](#langsmith-observability)
6. [Layer 4 — Interoperability via Model Context Protocol (MCP)](#6-layer-4--interoperability-via-model-context-protocol-mcp)
   - [Why Model Context Protocol?](#why-model-context-protocol)
   - [FastMCP Implementation & Schema Generation](#fastmcp-implementation--schema-generation)
   - [Transport Modes: Stdio vs SSE](#transport-modes-stdio-vs-sse)
   - [Async Graph Factory & Lifespan Architecture](#async-graph-factory--lifespan-architecture)
   - [Lazy Embedding Initialization](#lazy-embedding-initialization)
   - [Dynamic MCP Client Capabilities](#dynamic-mcp-client-capabilities)
7. [Layer 5 — Cloud-Native Packaging & Infrastructure (Docker, Helm, Kind)](#7-layer-5--cloud-native-packaging--infrastructure-docker-helm-kind)
   - [Multi-Stage Dockerfile Architecture](#multi-stage-dockerfile-architecture)
   - [Unified Docker Compose Orchestration](#unified-docker-compose-orchestration)
   - [Kubernetes & Parameterized Helm Chart](#kubernetes--parameterized-helm-chart)
8. [Architectural Decision Records (ADR Summary)](#8-architectural-decision-records-adr-summary)

---

## 1. Executive Summary & Problem Space

### The Incident Diagnosis Challenge
Diagnosing production incidents in distributed cloud architectures is inherently time-consuming and error-prone. Site Reliability Engineers (SREs) typically face:
1. **Log Fragmentation:** Thousands of unstructured log lines emitted every minute across dozens of independent microservices.
2. **Correlation Pressure:** Finding the causal chain (e.g., Auth service slows down $\rightarrow$ Payment service retries $\rightarrow$ Database connection pool starves) requires manual timestamp correlation across disparate dashboards.
3. **High Mean Time to Detect/Resolve (MTTD/MTTR):** High-stress manual grep/Elasticsearch queries delay mitigation.

### The Solution: Event-Driven Streaming RAG
This project bridges distributed data streaming and agentic reasoning. Rather than performing static batch ingestion or relying on LLM internal memory:
- **Continuous Ingestion:** Application logs are captured live from an Apache Kafka event stream with Dead Letter Queue (`system-logs-dlq`) protection.
- **In-Process Vectorization:** Incoming events are vectorized on the fly using a local dense embedding model and stored in a vector database alongside operational metadata (`event_id`, `service_name`, `log_level`, `timestamp`, `partition`, `offset`).
- **Agentic Reasoning Interface:** A LangGraph ReAct agent powered by a local quantized LLM dynamically selects between semantic similarity search and statistical aggregation queries, synthesizing grounded answers with enriched citations (`event_id` and similarity `score`).
- **Open Protocol Interoperability:** An integrated Model Context Protocol (MCP) server exposes the real-time context to external AI clients (IDE assistants, multi-agent systems) without requiring custom API clients.

---

## 2. End-to-End System Architecture

```
┌───────────────────────────────── Data Pipeline ──────────────────────────────────┐
│                                                                                  │
│   Mock Producer  ──────►  Kafka Broker  ──────►  Ingestion Consumer              │
│  (src/producer.py)        (KRaft Mode)           (src/consumer.py)               │
│   [stream / --burst]      ├── system-logs               │                        │
│                           └── system-logs-dlq ◄─────────┤ (DLQ on error/corrupt) │
│                                                         ▼                        │
│                                                 all-MiniLM-L6-v2                 │
│                                                (src/embeddings.py)               │
│                                                         │                        │
│                                                         ▼                        │
│                                                      ChromaDB                    │
│                                                (data/chroma or HTTP)             │
└────────────────────────────────────────┬─────────────────────────────────────────┘
                                         │
┌───────────────────────────────── Agentic Layer ──────────────────────────────────┐
│                                                                                  │
│   Client REST / SSE Request                                                      │
│              │                                                                   │
│              ▼                                                                   │
│   FastAPI (src/api/main.py)                                                      │
│              │                                                                   │
│              ▼                                                                   │
│   LangGraph ReAct Agent (src/agent/graph.py · Ollama llama3.2:3b)                │
│         ├── vector_search ──► ChromaDB (semantic retrieval + confidence score)   │
│         ├── log_stats    ──► ChromaDB (metadata aggregation)                     │
│         └── [MCP Tools]  ──► External MCP Servers (via SSE)                      │
└────────────────────────────────────────┬─────────────────────────────────────────┘
                                         │
┌───────────────────────────────── MCP Server ─────────────────────────────────────┐
│                                                                                  │
│   FastMCP Server (src/mcp_server/server.py)                                      │
│         ├── vector_search (ChromaDB + all-MiniLM-L6-v2)                          │
│         └── log_stats     (ChromaDB metadata filter)                             │
│                  │                                                               │
│                  ├── SSE (:8001/sse) ──► Remote AI Agents / IDEs                 │
│                  └── stdio           ──► Local Desktop Clients (Cursor, Claude)  │
└──────────────────────────────────────────────────────────────────────────────────┘
```

---

## 3. Layer 1 — Ingestion & Streaming (Kafka KRaft)

The ingestion layer establishes a high-throughput, fault-tolerant message pipeline that decouples log generation from indexing.

### Kafka KRaft Broker Architecture
- **Single-Node KRaft Mode (`confluentinc/cp-kafka:7.5.0`):** Operates without an Apache ZooKeeper cluster. Combined broker and controller roles (`KAFKA_PROCESS_ROLES=broker,controller`) drastically reduce container memory footprint and eliminate ZooKeeper metadata synchronization lag.
- **Topic Configuration:** Primary logs stream to `system-logs`; corrupted or persistently failing records route to `system-logs-dlq`.

### Typed Log Contract (`SystemLog`)
All log events adhere to a strict Pydantic model (`src/model/models.py`):

```python
class LogLevel(StrEnum):
    INFO = "INFO"
    WARN = "WARN"
    ERROR = "ERROR"


class SystemLog(BaseModel):
    event_id: UUID | None = Field(default=None)
    timestamp: str = Field(default_factory=lambda: datetime.now(timezone.utc).isoformat())
    service_name: str = Field(min_length=1)
    log_level: LogLevel
    message: str = Field(min_length=1)
    stack_trace: str | None = None
```

### Producer & Partition Key Strategy
The mock producer (`src/producer.py`) simulates realistic microservice activity across four domains (`payment-gateway`, `user-auth`, `inventory-service`, `frontend-bff`):
- **Multi-Archetype Realistic Telemetry:** Generates weighted log levels (70% `INFO`, 20% `WARN`, 10% `ERROR`), domain-specific messages with realistic identifiers (`usr_...`, `ord_...`, `SKU-...`, IPv4 addresses), and multi-line stack traces for `ERROR` events.
- **Programmatic & CLI Burst Mode:** Exposes `send_log()`, `generate_and_send()`, and `generate_and_send_burst(count)` as well as CLI `--burst <count>` for deterministic test seeding alongside continuous streaming.
- **Partition Key Assignment:** The message key is set to `service_name.encode("utf-8")`. Because Kafka maps partitions using `hash(key) % num_partitions`, all logs emitted by the same service land on the same partition in strict chronological order.

### Consumer Group, Retry Resilience & Dead Letter Queue (DLQ)
The consumer (`src/consumer.py`) belongs to the `rag-ingestion-group` consumer group:
- **`auto.offset.reset = earliest`:** Guarantees that new consumer instances replay existing topic history.
- **Manual Offset Commits (`enable.auto.commit = False`):**
  Kafka offsets are **only committed synchronously** (`consumer.commit(message=msg, asynchronous=False)`) *after* the log embedding has been successfully calculated and upserted into ChromaDB.
- **Exponential Backoff Retries:** `process_message()` retries transient embedding or ChromaDB errors up to `max_retries=2` times with exponential backoff (`1.0s`, `2.0s`).
- **Dead Letter Queue (`system-logs-dlq`):** Malformed payloads (`ValidationError` / `UnicodeDecodeError`) or messages that exhaust all retry attempts are published via `publish_to_dlq()` to `system-logs-dlq` with diagnostic headers (`dlq_reason`, `original_topic`, `original_partition`, `original_offset`) before committing offsets.

---

## 4. Layer 2 — Vector Processing & Semantic Storage (ChromaDB)

The vector layer converts raw string logs into spatial embeddings to allow semantic similarity lookups.

### Embedding Model Selection
- **Model:** `all-MiniLM-L6-v2` (SentenceTransformers, 384 dimensions).
- **In-Process Inference:** Executes locally in Python without making remote cloud API calls (no OpenAI API key dependencies, zero external network latency, full data privacy).
- **Latency & Resource Footprint:** Produces 384-dimensional dense vectors in ~5–15 ms per message on standard CPU hardware.

### Log Chunking & Synthesis
Logs are small structured payloads rather than lengthy unstructured documents. The chunking strategy (`src/chunking.py`) applies a **one-log-entry-to-one-chunk** rule:
- `SystemLog.to_embedding_text()` compiles service name, log level, UTC timestamp, message, and stack trace into a normalized plain-text format:
  ```text
  [ERROR] payment-gateway @ 2026-10-12T10:00:00+00:00
  Stripe API unreachable after 3 exponential backoff retries (HTTP 502)
  stack_trace: java.net.SocketTimeoutException: Read timed out connecting to https://api.stripe.com/v1/charges...
  ```
- Metadata persisted per chunk includes `event_id`, `service_name`, `log_level`, `timestamp`, `partition`, and `offset`.

### ChromaDB Architecture: Embedded vs Client-Server
The vector store wrapper (`src/vector_store.py`) provides seamless dual-mode operation:
- **Local Embedded Mode:** Uses `chromadb.PersistentClient(path="data/chroma")`. Runs directly in-process against local SQLite/DuckDB files (ideal for development without running extra containers).
- **Cloud-Native Server Mode:** When `CHROMA_HOST` is defined (in Docker Compose / Kubernetes), the wrapper automatically switches to `chromadb.HttpClient(host=CHROMA_HOST, port=CHROMA_PORT)`. Multiple containers (consumer, API, MCP) concurrently query the same ChromaDB instance.

### Idempotent Upserts
To preserve at-least-once delivery semantics without creating duplicate records on Kafka redelivery, every record in ChromaDB is indexed by `event_id` (falling back to `kafka-{partition}-{offset}`). Subsequent writes with the same ID update existing records rather than duplicating them.

---

## 5. Layer 3 — Agentic Reasoning & API (LangGraph & FastAPI)

The agentic layer provides the intelligence: an LLM that actively decides what data it needs to inspect in order to answer user inquiries.

### ReAct Agent Architecture
Rather than using a rigid, static retrieval chain, the system builds an autonomous ReAct (Reasoning + Acting) loop using **LangGraph** (`langgraph.prebuilt.create_react_agent`):
1. User provides a prompt (e.g., *"Why did the payment service fail in the last 15 minutes?"*).
2. The agent analyzes the question and selects an appropriate tool:
   - For statistical distributions: calls `log_stats`.
   - For semantic root cause diagnosis: calls `vector_search`.
3. The tool executes against ChromaDB and appends a `ToolMessage` with the retrieved log context.
4. The agent inspects the tool response, decides whether more evidence is required, and either repeats tool execution or synthesizes a final cited answer.

### Local LLM Engine (ChatOllama)
- **Model:** `llama3.2:3b` via Ollama (`ChatOllama`).
- **RAM Footprint:** ~2.0 GB VRAM/RAM (3x faster than 7B/8B models on CPU).
- **Tool-Calling Capability:** Supports OpenAI-compatible function calling natively.
- **Cold-Start Optimization:** Configured with `OLLAMA_KEEP_ALIVE=30m` to prevent the model from unloading between requests.

### Native Agent Tools (`vector_search` & `log_stats`)
Located in `src/agent/tools.py`:
1. **`vector_search(query: str, k: int = 5) -> str`**:
   Vectorizes the input query with `all-MiniLM-L6-v2`, queries ChromaDB for the top-$k$ nearest neighbors, computes a normalized similarity confidence score (`score = round(1.0 / (1.0 + max(0.0, distance)), 3)`), and formats the matching logs with `event_id` and `score` in the header. Excerpts are truncated to 400 characters to protect the LLM context window.
2. **`log_stats(log_level: str | None = None, service_name: str | None = None, minutes: int = 60) -> str`**:
   Bypasses semantic embedding. Directly queries ChromaDB metadata within a sliding time window (`now - timedelta(minutes=minutes)`), returning exact count breakdowns by service and level.

### SRE Persona Prompt & Hallucination Guardrails
The system prompt (`src/agent/prompts.py`) enforces strict SRE operational guidelines:
- **Mandatory Tool Usage:** The agent is explicitly forbidden from guessing or answering from training memory.
- **Citation Guardrail:** Every claim must cite the service name, log level, and timestamp of the retrieved log.
- **Anti-Hallucination Policy:** If no matching logs are found in ChromaDB, the agent must reply: *"No matching logs found in the vector store"* rather than inventing plausible scenarios.

### FastAPI HTTP Layer & Real-Time Token Streaming (SSE)
- **Synchronous Endpoint (`POST /api/v1/query`):** Returns `{ answer: str, sources: list[LogSource] }`, where each `LogSource` includes `event_id`, `service_name`, `log_level`, `timestamp`, `score`, and `excerpt`.
- **Streaming Endpoint (`POST /api/v1/query/stream`):** Implements Server-Sent Events (SSE) via `StreamingResponse(media_type="text/event-stream")`. Streams LLM tokens as they are generated, followed by a terminating `data: [DONE]` event.
- **Lifespan Context Manager:** The FastAPI `lifespan` hook pre-warms the Hugging Face embedding model into memory and initializes the shared singleton LangGraph agent graph at startup.

### LangSmith Observability
Configured automatically via standard environment variables:
- `LANGCHAIN_TRACING_V2=true`
- `LANGCHAIN_API_KEY=<key>`
- `LANGCHAIN_PROJECT=streaming-rag`

Logs every ReAct trajectory step, tool invocation latency, token usage breakdown, and LLM input/output directly to the LangSmith dashboard.

---

## 6. Layer 4 — Interoperability via Model Context Protocol (MCP)

The Model Context Protocol layer exposes the log intelligence engine to external AI clients without coupling them to FastAPI REST endpoints.

### Why Model Context Protocol?
Model Context Protocol (MCP) is an open standard designed to decouple AI applications from tool providers. By exposing tools through MCP, any MCP-compatible environment (Cursor IDE, Claude Desktop, autonomous multi-agent systems) can inspect the live log context natively.

### FastMCP Implementation & Schema Generation
Built using `fastmcp` (`src/mcp_server/server.py`):
- `fastmcp.FastMCP` auto-generates JSON Schema tool definitions from Python type annotations and docstrings.
- Tools (`vector_search` and `log_stats`) share the underlying vector store, similarity score normalization, and embedder logic with the agent layer, ensuring unified query behavior without code duplication.

### Transport Modes: Stdio vs SSE
1. **SSE Transport (`python src/mcp_server/server.py --sse`):**
   Runs an HTTP server on port 8001. Ideal for networked, containerized environments (Docker Compose, Kubernetes) and remote AI clients.
2. **Stdio Transport (`python src/mcp_server/server.py`):**
   Communicates over standard input/output. Spawns directly as a child process of desktop IDE clients (Cursor, Claude Desktop) without opening network ports.

### Async Graph Factory & Lifespan Architecture
In `src/agent/graph.py`, agent graph creation was designed as an async factory (`async def build_agent_graph(mcp_server_url=None)`):
- When `MCP_SERVER_URL` is configured, `langchain-mcp-adapters` initializes a `MultiServerMCPClient`.
- Because MCP clients require an active async session context, managing the client inside the FastAPI `lifespan` handler ensures that connections remain open during request processing and close cleanly on shutdown.

### Lazy Embedding Initialization
In `src/mcp_server/server.py`, the embedding model is loaded lazily on the first invocation of `vector_search` rather than at module import. This guarantees that MCP protocol handshakes (which have strict timeout thresholds) complete in milliseconds without blocking on heavy model loading.

### Dynamic MCP Client Capabilities
The LangGraph agent can act as both an MCP provider and consumer:
- Setting `MCP_SERVER_URL` allows the agent to fetch external tools (e.g., GitHub MCP, Prometheus MCP, Slack MCP) at runtime, expanding its capabilities dynamically without code modifications.

---

## 7. Layer 5 — Cloud-Native Packaging & Infrastructure (Docker, Helm, Kind)

The platform is fully packaged for declarative, single-command deployment locally and on Kubernetes.

### Multi-Stage Dockerfile Architecture
Located at `Dockerfile`:
- **Stage 1 (Builder):** Uses `python:3.11-slim`. Installs build dependencies (C compilers required for `confluent-kafka`), installs Python packages into `/install`, and pre-downloads the `all-MiniLM-L6-v2` SentenceTransformer weights into the image cache.
- **Stage 2 (Runtime):** Copies only the installed Python packages and cached model weights into a clean, minimal runtime layer.
- **Unified Image:** The same image serves all four application roles (`producer`, `consumer`, `api`, `mcp`), with entrypoints customized via container commands.

### Unified Docker Compose Orchestration
The root `docker-compose.yml` orchestrates the complete 7-container topology:
- **Infrastructure Services:** `kafka` (KRaft broker), `chroma` (ChromaDB HTTP server), `ollama` (peer LLM container), and `ollama-init` (one-shot model pull helper).
- **Application Services:** `producer`, `consumer`, `api`, and `mcp`.
- **Healthcheck-Driven Dependency Tree:**
  1. `kafka` and `chroma` report healthy.
  2. `producer` and `consumer` begin streaming.
  3. `ollama` passes healthcheck $\rightarrow$ `ollama-init` pulls `llama3.2:3b` and terminates cleanly.
  4. `api` starts only after `chroma` is healthy and `ollama-init` is completed.

### Kubernetes & Parameterized Helm Chart
Packaged in `charts/streaming-rag/`:
- **Kubernetes Resources:** Deployments for API, Consumer, Producer, and MCP; StatefulSet/Deployment for Chroma and Ollama; PersistentVolumeClaims for model and vector storage; ClusterIP Services; ConfigMaps and Secrets.
- **Helm Values:** Parameterized model names (`ollama.model`), Kafka broker addresses, replicas, and CPU/memory resource limits.
- **One-Command Kind Deployment:** `./scripts/kind-deploy.sh` creates a local Kind cluster, builds the application container, loads the image into cluster nodes, and applies the Helm chart.

---

## 8. Architectural Decision Records (ADR Summary)

| ID | Topic | Decision | Primary Rationale |
|---|---|---|---|
| **ADR-01** | Kafka Mode | **KRaft (Single-Node)** | Eliminates ZooKeeper dependency, decreases RAM footprint, uses modern Kafka metadata protocol. |
| **ADR-02** | Partitioning | **Key by `service_name`** | Guarantees that logs for the same service arrive on the same partition in chronological sequence. |
| **ADR-03** | Ingestion Offsets | **Manual Synchronous Commits** | Offsets commit only after successful vector write, guaranteeing at-least-once processing without data loss. |
| **ADR-04** | Embedding Model | **In-Process `all-MiniLM-L6-v2`** | 384 dimensions, zero cloud API fees, no remote network latency, complete data isolation. |
| **ADR-05** | Vector DB Mode | **Dual-Mode (Embedded / HTTP)** | Embedded mode simplifies local development without Docker; HTTP mode enables shared access in Compose/K8s. |
| **ADR-06** | Agent Framework | **LangGraph ReAct Loop** | Autonomous tool selection (`vector_search` vs `log_stats`) avoids rigid, brittle hardcoded chains. |
| **ADR-07** | LLM Engine | **Ollama (`llama3.2:3b`)** | 2 GB memory footprint, tool-calling capability, fast CPU token streaming without cloud billing. |
| **ADR-08** | Interoperability | **Model Context Protocol (FastMCP)** | Exposes live vector store to any MCP-compliant agent (Cursor, Claude) as a standard protocol. |
| **ADR-09** | Containerization | **Single Multi-Stage Image** | Pre-bakes model weights for rapid cold starts; simplifies container registry management. |
| **ADR-10** | Consumer Resilience | **DLQ (`system-logs-dlq`) + Retries** | Exponential backoff retries absorb transient errors; DLQ preserves poison-pill payloads with diagnostic headers. |
| **ADR-11** | Log Simulation | **Multi-Archetype + Burst API** | Service-specific incident templates, weighted log levels, and `--burst N` / `generate_and_send_burst`. |
| **ADR-12** | Source Citations | **Event ID + Similarity Score** | Normalizes vector distance into $[0, 1]$ confidence score and exposes `event_id` in `LogSource`. |
