# Architectural Decision Records (ADR) — Streaming RAG

This document details the **Architectural Decision Records (ADRs)** for the Streaming RAG platform. Each record outlines the operational context, the problem statement, the evaluated alternatives, the final decision with engineering rationales, trade-offs, and exact code references.

---

## 📋 ADR Index

| ADR ID | Title | Status | Scope |
| :--- | :--- | :--- | :--- |
| [**ADR-01**](#adr-01-kafka-kraft-mode-over-zookeeper) | Kafka KRaft Mode over ZooKeeper | **Accepted** | Layer 1 (Ingestion) |
| [**ADR-02**](#adr-02-kafka-partitioning-key-by-service_name) | Kafka Partitioning Key by `service_name` | **Accepted** | Layer 1 (Ingestion) |
| [**ADR-03**](#adr-03-manual-synchronous-offset-commits-over-auto-commit) | Manual Synchronous Offset Commits over Auto-Commit | **Accepted** | Layer 1 (Ingestion) |
| [**ADR-04**](#adr-04-in-process-all-minilm-l6-v2-embeddings-over-cloud-apis) | In-Process `all-MiniLM-L6-v2` Embeddings over Cloud APIs | **Accepted** | Layer 2 (Vector Storage) |
| [**ADR-05**](#adr-05-dual-mode-chromadb-embedded-sqlite-vs-http-client) | Dual-Mode ChromaDB (Embedded SQLite vs. HTTP Client) | **Accepted** | Layer 2 (Vector Storage) |
| [**ADR-06**](#adr-06-langgraph-autonomous-react-loop-over-naive-rag) | LangGraph Autonomous ReAct Loop over Naive RAG | **Accepted** | Layer 3 (Agentic Layer) |
| [**ADR-07**](#adr-07-local-quantized-ollama-llama323b-with-keep-alive) | Local Quantized Ollama `llama3.2:3b` with Keep-Alive | **Accepted** | Layer 3 (Agentic Layer) |
| [**ADR-08**](#adr-08-model-context-protocol-fastmcp-for-interoperability) | Model Context Protocol (FastMCP) for Interoperability | **Accepted** | Layer 4 (MCP Protocol) |
| [**ADR-09**](#adr-09-single-multi-stage-docker-image-with-pre-baked-weights) | Single Multi-Stage Docker Image with Pre-Baked Weights | **Accepted** | Layer 5 (Infrastructure) |
| [**ADR-10**](#adr-10-dead-letter-queue-dlq-and-exponential-backoff-retry-resilience) | Dead Letter Queue (DLQ) & Exponential Backoff Retry Resilience | **Accepted** | Layer 1 (Ingestion Resilience) |
| [**ADR-11**](#adr-11-multi-archetype-incident-generator-and-programmatic-burst-api) | Multi-Archetype Incident Generator & Programmatic Burst API | **Accepted** | Layer 1 (Simulation & Testing) |
| [**ADR-12**](#adr-12-enriched-source-citations-with-normalized-similarity-scores) | Enriched Source Citations with Normalized Similarity Scores | **Accepted** | Layer 2 & 3 (Retrieval & API) |

---

## ADR-01: Kafka KRaft Mode over ZooKeeper

### Status
**Accepted**

### Context & Problem Statement
Microservice architectures generate continuous, bursty streams of logs and diagnostic traces. We needed an event streaming broker to buffer and decouple log emission from vector indexing. Traditionally, Apache Kafka required a separate Apache ZooKeeper ensemble to manage broker metadata, topic configurations, and partition leader elections. Running ZooKeeper alongside Kafka in local developer environments (Docker Desktop, Kind) adds significant memory overhead and startup complexity.

### Considered Alternatives
1. **Kafka with ZooKeeper ensemble:** Traditional multi-container setup (`cp-zookeeper` + `cp-kafka`).
2. **RabbitMQ / Redis Streams:** Lightweight message brokers, but lack Kafka's robust replayability, distributed partition log model, and ecosystem-standard consumer group semantics.
3. **Kafka in KRaft Mode (KIP-500):** Modern single-node Kafka operating without ZooKeeper, utilizing Raft metadata quorum internally.

### Decision Outcome
We adopted **Kafka KRaft Mode** using `confluentinc/cp-kafka:7.5.0` with combined broker and controller roles (`KAFKA_PROCESS_ROLES=broker,controller`).

### Decision Rationales & Trade-Offs
* **Reduced Memory Footprint:** Eliminates the ZooKeeper JVM container entirely, saving ~1 GB of host RAM in Docker Desktop and Kind.
* **Faster Recovery & Zero Sync Lag:** Metadata changes are committed to an internal Raft partition rather than synchronized asynchronously across an external ZooKeeper cluster.
* **Modern Standards:** Prepares the architecture for modern production Kafka topologies (ZooKeeper has been officially deprecated by the Apache Kafka project).
* **Trade-Off:** Single-node KRaft is optimized for development, testing, and single-broker validation; production horizontal scale requires configuring a 3-node controller quorum.

### Code References
* [`docker-compose.yml`](../docker-compose.yml#L9-L33) (lines 9–33)
* [`charts/streaming-rag/templates/kafka.yaml`](../charts/streaming-rag/templates/kafka.yaml)

---

## ADR-02: Kafka Partitioning Key by `service_name`

### Status
**Accepted**

### Context & Problem Statement
Kafka topics are divided into partitions for horizontal scale. However, Kafka only guarantees **strict message ordering within a single partition**, not across different partitions. In site reliability engineering (SRE) incident investigation, diagnosing root cause requires preserving the chronological sequence of operations emitted by a specific service (e.g., `user-auth` logs *CredentialCheck $\rightarrow$ RateLimitExceeded $\rightarrow$ 401 Unauthorized*).

### Considered Alternatives
1. **Null Partition Key (Round-Robin):** Distributes messages evenly across all partitions.
2. **Keying by `event_id` (Random UUID):** Uniformly hashes random UUIDs across partitions.
3. **Keying by `service_name`:** Uses `service_name.encode("utf-8")` as the Kafka message key.

### Decision Outcome
We chose to **partition explicitly by `service_name`**.

### Decision Rationales & Trade-Offs
* **Chronological Causality:** Kafka computes `hash(service_name) % num_partitions`. Consequently, all logs generated by a specific microservice land on the exact same partition in strict chronological order.
* **Consumer Group Scalability:** Multiple consumer replicas within the same consumer group can process different services in parallel without race conditions or timestamp inversions.
* **Trade-Off (Partition Hotspotting):** If one service logs orders of magnitude more events than others (e.g. an aggressive chatty service), its assigned partition receives disproportionate traffic. This is mitigated in production by balancing topic partition counts and configuring log sampling rates.

### Code References
* [`src/producer.py`](../src/producer.py#L194-L212) (`send_log`)
* [`src/model/models.py`](../src/model/models.py#L25-L28) (lines 25–28)

---

## ADR-03: Manual Synchronous Offset Commits over Auto-Commit

### Status
**Accepted**

### Context & Problem Statement
When ingesting streaming logs into a vector database, the consumer must calculate high-dimensional embeddings and execute vector store writes. If a container crashes, runs out of memory, or loses network connectivity mid-operation, we must guarantee that no log entries are permanently lost.

### Considered Alternatives
1. **Auto-Commit (`enable.auto.commit = True`):** Kafka commits the latest polled offset periodically in background (e.g., every 5000 ms).
2. **Asynchronous Commit (`consumer.commit(asynchronous=True)`):** Non-blocking offset commit after each message.
3. **Manual Synchronous Commit (`enable.auto.commit = False`):** Blocking offset commit executed strictly after the vector database write succeeds.

### Decision Outcome
We adopted **Manual Synchronous Commits** (`enable.auto.commit = False`) combined with **Idempotent Upserts** in ChromaDB.

### Decision Rationales & Trade-Offs
* **Zero Data Loss Guarantee (At-Least-Once Delivery):** Auto-commit risks committing offsets before vectorization finishes. If the worker crashes during embedding generation, the message is lost forever (*At-Most-Once*). By committing synchronously only after `upsert_log()` completes, any uncompleted message is automatically redelivered upon restart.
* **Handling Redelivery with Idempotency:** Because Kafka guarantees *At-Least-Once*, network failures during commit could trigger redelivery. To prevent duplicate records in ChromaDB, every log is indexed by a deterministic `event_id` (or fallback `kafka-{partition}-{offset}`). On replay, `collection.upsert()` overwrites the existing entry rather than duplicating it.
* **Trade-Off (Commit Latency):** Synchronous commits add a round-trip network hop to Kafka per message. In high-throughput production, this can be optimized by batching messages (`consumer.poll()` micro-batches followed by single batch commit).

### Code References
* [`src/consumer.py`](../src/consumer.py#L53-L98) (`process_message`)
* [`src/consumer.py`](../src/consumer.py#L101-L108) (`conf`)
* [`src/vector_store.py`](../src/vector_store.py#L32-L43) (`upsert_log`)

---

## ADR-04: In-Process `all-MiniLM-L6-v2` Embeddings over Cloud APIs

### Status
**Accepted**

### Context & Problem Statement
Every log message streamed from Kafka must be converted into a spatial dense vector to enable semantic similarity searches for error descriptions, root causes, and stack traces.

### Considered Alternatives
1. **Cloud Embedding APIs (OpenAI `text-embedding-3-small`, Cohere):** High quality, but requires remote HTTP round-trips for every log entry and incurs usage costs.
2. **Heavyweight Local Models (e.g. `bge-large-en-v1.5`, 1024 dimensions):** High accuracy, but requires GPU acceleration and introduces high CPU latency on developer laptops.
3. **In-Process SentenceTransformer (`all-MiniLM-L6-v2`, 384 dimensions):** Lightweight 80 MB model running locally on CPU.

### Decision Outcome
We selected **in-process `all-MiniLM-L6-v2`** via the `sentence-transformers` library.

### Decision Rationales & Trade-Offs
* **Privacy & Security Compliance:** Infrastructure and security logs frequently contain internal IPs, service names, and diagnostic traces. In-process vectorization ensures zero sensitive telemetry leaves the infrastructure boundary.
* **Predictable Low Latency:** Produces 384-dimensional embeddings in ~5–15 ms on standard CPU without outbound network hops, rate limits, or HTTP serialization overhead.
* **Zero Operational Cost:** No recurring billing for embedding millions of synthetic or production log events.
* **Trade-Off:** CPU consumption during sustained burst ingestion. Mitigated by pre-baking model weights into Docker and supporting batch embedding (`embed_batch`).

### Code References
* [`src/embeddings.py`](../src/embeddings.py#L6-L21) (lines 6–21)
* [`Dockerfile`](../Dockerfile#L18-L23) (lines 18–23, pre-download)

---

## ADR-05: Dual-Mode ChromaDB (Embedded SQLite vs. HTTP Client)

### Status
**Accepted**

### Context & Problem Statement
The system must support two conflicting operational workflows:
1. **Rapid Local Development & Testing:** Developers running unit tests (`pytest`) want zero external dependencies (no running Docker containers).
2. **Containerized Multi-Service Deployment:** In Docker Compose and Kubernetes, multiple independent processes (`consumer`, `api`, `mcp_server`) need concurrent read/write access to the exact same vector store.

### Considered Alternatives
1. **Always Require External Chroma Server:** Forces developers to start Docker even for quick unit tests.
2. **Embedded Client Only:** Fails in Docker Compose because multiple containers cannot concurrently share a local SQLite lock without database corruption.
3. **Dedicated Enterprise Vector DB (Qdrant, Milvus):** High operational overhead for single-developer environments.

### Decision Outcome
We implemented a **transparent dual-mode client wrapper** in [`src/vector_store.py`](../src/vector_store.py#L15-L23).

### Decision Rationales & Trade-Offs
* **Zero-Friction Local Testing:** When `CHROMA_HOST` is empty (default in local `.env`), it initializes `chromadb.PersistentClient(path="data/chroma")`. Tests run in-process against local disk files.
* **Multi-Container Concurrency:** When `CHROMA_HOST="chroma"` (set in Compose and Helm), it automatically switches to `chromadb.HttpClient(host=..., port=...)`, allowing the consumer to write while the API and MCP server query concurrently.
* **Clean Abstraction:** Downstream code calls `get_collection()`, `upsert_log()`, and `query_similar()` without needing to know which mode is active.

### Code References
* [`src/vector_store.py`](../src/vector_store.py#L15-L30) (lines 15–30)
* [`src/config.py`](../src/config.py#L16-L22) (lines 16–22)

---

## ADR-06: LangGraph Autonomous ReAct Loop over Naive RAG

### Status
**Accepted**

### Context & Problem Statement
Traditional "Naive RAG" follows a rigid sequential pipeline: `User Query -> Embed Query -> Top-K Vector Search -> LLM Prompt Synthesis`.
In operational log analysis, this pattern fundamentally fails. For example:
* If an SRE asks: *"How many 500 errors occurred in the payment service over the last 30 minutes?"*, vector search cannot count or aggregate frequencies.
* If an SRE asks a multi-part question: *"Which service had the highest error volume and what was the root cause?"*, answering requires first querying aggregate metrics and then performing targeted vector retrieval.

### Considered Alternatives
1. **Naive RAG Pipeline:** Static vector search on every question.
2. **Hardcoded Router Pipeline:** Rule-based if/else router determining whether to do vector search or database aggregation.
3. **LangGraph ReAct (Reasoning + Acting) Agent:** Autonomous agent loop powered by `create_react_agent` with native tools.

### Decision Outcome
We implemented a **LangGraph ReAct Agent** with two specialized tools:
1. [`vector_search`](../src/agent/tools.py#L20-L65): Semantic similarity retrieval for errors and stack traces.
2. [`log_stats`](../src/agent/tools.py#L68-L152): ChromaDB metadata filtering for exact frequency distributions across sliding time windows.

### Decision Rationales & Trade-Offs
* **Autonomous Intent Routing:** The agent dynamically decides which tool to invoke, refines queries if initial context is insufficient, and chains tools when questions require both statistical metrics and semantic evidence.
* **Strict Anti-Hallucination Guardrails:** The system prompt explicitly mandates tool usage before answering, enforces exact source citations (`service_name`, `log_level`, `timestamp`), and forbids inventing unindexed logs.
* **Trade-Off (Latency & Loop Budget):** ReAct loops involve multiple LLM generation cycles (think $\rightarrow$ tool call $\rightarrow$ think $\rightarrow$ response). Mitigated by temperature 0 and concise system prompts.

### Code References
* [`src/agent/graph.py`](../src/agent/graph.py#L33-L48) (lines 33–48)
* [`src/agent/tools.py`](../src/agent/tools.py#L20-L152)
* [`src/agent/prompts.py`](../src/agent/prompts.py#L3-L39)

---

## ADR-07: Local Quantized Ollama `llama3.2:3b` with Keep-Alive

### Status
**Accepted**

### Context & Problem Statement
The agent requires an LLM engine capable of native function/tool calling. Running large 7B or 8B parameter models locally on standard laptops without dedicated GPUs causes slow token generation (high TTFT - Time To First Token) and consumes 6–8 GB of RAM. Conversely, calling cloud LLM APIs (OpenAI GPT-4, Anthropic Claude) incurs ongoing costs and leaks operational log data outside the local network.

### Considered Alternatives
1. **Cloud LLM APIs (OpenAI / Anthropic):** High quality, but introduces cloud billing and sensitive log data exfiltration.
2. **Local 7B/8B Models (`llama3.1:8b`, `mistral:7b`):** High quality tool calling, but exceeds standard laptop RAM thresholds (~8 GB just for weights) and runs slowly on CPU.
3. **Small Non-Tool Models (e.g. `phi-2`):** Low memory, but lacks reliable structured tool-calling capabilities.
4. **Local Quantized `llama3.2:3b` via Ollama:** Compact 3B model optimized for edge devices and native function calling.

### Decision Outcome
We adopted **`llama3.2:3b` hosted via Ollama** with `OLLAMA_KEEP_ALIVE=30m`.

### Decision Rationales & Trade-Offs
* **Low Memory Footprint:** Requires only ~2.0 GB VRAM/RAM, leaving ample host memory for Kafka, ChromaDB, and Python services.
* **Fast CPU Token Generation:** Produces responsive streaming answers on standard CPU cores (~3x faster than 7B/8B models).
* **Native Tool-Calling Support:** Fine-tuned specifically for structured JSON function calling.
* **Cold-Start Elimination:** Setting `keep_alive=30m` prevents Ollama from unloading the model from RAM after idle periods, eliminating 5–10 second reload delays.
* **Trade-Off:** 3B models have smaller reasoning capacity than 70B models. Mitigated by strict prompt constraints and structured tool contracts.

### Code References
* [`src/agent/graph.py`](../src/agent/graph.py#L24-L30) (lines 24–30)
* [`docker-compose.yml`](../docker-compose.yml#L53-L84) (lines 53–84)
* [`charts/streaming-rag/templates/ollama.yaml`](../charts/streaming-rag/templates/ollama.yaml)

---

## ADR-08: Model Context Protocol (FastMCP) for Interoperability

### Status
**Accepted**

### Context & Problem Statement
Building custom web frontends or proprietary REST integrations creates vendor lock-in. Modern AI-assisted development environments (such as Cursor IDE and Claude Desktop) require standardized protocols to discover and invoke contextual tools on behalf of engineers.

### Considered Alternatives
1. **REST Endpoints Only:** Requires external clients to write custom HTTP glue code, authentication, and parsing logic.
2. **GraphQL API:** Flexible, but still requires custom client integration and lacks agentic tool auto-discovery.
3. **Model Context Protocol (FastMCP):** Open protocol standard (originated by Anthropic) for tool discovery, execution, and contextual grounding.

### Decision Outcome
We implemented a dedicated **FastMCP server** ([`src/mcp_server/server.py`](../src/mcp_server/server.py)) and equipped the FastAPI agent to act as both an **MCP Provider** and an **MCP Consumer**.

### Decision Rationales & Trade-Offs
* **Native IDE Integration:** Developers diagnosing production issues can query live logs directly inside Cursor or Claude Desktop without leaving their IDE.
* **Dual Transports:** Supports `stdio` transport for desktop IDE child processes and `SSE` (Server-Sent Events) HTTP transport for networked/Kubernetes deployment.
* **Lazy Embedding Loading:** The embedding model is loaded lazily on the first `vector_search` call rather than at import time, preventing MCP handshake protocol timeouts.
* **Dynamic Tool Ingestion:** Setting `MCP_SERVER_URL` allows the LangGraph agent to dynamically consume external MCP tools (e.g. GitHub MCP, Prometheus MCP) at startup without modifying code.

### Code References
* [`src/mcp_server/server.py`](../src/mcp_server/server.py#L25-L176)
* [`src/api/main.py`](../src/api/main.py#L41-L53) (lines 41–53)

---

## ADR-09: Single Multi-Stage Docker Image with Pre-Baked Weights

### Status
**Accepted**

### Context & Problem Statement
Packaging four distinct Python application roles (`producer`, `consumer`, `api`, `mcp`) could lead to maintaining four separate Dockerfiles with duplicate dependency installation steps. Furthermore, if container images do not include embedding weights, containers attempt to download model files from the Hugging Face Hub on first startup, leading to deployment delays and failures in air-gapped or restricted production networks.

### Considered Alternatives
1. **Multiple Individual Dockerfiles:** Separate images for producer, consumer, API, and MCP.
2. **Runtime Hugging Face Downloads:** Downloading `all-MiniLM-L6-v2` when the container first boots.
3. **Single Multi-Stage Image with Pre-Baked Weights:** One consolidated build that installs dependencies, pre-downloads the model cache into `/models`, and produces a slim runtime image.

### Decision Outcome
We designed a **single multi-stage `Dockerfile`** that builds a unified image (`streaming-rag:local`).

### Decision Rationales & Trade-Offs
* **Instant Cold Starts:** Model weights are baked into `/models` during `docker build`. Containers start immediately without external internet access.
* **Unified Maintenance & CI/CD Efficiency:** CI/CD builds, scans, and caches a single image layer. The specific service role is determined at runtime by overriding the container `command:` (e.g., in Docker Compose and Helm).
* **Minimal Image Size:** The builder stage compiles C dependencies (`librdkafka-dev`), while the runtime stage copies only clean wheels and cached weights into `python:3.11-slim`, discarding compilation toolchains.

### Code References
* [`Dockerfile`](../Dockerfile#L5-L46)
* [`docker-compose.yml`](../docker-compose.yml#L85-L165)
* [`.github/workflows/ci.yml`](../.github/workflows/ci.yml#L46-L67) (lines 46–67)

---

## ADR-10: Dead Letter Queue (DLQ) and Exponential Backoff Retry Resilience

### Status
**Accepted**

### Context & Problem Statement
In a live Kafka ingestion pipeline, two distinct failure classes can occur:
1. **Poison-Pill / Schema-Violating Payloads:** Malformed JSON or payloads missing mandatory `SystemLog` fields (`ValidationError` / `UnicodeDecodeError`).
2. **Transient Downstream Failures:** Temporary ChromaDB HTTP timeouts or embedding worker contention during vector upsert.

Previously, malformed messages were logged to `stdout` and dropped, losing forensic evidence of producer contract drift, while transient downstream errors failed without retry or dead-letter preservation.

### Considered Alternatives
1. **Drop & Log Only:** Simple, but loses failed payloads permanently and provides no automated recovery from transient vector store blips.
2. **Infinite Consumer Blocking:** Refusing to advance offsets until every message succeeds blocks the entire partition on a single poison-pill payload.
3. **Bounded Exponential Backoff Retries + Dedicated DLQ Topic (`system-logs-dlq`):** Retry transient processing errors with exponential backoff (`1s`, `2s`, ...), and route unparseable or persistently failing records to `system-logs-dlq` with diagnostic Kafka headers before committing the offset.

### Decision Outcome
We implemented **`publish_to_dlq()`** and **bounded exponential backoff retries** in [`src/consumer.py`](../src/consumer.py), aligned with the Java sister project's `DefaultErrorHandler` + `DeadLetterPublishingRecoverer`.

### Decision Rationales & Trade-Offs
* **Zero Poison-Pill Blocking:** Malformed payloads immediately route to `system-logs-dlq` and their offsets are committed, keeping the ingestion pipeline healthy.
* **Forensic Traceability:** Each DLQ message preserves the original key and payload along with structured Kafka headers (`dlq_reason`, `original_topic`, `original_partition`, `original_offset`) for downstream inspection or replay.
* **Transient Fault Tolerance:** Bounded retries (`max_retries=2`, `retry_backoff_sec=1.0`) absorb brief ChromaDB restarts without dropping valid logs.

### Code References
* [`src/consumer.py`](../src/consumer.py#L19-L98) (`publish_to_dlq`, `process_message`)
* [`src/config.py`](../src/config.py#L11-L14) (`KAFKA_TOPIC_SYSTEM_LOGS_DLQ`)

---

## ADR-11: Multi-Archetype Incident Generator and Programmatic Burst API

### Status
**Accepted**

### Context & Problem Statement
Effective evaluation of an SRE ReAct agent requires realistic, domain-specific telemetry across services and log levels, as well as deterministic batch seeding for automated tests and demos. A static single-error mock generator limits root-cause diagnosis scenarios.

### Considered Alternatives
1. **Static Generic Strings:** Fast to write, but prevents meaningful semantic search differentiation across services.
2. **External Log Replay Files Only:** Requires maintaining large static fixture files.
3. **Multi-Archetype Domain Generator + Programmatic Burst API:** Service-specific message templates (`user-auth`, `payment-gateway`, `inventory-service`, `frontend-bff`) with weighted log levels (70% `INFO`, 20% `WARN`, 10% `ERROR`), realistic entity IDs (`usr_...`, `ord_...`, `SKU-...`, IPv4), multi-line stack traces, and `generate_and_send_burst(count)` / `--burst <count>`.

### Decision Outcome
We upgraded [`src/producer.py`](../src/producer.py) to full parity with the Java `MockLogGenerator` and `LogProducerService`.

### Decision Rationales & Trade-Offs
* **Rich Semantic Variety:** Enables realistic SRE queries across authentication failures (LDAP timeouts, JWT expiry), payment gateway faults (Stripe 502s, ledger deadlocks), inventory locking (`OptimisticLockingFailureException`), and BFF circuit-breaker trips.
* **Automated Seeding & CI Ergonomics:** `generate_and_send_burst(count)` and `python src/producer.py --burst N` allow one-shot seeding in scripts and tests without managing infinite loops.

### Code References
* [`src/producer.py`](../src/producer.py#L15-L265)

---

## ADR-12: Enriched Source Citations with Normalized Similarity Scores

### Status
**Accepted**

### Context & Problem Statement
When the ReAct agent answers an incident query using `vector_search`, downstream API consumers and SREs need to know **which exact log event UUID** was cited and **how semantically close** each retrieved chunk was to the query.

### Considered Alternatives
1. **Unscored Metadata Citations:** Return only `service_name`, `log_level`, `timestamp`, and `excerpt`.
2. **Raw L2 Distance Exposure:** Expose raw ChromaDB distance values, which are unbounded $[0, \infty)$ and counter-intuitive (lower is better).
3. **Event ID + Normalized Confidence Score $[0, 1]$:** Store `event_id` in chunk metadata, convert vector distance $d$ via $1 / (1 + \max(0, d))$ into a bounded similarity score in $(0, 1]$, include `event_id` and `score` in tool output headers, and parse them into `LogSource`.

### Decision Outcome
We enriched [`chunk_log()`](../src/chunking.py), [`vector_search`](../src/agent/tools.py), [`mcp_server/server.py`](../src/mcp_server/server.py), [`LogSource`](../src/api/schemas.py), and [`_extract_sources()`](../src/api/routes.py) with `event_id` and normalized `score` while maintaining backward compatibility with legacy header formats.

### Decision Rationales & Trade-Offs
* **End-to-End Traceability:** Every `LogSource` citation in `QueryResponse` carries the exact `event_id` for cross-referencing against Kafka or ChromaDB.
* **Intuitive Relevance Metric:** Normalized scores in $(0.0, 1.0]$ allow UIs and evaluation harnesses to threshold or rank citations consistently with the Java implementation.

### Code References
* [`src/chunking.py`](../src/chunking.py#L13-L27)
* [`src/agent/tools.py`](../src/agent/tools.py#L35-L65)
* [`src/api/schemas.py`](../src/api/schemas.py#L20-L32)
* [`src/api/routes.py`](../src/api/routes.py#L19-L47)
