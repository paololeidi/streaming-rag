# Engineering Roadmap & Production Improvements

This document outlines the detailed technical roadmap designed to transition the project from a functional proof-of-concept into a production-grade, enterprise-ready system. It is structured to demonstrate senior-level software engineering, data streaming scalability, advanced agentic orchestration, and LLMOps evaluation—key dimensions evaluated in AI Engineer and AI Platform roles.

---

## 🔄 Cross-Repository Parity & Alignment Backlog (from Java Sister Project)

The following architectural and functional capabilities exist in the Java implementation ([`streaming-rag-java`](https://github.com/paololeidi/streaming-rag-java)) and have been brought to full parity in Python:

- [x] **P0.1 Multi-Archetype Incident & Realistic Log Generator:**
  - *Previous limitation:* The Python mock producer alternated only between generic INFO logs and a single static ERROR string (`Connection timeout to database for {service}`) with unstructured fake text.
  - *Implemented parity:* `src/producer.py` mirrors `MockLogGenerator.java` with weighted log distribution (70% `INFO`, 20% `WARN`, 10% `ERROR`) and realistic enterprise incident archetypes with authentic diagnostic context and stack traces:
    - `user-auth`: JWT expiry (`TTL 3600s`), OAuth2 multi-factor challenges, brute-force IP rate-limiting (`/oauth/token`), LDAP timeouts (`ldap.corp.internal:636`).
    - `inventory-service`: Optimistic locking failure exceptions (`OptimisticLockingFailureException`) on SKU decrement with multi-line stack traces.
    - `payment-gateway`: Stripe API timeouts, 3D-Secure/CVV retries, and idempotent transaction references (`ord_...`, `usr_...`).
    - `frontend-bff`: Upstream 500/504 gateway timeouts and circuit-breaker alerts.
- [x] **P0.2 Programmatic Burst Log Generation API:**
  - *Previous limitation:* The Python producer only ran as an infinite `while True:` loop.
  - *Implemented parity:* `send_log()`, `generate_and_send()`, and `generate_and_send_burst(count)` (plus CLI `--burst <count>` support) allow programmatic and automated generation of batch test bursts for CI/CD, integration testing, and live demo scenarios.
- [x] **P0.3 Kafka Consumer Dead Letter Queue (DLQ) & Resilience:**
  - *Previous limitation:* Unparseable or poisoned messages were skipped and logged to stdout, but not preserved.
  - *Implemented parity:* `src/consumer.py` implements exponential backoff retries (default 2 retries, 1s base backoff) for transient processing errors and routes corrupted or persistently failing messages via `publish_to_dlq()` to a dedicated Dead Letter Queue topic (`system-logs-dlq`) with diagnostic headers (`dlq_reason`, `original_topic`, `original_partition`, `original_offset`).
- [x] **P0.4 Enriched Source Citations with Similarity Confidence Scores:**
  - *Previous limitation:* Python's `LogSource` schema included only `service_name`, `log_level`, `timestamp`, and `excerpt`.
  - *Implemented parity:* `chunk_log()` persists `event_id` in metadata, `vector_search` (in both `src/agent/tools.py` and `src/mcp_server/server.py`) computes normalized similarity confidence scores (`score = 1 / (1 + distance)`) and emits `event_id` and `score`, and `LogSource` / `_extract_sources` expose `event_id` and `score` alongside backward-compatible legacy header parsing.

---

## 🎯 Strategic Priorities Overview

```
Phase 1: Reliability, Data Integrity & Contract Remediation (Idempotency, Durability, API/Tool Contracts)
   │
   ▼
Phase 2: Software Hardening (Tests, CI/CD, Micro-batching, Concurrency)
   │
   ▼
Phase 3: Advanced Agent Capabilities (Memory, Structured Tools, Multi-MCP)
   │
   ▼
Phase 4: LLMOps & Evaluation (Golden Dataset, Ragas/DeepEval, Guardrails)
   │
   ▼
Phase 5: Streaming & Vector Engine Optimization (Hybrid Search, TTL, Chunking)
   │
   ▼
Phase 6: Interactive Demo & Showcase (Streamlit/Chainlit UI, Live Incident Demo)
```

---

## 🛡️ Phase 1: Reliability, Data Integrity & Contract Remediation

**Goal:** Prioritize end-to-end data integrity, crash durability, and strict API/tool contract enforcement so every advertised reliability guarantee is backed by deterministic verification.

- [ ] **1.1 Idempotent Ingestion & Event Identity Guarantees:**
  - Define strict event identity using `SystemLog.event_id` (with deterministic fallback `kafka-{partition}-{offset}` only when absent), including the conflict policy when a repeated ID arrives with differing content.
  - Ensure `upsert_log()` in `src/vector_store.py` overwrites existing records idempotently without inflating unique event counts upon Kafka consumer redelivery or partition rebalance.
  - Keep operational processing-attempt metrics distinct from counts of unique indexed events.
  - Test repeated writes, concurrent duplicate delivery, replay, and crash recovery after vector store write but before Kafka offset commit.
- [ ] **1.2 Durable Vector Storage Aligned with Kafka Offset Commits:**
  - Establish a strict durable-write boundary in ChromaDB before committing the corresponding Kafka offsets in `src/consumer.py`.
  - Propagate indexing and persistence errors so failed or partially written records are never acknowledged to Kafka before retry/DLQ handling.
  - Preserve durability-before-commit ordering when micro-batching is introduced, and add abrupt-process-termination recovery tests against persistent disk storage.
- [ ] **1.3 Enforceable Request `k` Parameter Contract:**
  - *Current limitation:* `QueryRequest.k` is passed into `config={"configurable": {"k": body.k}}`, but `vector_search(query: str, k: int = 5)` is invoked by the LLM with its own default or chosen `k`, so the HTTP request parameter does not strictly govern retrieval or response citations.
  - *Target remediation:* Bind or clamp the tool's effective retrieval limit and final `QueryResponse.sources` list to the request's validated `k` parameter (`1 <= k <= 20`) across both synchronous (`/api/v1/query`) and streaming (`/api/v1/query/stream`) execution paths, and apply consistent bounds to MCP tool calls.
- [ ] **1.4 Truthful Time-Windowed Statistics & Dynamic Service Discovery:**
  - Ensure `log_stats` computes totals, severity breakdowns, and service breakdowns over the exact same filtered event set within the sliding UTC cutoff (`now - timedelta(minutes=minutes)`).
  - Define explicit UTC cutoff boundaries, late/future event timestamp handling, and the distinction between event timestamp and ingestion time.
  - Inject a deterministic clock/time provider in tests to verify cutoff boundary conditions, combined filters, unknown services, and duplicate deliveries.
- [ ] **1.5 Request-Scoped Evidence Preservation Across Multi-Tool Calls:**
  - Define `QueryResponse.sources` clearly as retrieved evidence (or verified citations), accumulating and deduplicating records by `event_id` across multiple `vector_search` calls within a single investigation.
  - Ensure an empty follow-up search does not wipe or corrupt earlier valid evidence, and report missing/malformed metadata explicitly rather than fabricating fallback values.
  - Isolate per-request evidence state across concurrent async requests and clean up context on completion, error, timeout, or cancellation.
- [ ] **1.6 Genuine Token Streaming & Client Disconnect Lifecycle:**
  - Ensure `POST /api/v1/query/stream` streams genuine model tokens from the final synthesis pass without emitting intermediate tool-call JSON fragments or artificial buffered delays.
  - Define explicit terminal (`data: [DONE]`) and error event contracts, handle client disconnects and timeouts cleanly, and cancel outstanding model/graph execution when the HTTP client disconnects.
- [ ] **1.7 Real Pipeline Integration & Runtime Baseline Verification:**
  - Separate mocked unit wiring tests from real pipeline integration tests that exercise Kafka -> SentenceTransformers (`all-MiniLM-L6-v2`) -> persistent ChromaDB -> retrieval of the exact produced `event_id`.
  - Require the real integration suite in Docker-enabled CI so infrastructure or listener regressions never silently pass, and pin the tested Python runtime range (`3.11`–`3.12`) in CI.

---

## 🛠️ Phase 2: Software Engineering Hardening & Reliability

**Goal:** Eliminate single points of failure, ensure code correctness through automated testing, and optimize ingestion throughput.

- [x] **2.1 Baseline Test Suite (`pytest` + `unittest.mock`):**
  - **Unit Tests:**
    - Test `SystemLog` Pydantic validation (valid logs, missing required fields, corrupted payloads, ISO timestamp parsing).
    - Test `chunk_log()` transformation and metadata dictionary consistency.
    - Test `agent.prompts` formatting and system prompt assembly.
    - Mock ChromaDB client to unit test `vector_search` and `log_stats` tool logic in isolation.
  - **Integration Tests:**
    - Test FastAPI endpoints (`/health`, `/api/v1/query`, `/api/v1/query/stream`) using `httpx.AsyncClient` or FastAPI `TestClient`.
    - End-to-end ingestion test: push sample message to mock consumer -> chunk -> embed -> verify vector store insertion.
  - **Remaining verification gaps:** Add real Kafka-to-SentenceTransformers-to-ChromaDB integration coverage, full HTTP contract tests, and question-to-evidence agent evaluations in items 1.7, 2.5, and 4.1–4.2.
- [x] **2.2 CI/CD Pipeline (GitHub Actions):**
  - Create `.github/workflows/ci.yml` executing on every pull request and push to `main`.
  - Stages:
    1. Code quality & linting: `ruff check .` and `ruff format --check .`.
    2. Static type checking: `mypy src/`.
    3. Automated test execution: `pytest --cov=src tests/`.
    4. Docker build validation: verify that application containers build successfully.
- [ ] **2.3 Kafka Consumer Micro-Batching & Throughput Optimization:**
  - *Current limitation:* Consuming and embedding logs individually ($O(1)$ synchronous calls) bottlenecks ingestion throughput to ~10–30 logs/sec.
  - *Target architecture:*
    - Buffer incoming logs into micro-batches (e.g., maximum batch size of 50–100 messages or a 200ms time window).
    - Vectorize the entire batch using `embedder.embed_batch()` leveraging vectorized tensor operations.
    - Execute a single batch `collection.upsert()` in ChromaDB.
    - Commit Kafka offsets asynchronously per batch instead of synchronously per message.
- [x] **2.4 Error Handling & Resilience (Dead Letter Queue):**
  - Implemented a Dead Letter Queue (DLQ) topic (`system-logs-dlq`) with diagnostic Kafka headers (`dlq_reason`, `original_topic`, `original_partition`, `original_offset`) for unparseable or schema-violating messages.
  - Added exponential backoff retry logic (`max_retries=2`, `retry_backoff_sec=1.0`) for transient Vector DB or embedding failures before DLQ routing.
- [ ] **2.5 Question-to-Evidence End-to-End Tests:**
  - After adding real pipeline integration coverage, seed a small, controlled incident and submit a representative question through the query API.
  - Assert that retrieval returns the expected event IDs and that the answer reflects required evidence and key facts; do not compare generated prose to an exact answer string.
  - Include ambiguous and contradictory evidence cases, such as a log message saying "insufficient funds" whose exception stack trace reports a socket timeout. Require the answer to distinguish the message from the exception, acknowledge conflicts, and avoid asserting an unproven single root cause.
  - Verify citation IDs resolve to retrieved records and that unsupported details are not presented as established facts. Keep deterministic retrieval/API assertions in CI and place live-model variability in the evaluation suite.
- [ ] **2.6 Concurrency Hardening & CPU-Bound Inference Isolation:**
  - **Python (`asyncio` Event Loop):** Offload synchronous CPU-bound SentenceTransformers embedding calls (`embedder.embed_text()`) in async API/MCP query paths via `asyncio.to_thread()` or a bounded `ProcessPoolExecutor` / `ThreadPoolExecutor` so the Uvicorn event loop is never blocked during burst traffic.
  - **JNI Pinning & CPU-Bound Inference Caveat (Interview Deep-Dive):** While Virtual Threads excel at I/O-bound waiting, executing in-process **ONNX Runtime via JNI** enters a native C++ frame, which **pins** the Virtual Thread to its underlying OS **Carrier Thread** (preventing unmount) while performing **CPU-bound** matrix math. Under burst traffic, if dozens of Virtual Threads invoke JNI ONNX inference simultaneously, they can pin and saturate all Carrier Threads in the `ForkJoinPool`, causing **Carrier Thread starvation** and delaying lightweight HTTP/SSE endpoints. Mitigation: keep Virtual Threads for I/O orchestration, gate concurrent JNI ONNX calls behind a `Semaphore` (bulkhead) or a dedicated bounded platform thread pool, and batch embeddings during ingestion.

---

## 🧠 Phase 3: Advanced Agentic Architecture & Orchestration

**Goal:** Upgrade the LangGraph agent from a stateless ReAct loop into a stateful, robust investigative assistant integrated with multiple external systems.

- [ ] **3.1 Multi-Turn Conversational Memory & State Checkpointing:**
  - Introduce session identification (`thread_id` / `session_id`) in `QueryRequest`.
  - Configure LangGraph with a checkpointer (`MemorySaver` for in-memory or Redis/PostgreSQL for persistent session state).
  - Enable conversational investigations (e.g., User: *"What failed?"* -> Agent: *"Payment service timed out"* -> User: *"Can you show me the stack trace for the third occurrence?"*).
- [ ] **3.2 Structured Tool Outputs & Eliminating Regex Parsing:**
  - *Current limitation:* Tool output is formatted as plain text with header prefixes (`[1] LEVEL | service @ ts`), which the API then reverse-engineers with regex `_SOURCE_HEADER`.
  - *Target architecture:* Return structured artifacts (`ToolMessage(artifact=...)` or Pydantic models) directly from tools, allowing the API route to cleanly extract typed `LogSource` models without fragile string parsing.
- [ ] **3.3 Multi-Server MCP Integration (Autonomous Root-Cause Analysis):**
  - Connect the LangGraph agent to external MCP servers alongside the native log tools:
    - **GitHub MCP Server:** Inspect recent git commits, PRs, and active releases to correlate log spikes with recent code changes.
    - **Prometheus / Metrics MCP Server:** Correlate application error spikes with infrastructure metrics (CPU load, memory pressure, connection pool exhaustion).
    - **Slack MCP Server:** Enable the agent to broadcast incident summaries directly to on-call incident channels.
  - *Demo Scenario:* Agent detects DB connection timeouts -> checks Prometheus MCP to see DB pool saturation -> queries GitHub MCP to discover a recent PR that lowered timeout thresholds -> provides root cause and suggests rollback.
- [ ] **3.4 Query Routing & Fallbacks:**
  - Implement query preprocessing (e.g., detecting whether a query is purely statistical, purely semantic, or conversational).
  - Add fallback logic when the local LLM struggles with tool calling (retry mechanism with self-correction prompt).

---

## 📊 Phase 4: LLMOps, Evaluation & Quality Assurance

**Goal:** Establish systematic quantitative metrics to measure retrieval accuracy, hallucination prevention, and agent answer quality.

- [ ] **4.1 Incident Golden Benchmark Dataset:**
  - Create a curated dataset (`tests/evals/golden_dataset.json`) containing 20+ realistic incident scenarios across microservices.
  - Fields per scenario:
    - `query`: The user question.
    - `relevant_log_ids`: Ground truth log entries that must be retrieved.
    - `expected_facts`: Required evidence-backed facts and acceptable root-cause conclusions, rather than one exact answer string.
    - `unsupported_claims`: Conclusions the evidence does not support and the answer must not assert.
    - `conflicting_evidence`: Optional competing log fields or records the answer should distinguish or qualify.
    - `expected_tools`: The tools that should have been invoked (`vector_search`, `log_stats`, etc.).
- [ ] **4.2 Deterministic Agent & RAG Evaluation Pipeline:**
  - Add reproducible tests with a stubbed LLM to verify query routing, expected tool calls, evidence propagation, citation mapping, and API response contracts against controlled fixtures. These tests validate orchestration and evidence handling, not the semantic quality of a live model's generated answer.
  - Score retrieval independently with Hit Rate @ K, Recall @ K, and Mean Reciprocal Rank so retrieval regressions are separable from generation variability.
  - Use fixed fixture answers where deterministic answer assembly needs coverage; compare against semantic criteria or structured results, never exact prose from a generative model.
  - Expose an automated evaluation runner (`scripts/run_evals.py`) that generates a machine-readable result and concise summary report.
- [ ] **4.3 Optional Live-Model Evaluation & Quality Gates:**
  - Run a small curated question set against the configured Ollama model as a separate, opt-in or scheduled evaluation, not as a timing-sensitive unit test on every CI run.
  - Record model name/version, prompt/configuration, retrieved evidence, answer, latency, and per-criterion scores to make regressions diagnosable and comparisons reproducible.
  - Evaluate whether required facts are present, claims are grounded in retrieved evidence, conflicts and uncertainty are represented, citations point to relevant records, and unsupported claims are avoided. Use semantic criteria or a calibrated evaluator; do not require exact answer wording.
  - Consider an evaluation framework (e.g., Ragas or DeepEval) for context precision/recall, faithfulness, and answer relevance, while keeping evaluator variability visible in results.
  - Define explicit acceptance thresholds for evidence coverage, citation relevance, unsupported-claim rate, and correct handling of ambiguous evidence before using results as a release gate.
- [ ] **4.4 Observability & LangSmith Tracing Dashboard:**
  - Maintain comprehensive trace logging for:
    - Token consumption, tool execution latencies, LLM invocation duration.
    - Agent decision trajectories (ReAct reasoning steps).

---

## ⚡ Phase 5: Streaming, Data Ingestion & Vector Optimization

**Goal:** Enhance log generation realism, optimize data retention, and improve search relevance across metadata dimensions.

- [ ] **5.1 Scenario-Driven Log Generation & Multi-Incident Simulation:**
  - *Note:* Multi-archetype service messages (`user-auth`, `payment-gateway`, `inventory-service`, `frontend-bff`), realistic entity IDs (`usr_...`, `ord_...`, `SKU-...`, IPv4), weighted log levels, and realistic multi-line stack traces are now implemented in `src/producer.py` (see **P0.1** & **P0.2**).
  - *Remaining target architecture:*
    - **Enterprise Schema Enrichment:** Add `trace_id`, `span_id`, `environment` (`prod`/`staging`), `region` (`us-east-1`/`eu-west-1`), and HTTP context (`status_code`, `http_method`, `endpoint`, `duration_ms`, `customer_id`).
    - **Correlated Multi-Service Incident Profiles:** Trigger synchronized multi-service cascading failure timelines with shared `trace_id`.
- [ ] **5.2 Advanced Chunking Strategies:**
  - Experiment with dynamic chunking:
    - Window-based aggregation: group consecutive logs from the same service/trace within a 10-second window into a single chunk.
    - Stack trace segmentation: split extensive stack traces into structured chunks while preserving the top-level exception header.
- [ ] **5.3 Vector Store Lifecycle & Retention Policy (TTL):**
  - Implement a background retention worker to prune logs older than a configurable retention period (e.g., 24 hours), avoiding unbounded vector index growth.
- [ ] **5.4 Hybrid Search (Dense Embeddings + Sparse BM25):**
  - Log analysis frequently requires exact keyword lookups (error codes like `ERR_503_CONN_RESET`, specific IP addresses, or exact UUIDs) which dense embeddings can miss.
  - Implement hybrid search combining dense semantic similarity with sparse BM25/lexical retrieval.

---

## 🖥️ Phase 6: Interactive Demo & Showcase Experience

**Goal:** Deliver an intuitive, visually striking interface and reproducible presentation materials tailored for live demos and technical interviews.

- [ ] **6.1 Interactive Web UI (Streamlit or Chainlit):**
  - **Split-View Dashboard:**
    - *Left Pane:* Live real-time log ingestion stream with filtering by log level and visual alerts on `ERROR`.
    - *Right Pane:* Interactive chat interface featuring:
      - Real-time token streaming via SSE (`/api/v1/query/stream`).
      - Visual expander widgets revealing the agent's thought process, tool invocations, and raw source documents.
      - Service health statistics widgets (error rate breakdown).
- [ ] **6.2 End-to-End Incident Demo Script ("The Golden Path"):**
  - Document a guided 3-minute walkthrough script:
    1. Trigger a synthetic cascading failure via a script command.
    2. Watch real-time logs flood the Kafka stream and Chroma collection.
    3. Query the agent: *"Why did payment-service fail around 14:00?"*.
    4. Observe the agent execute `log_stats` to detect the error spike, followed by `vector_search` to pinpoint the database timeout.
    5. Follow up with conversational memory: *"Which specific customer transactions were impacted?"*.
- [ ] **6.3 Presentation & Visual Portfolio Assets:**
  - Record a 90-second high-resolution demo video / GIF showcasing the interactive UI, SSE streaming, and MCP interaction.
  - Include an interactive system architecture diagram in `README.md`.
