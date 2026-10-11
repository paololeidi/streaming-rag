# Engineering Roadmap & Production Improvements

This document outlines the detailed technical roadmap designed to transition the project from a functional proof-of-concept into a production-grade, enterprise-ready system. It is structured to demonstrate senior-level software engineering, data streaming scalability, advanced agentic orchestration, and LLMOps evaluation—key dimensions evaluated in AI Engineer and AI Platform roles.

---

## 🎯 Strategic Priorities Overview

```
Phase 1: Software Hardening (Tests, CI/CD, Micro-batching)
   │
   ▼
Phase 2: Advanced Agent Capabilities (Memory, Structured Tools, Multi-MCP)
   │
   ▼
Phase 3: LLMOps & Evaluation (Golden Dataset, Ragas/DeepEval, Guardrails)
   │
   ▼
Phase 4: Streaming & Vector Engine Optimization (Hybrid Search, TTL, Chunking)
   │
   ▼
Phase 5: Interactive Demo & Showcase (Streamlit/Chainlit UI, Live Incident Demo)
```

---

## 🛠️ Phase 1: Software Engineering Hardening & Reliability

**Goal:** Eliminate single points of failure, ensure code correctness through automated testing, and optimize ingestion throughput.

- [x] **1.1 Comprehensive Test Suite (`pytest`):**
  - **Unit Tests:**
    - Test `SystemLog` Pydantic validation (valid logs, missing required fields, corrupted payloads, ISO timestamp parsing).
    - Test `chunk_log()` transformation and metadata dictionary consistency.
    - Test `agent.prompts` formatting and system prompt assembly.
    - Mock ChromaDB client to unit test `vector_search` and `log_stats` tool logic in isolation.
  - **Integration Tests:**
    - Test FastAPI endpoints (`/health`, `/api/v1/query`, `/api/v1/query/stream`) using `httpx.AsyncClient` or FastAPI `TestClient`.
    - End-to-end ingestion test: push sample message to mock consumer -> chunk -> embed -> verify vector store insertion.
- [x] **1.2 CI/CD Pipeline (GitHub Actions):**
  - Create `.github/workflows/ci.yml` executing on every pull request and push to `main`.
  - Stages:
    1. Code quality & linting: `ruff check .` and `ruff format --check .`.
    2. Static type checking: `mypy src/`.
    3. Automated test execution: `pytest --cov=src tests/`.
    4. Docker build validation: verify that application containers build successfully.
- [ ] **1.3 Kafka Consumer Micro-Batching & Throughput Optimization:**
  - *Current limitation:* Consuming and embedding logs individually ($O(1)$ synchronous calls) bottlenecks ingestion throughput to ~10–30 logs/sec.
  - *Target architecture:*
    - Buffer incoming logs into micro-batches (e.g., maximum batch size of 50–100 messages or a 200ms time window).
    - Vectorize the entire batch using `embedder.embed_batch()` leveraging vectorized tensor operations.
    - Execute a single batch `collection.upsert()` in ChromaDB.
    - Commit Kafka offsets asynchronously per batch instead of synchronously per message.
- [ ] **1.4 Error Handling & Resilience (Dead Letter Queue):**
  - Implement a Dead Letter Queue (DLQ) topic (e.g., `system-logs-dlq`) for unparseable or schema-violating messages.
  - Add exponential backoff retry logic for transient Vector DB or network failures.
  - Add structured consumer startup logs with readiness polling to provide clear feedback while dependencies initialize.

---

## 🧠 Phase 2: Advanced Agentic Architecture & Orchestration

**Goal:** Upgrade the LangGraph agent from a stateless ReAct loop into a stateful, robust investigative assistant integrated with multiple external systems.

- [ ] **2.1 Multi-Turn Conversational Memory & State Checkpointing:**
  - Introduce session identification (`thread_id` / `session_id`) in `QueryRequest`.
  - Configure LangGraph with a checkpointer (`MemorySaver` for in-memory or Redis/PostgreSQL for persistent session state).
  - Enable conversational investigations (e.g., User: *"What failed?"* -> Agent: *"Payment service timed out"* -> User: *"Can you show me the stack trace for the third occurrence?"*).
- [ ] **2.2 Structured Tool Outputs & Eliminating Regex Parsing:**
  - *Current limitation:* Tool output is formatted as plain text with header prefixes (`[1] LEVEL | service @ ts`), which the API then reverse-engineers with regex `_SOURCE_HEADER`.
  - *Target architecture:* Return structured artifacts (`ToolMessage(artifact=...)` or Pydantic models) directly from tools, allowing the API route to cleanly extract typed `LogSource` models without fragile string parsing.
- [ ] **2.3 Multi-Server MCP Integration (Autonomous Root-Cause Analysis):**
  - Connect the LangGraph agent to external MCP servers alongside the native log tools:
    - **GitHub MCP Server:** Inspect recent git commits, PRs, and active releases to correlate log spikes with recent code changes.
    - **Prometheus / Metrics MCP Server:** Correlate application error spikes with infrastructure metrics (CPU load, memory pressure, connection pool exhaustion).
    - **Slack MCP Server:** Enable the agent to broadcast incident summaries directly to on-call incident channels.
  - *Demo Scenario:* Agent detects DB connection timeouts -> checks Prometheus MCP to see DB pool saturation -> queries GitHub MCP to discover a recent PR that lowered timeout thresholds -> provides root cause and suggests rollback.
- [ ] **2.4 Query Routing & Fallbacks:**
  - Implement query preprocessing (e.g., detecting whether a query is purely statistical, purely semantic, or conversational).
  - Add fallback logic when the local LLM struggles with tool calling (retry mechanism with self-correction prompt).

---

## 📊 Phase 3: LLMOps, Evaluation & Quality Assurance

**Goal:** Establish systematic quantitative metrics to measure retrieval accuracy, hallucination prevention, and agent answer quality.

- [ ] **3.1 Incident Golden Benchmark Dataset:**
  - Create a curated dataset (`tests/evals/golden_dataset.json`) containing 20+ realistic incident scenarios across microservices.
  - Fields per scenario:
    - `query`: The user question.
    - `relevant_log_ids`: Ground truth log entries that must be retrieved.
    - `expected_answer`: Reference root-cause explanation.
    - `expected_tools`: The tools that should have been invoked (`vector_search`, `log_stats`, etc.).
- [ ] **3.2 Automated RAG & Agent Evaluation Pipeline:**
  - Integrate an LLM evaluation framework (e.g., **Ragas** or **DeepEval**):
    - **Context Precision & Recall:** Evaluate retrieval quality (Hit Rate @ K, MRR).
    - **Faithfulness:** Quantify hallucination by verifying that claims in the answer are grounded strictly in retrieved logs.
    - **Answer Relevance:** Measure how directly the answer addresses the incident prompt.
  - Expose an evaluation script (`scripts/run_evals.py`) generating a markdown summary report.
- [ ] **3.3 Observability & LangSmith Tracing Dashboard:**
  - Maintain comprehensive trace logging for:
    - Token consumption, tool execution latencies, LLM invocation duration.
    - Agent decision trajectories (ReAct reasoning steps).

---

## ⚡ Phase 4: Streaming, Data Ingestion & Vector Optimization

**Goal:** Enhance log generation realism, optimize data retention, and improve search relevance across metadata dimensions.

- [ ] **4.1 Richer Distributed Log Generation:**
  - Expand `SystemLog` schema with enterprise distributed tracing metadata:
    - `trace_id` and `span_id` (enabling correlation across microservices for a single user journey).
    - `environment` (`production`, `staging`), `region` (`us-east-1`, `eu-west-1`), `cluster_id`.
    - HTTP context (`status_code`, `http_method`, `endpoint`, `duration_ms`, `customer_id`).
  - Upgrade mock producer to simulate cascading failures (e.g., Auth service slows down -> Payment service retries -> Database runs out of connections).
- [ ] **4.2 Advanced Chunking Strategies:**
  - Experiment with dynamic chunking:
    - Window-based aggregation: group consecutive logs from the same service/trace within a 10-second window into a single chunk.
    - Stack trace segmentation: split extensive stack traces into structured chunks while preserving the top-level exception header.
- [ ] **4.3 Vector Store Lifecycle & Retention Policy (TTL):**
  - Implement a background retention worker to prune logs older than a configurable retention period (e.g., 24 hours), avoiding unbounded vector index growth.
- [ ] **4.4 Hybrid Search (Dense Embeddings + Sparse BM25):**
  - Log analysis frequently requires exact keyword lookups (error codes like `ERR_503_CONN_RESET`, specific IP addresses, or exact UUIDs) which dense embeddings can miss.
  - Implement hybrid search combining dense semantic similarity with sparse BM25/lexical retrieval.

---

## 🖥️ Phase 5: Interactive Demo & Showcase Experience

**Goal:** Deliver an intuitive, visually striking interface and reproducible presentation materials tailored for live demos and technical interviews.

- [ ] **5.1 Interactive Web UI (Streamlit or Chainlit):**
  - **Split-View Dashboard:**
    - *Left Pane:* Live real-time log ingestion stream with filtering by log level and visual alerts on `ERROR`.
    - *Right Pane:* Interactive chat interface featuring:
      - Real-time token streaming via SSE (`/api/v1/query/stream`).
      - Visual expander widgets revealing the agent's thought process, tool invocations, and raw source documents.
      - Service health statistics widgets (error rate breakdown).
- [ ] **5.2 End-to-End Incident Demo Script ("The Golden Path"):**
  - Document a guided 3-minute walkthrough script:
    1. Trigger a synthetic cascading failure via a script command.
    2. Watch real-time logs flood the Kafka stream and Chroma collection.
    3. Query the agent: *"Why did payment-service fail around 14:00?"*.
    4. Observe the agent execute `log_stats` to detect the error spike, followed by `vector_search` to pinpoint the database timeout.
    5. Follow up with conversational memory: *"Which specific customer transactions were impacted?"*.
- [ ] **5.3 Presentation & Visual Portfolio Assets:**
  - Record a 90-second high-resolution demo video / GIF showcasing the interactive UI, SSE streaming, and MCP interaction.
  - Include an interactive system architecture diagram in `README.md`.