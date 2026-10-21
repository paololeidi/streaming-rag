# AGENT RUNBOOK & ONBOARDING GUIDE: Streaming RAG

This document is an operational guide designed for **AI Coding Assistants, LLM Agents, and Technical Navigators** (e.g., Cursor, Claude Code, Copilot, Antigravity, Aider).
When a user asks to run, test, inspect, or troubleshoot the **Streaming RAG** project, follow the rules, workflows, and diagnostic recipes described here.

---

## 1. Agent Persona & Guiding Principles

1. **Ask Path Preference Upfront, Then Execute Autonomously (Minimum User Effort)**:
   - When the user asks to run, setup, or test the project, ask **one single upfront question**:
     > *"Would you like to run the project via **Path A (Full Docker Compose - zero local setup)** or **Path B (Local Python Development - hybrid)**?"*
   - As soon as the user selects a path, **take complete autonomous control**: do NOT pause or ask for confirmation at every intermediate step.
   - Execute all prerequisite checks, file creations, service launches, and healthchecks in the background until the system reaches a **verified, working state**.
   - Present the finished result directly to the user with live endpoints and test queries.
   - Intervene or ask further questions ONLY if hitting an external, unresolvable blocker (e.g. Docker daemon is not running, host out of memory).
2. **Respect Long-Running vs. One-Shot Processes**:
   - The Producer (in default stream mode), Consumer, Kafka Broker, Ollama LLM, FastAPI server, and MCP server are **daemon/long-running services**. (Note: `python src/producer.py --burst 20` can also be used as a fast one-shot command to seed 20 logs immediately).
   - Always run long-running services in detached/background mode (e.g., `docker compose up -d` or managed background tasks) so the agent turn is never blocked.
3. **OS-Aware Execution (Windows vs. Unix)**:
   - On Windows PowerShell:
     - Activate venv with `.\.venv\Scripts\Activate.ps1` (or invoke `.\.venv\Scripts\python.exe` directly).
     - Always use `python -m pip install ...` instead of raw `pip install` to prevent `pip.exe` file-locking errors.
     - When using `curl`, be aware that PowerShell aliases `curl` to `Invoke-WebRequest` unless calling `curl.exe` or using `Invoke-RestMethod`.
   - On POSIX (Linux / macOS / Git Bash):
     - Activate venv with `source .venv/bin/activate`.
     - Standard `curl` works out of the box.
4. **Step-by-Step Progress Tracking & Final Status Report**:
   - While executing setup steps autonomously, output compact, transparent progress checkpoints so the user can follow along (e.g., `[1/4] Environment Config: OK`, `[2/4] Service Startup: OK`, etc.).
   - As soon as the stack is running and healthy (in Path A or Path B), **always compile and deliver the Final Setup Status Report** (defined in Section 6) containing the component status table, data pipeline check, live URLs, and sample queries ready to execute.
5. **Mandatory End-to-End Validation & User Notice on LLM Latency**:
   - The agent MUST NOT stop at merely launching processes. It must automatically verify the `/health` endpoint (`{"status": "ok"}`) AND execute a live verification query against `POST /api/v1/query` to confirm the entire agentic loop (FastAPI + LangGraph + Ollama + ChromaDB) works properly.
   - **Inform the User Upfront**: Before/during running the test query, the agent MUST explicitly inform the user that this verification query can take noticeable time (typically 30–90+ seconds, especially on CPU hardware without a dedicated GPU) because Ollama may need to load model weights into RAM (cold start) and the ReAct agent executes multiple inference passes (tool selection, retrieval, synthesis). This sets expectations and prevents the user from assuming the application is stuck.

---

## 2. System Architecture & Port Allocation

```
 ┌──────────────────────── Data Pipeline ─────────────────────────┐
 │                                                                │
 │  Producer ──► Kafka ──► Consumer ──► HF Embedder ──► ChromaDB  │
 │  (stream / --burst)     │ (retries + DLQ: system-logs-dlq)     │
 └────────────────────────────────────────────┬───────────────────┘
                                              │
 ┌──────────────────────── Agentic Layer ─────▼────────────────────┐
 │                                                                 │
 │  POST /api/v1/query                                             │
 │         │                                                       │
 │         ▼                                                       │
 │  LangGraph ReAct Agent (Ollama · llama3.2:3b)                   │
 │         ├── vector_search ──► ChromaDB  (native tool + score)   │
 │         ├── log_stats    ──► ChromaDB  (native tool)            │
 │         └── [MCP tools]  ──► MCP Server (optional, SSE)         │
 └─────────────────────────────┬───────────────────────────────────┘
                               │
            ┌──────────────────▼──────────────────┐
            │             MCP Server              │
            │  (src/mcp_server/server.py)         │
            │  ├── vector_search                  │
            │  └── log_stats                      │
            └─────────────────────────────────────┘
```

| Service | Port | Protocol | Purpose |
|---|---|---|---|
| **FastAPI Backend** | `8000` | HTTP / SSE | REST API (`/api/v1/query`), Stream (`/api/v1/query/stream`), Swagger (`/docs`) |
| **MCP Server** | `8001` | HTTP / SSE | Model Context Protocol SSE endpoint (`/sse`) |
| **Kafka Broker** | `9092` | TCP (KRaft) | Log event streaming pipeline (`system-logs` & `system-logs-dlq`) |
| **Ollama Runtime** | `11434` | HTTP | Local LLM inference engine (`llama3.2:3b`) |
| **ChromaDB** | Internal (`8000`) or Embedded | SQLite / HTTP | Vector store (`data/chroma/` in local mode) |

---

## 3. Decision Matrix: Initial User Selection

When the user asks to run or test the project, prompt them to choose their target mode, then immediately execute that path autonomously:

```
Step 0: Ask User Preference
 ├── Option 1: "Path A (Docker Compose - Recommended for fast testing)"
 │    └──► Agent executes PATH A autonomously to completion -> Delivers working endpoints
 │
 └── Option 2: "Path B (Local Python Development - For code inspection / editing)"
      └──► Agent executes PATH B autonomously to completion -> Delivers working endpoints
```

---

## 4. PATH A: Full-Stack Docker Compose (Zero Friction)

Best for rapid evaluation and testing without local Python dependency management.

### Step A.1: Configuration Setup
Verify if `.env` exists. If not, copy from `.env.example`:
```bash
# Windows PowerShell
if (-not (Test-Path .env)) { Copy-Item .env.example .env }

# Unix / Bash
[ ! -f .env ] && cp .env.example .env
```

### Step A.2: Start All Services Detached
```bash
docker compose up --build -d
```
*Note: The first run downloads the base images and the Ollama model (`llama3.2:3b`). Subsequent starts are instant due to Docker volumes.*

### Step A.3: Health & Readiness Verification
Poll until the API container reports healthy:
```bash
# cURL
curl -sf http://localhost:8000/health

# PowerShell
(Invoke-RestMethod -Uri http://localhost:8000/health).status
```
Expected output: `{"status": "ok"}`.

### Step A.4: Check Streaming Pipeline Logs
Verify that synthetic logs are flowing through Kafka and ChromaDB:
```bash
# Check producer
docker compose logs -n 20 producer

# Check consumer
docker compose logs -n 20 consumer
```

### Step A.5: End-to-End Query Verification & Delivery (Manage Latency Expectations)
As soon as `/health` returns `ok` and logs are verified:
1. **Inform User**: Notify the user that an end-to-end test query is being executed to verify the full stack, explicitly mentioning: *"Testing `/api/v1/query`. Please note that this may take 30–90 seconds while Ollama loads the model weights into RAM and the ReAct agent runs its reasoning loop."*
2. **Execute Test Query**: Run a verification query against `POST /api/v1/query` and verify a valid JSON answer is returned.
3. **Deliver Status Report**: Compile and deliver the structured **Final Setup Status Report** (as specified in Section 6).
4. **Offer Interactive Testing**: Suggest trying the streaming endpoint (`/api/v1/query/stream`) or Swagger UI for further tests.

### Step A.6: Teardown
When the user finishes testing:
```bash
docker compose down -v
```

---

## 5. PATH B: Local Hybrid Development (Step-by-Step)

Best for code editing, unit tests, and debugging. Infrastructure runs in Docker; Python code runs on the host.

### Step B.1: Virtual Environment & Dependencies
```powershell
# Windows PowerShell
python -m venv .venv
.\.venv\Scripts\Activate.ps1
python -m pip install --upgrade pip
python -m pip install -r requirements.txt
python -m pip install -r requirements-dev.txt
```
```bash
# Unix / macOS / Bash
python3 -m venv .venv
source .venv/bin/activate
python -m pip install --upgrade pip
python -m pip install -r requirements.txt
python -m pip install -r requirements-dev.txt
```

### Step B.2: Start Kafka (KRaft mode)
```bash
docker compose -f infrastructure/kafka/docker-compose.yml up -d
```
Verify port `9092` is accessible.

### Step B.3: Ensure Ollama is Ready
Ensure Ollama is running locally and has the required model:
```bash
ollama list
# If llama3.2:3b is missing:
ollama pull llama3.2:3b
```

### Step B.4: Seed Log Data (Producer)
Seed a burst of logs immediately or run the continuous stream in the background:
```bash
# One-shot burst of 20 synthetic logs:
python src/producer.py --burst 20

# Or run continuous stream in background / separate terminal:
python src/producer.py
```

### Step B.5: Consume & Vectorize Logs (Consumer)
The local consumer reads from Kafka and embeds vectors into SQLite (`data/chroma/`):
```bash
python src/consumer.py
```
*In local mode, ChromaDB runs embedded in-process via `PersistentClient`. No standalone Chroma container is needed.*

### Step B.6: Launch the FastAPI Application
```bash
uvicorn api.main:app --reload --app-dir src --port 8000
```
Swagger UI will be live at: `http://localhost:8000/docs`.

### Step B.7: Model Context Protocol (MCP) Server (Optional)
The MCP server defaults to **`stdio` mode** (matching `python src/mcp_server/server.py`):

- **Default (stdio mode for Cursor / Claude Desktop)**:
  Desktop AI clients launch the script directly as a subprocess via standard input/output. You typically configure it in the client settings rather than running it interactively:
  ```json
  "mcpServers": {
    "log-analysis": {
      "command": "python",
      "args": ["src/mcp_server/server.py"]
    }
  }
  ```
- **HTTP / SSE Mode (only if testing via browser or MCP Inspector)**:
  Run explicitly with `--sse` to expose the HTTP server on port `8001`:
  ```bash
  python src/mcp_server/server.py --sse
  ```

### Step B.8: Health Check, End-to-End Test Query & Status Delivery
Once FastAPI is running and ChromaDB has ingested initial log vectors:
1. **Health Verification**: Check that `GET /health` returns `{"status": "ok"}`.
2. **Inform User on Latency**: Inform the user: *"Executing an end-to-end verification query against `/api/v1/query`. Note: This can take 30–90 seconds (longer on CPU systems without a dedicated GPU) while Ollama loads the model weights into RAM and the LangGraph ReAct agent runs multi-pass reasoning."*
3. **Run Test Query**: Execute a verification query against `POST /api/v1/query` and confirm that a structured answer is received.
4. **Compile & Deliver Report**: Compile and print the **Final Setup Status Report** (as specified in Section 6).
5. **Offer Interactive Testing**: Recommend the SSE streaming endpoint (`/api/v1/query/stream`) or Swagger UI for smooth testing.

---

## 6. Setup Progress Tracking & Final Status Report Specification

Whenever the agent sets up the environment (Path A or Path B), it must:
1. **Provide live progress checkpoints** during execution:
   ```text
   ⏳ [1/4] Environment Configuration (.env, prerequisites)... OK
   ⏳ [2/4] Service Startup (Kafka, Ollama, ChromaDB, FastAPI)... OK
   ⏳ [3/4] Ingestion Pipeline Verification (Producer -> Consumer)... OK
   ⏳ [4/4] End-to-End Validation (/health OK & query test verified)... OK
   ```
   *(Note: While executing checkpoint 4, always alert the user that running the test query may take 30–90 seconds due to Ollama loading the model and CPU reasoning cycles).*
2. **Deliver the structured Final Setup Status Report** upon completion:

```markdown
## 🏁 Setup Status Report: Streaming RAG

### 🟢 System Status: READY & OPERATIONAL

| Component | Status | Port / Target | Health / Access URL |
|---|---|---|---|
| **FastAPI Backend** | 🟢 Healthy | `:8000` | [http://localhost:8000/health](http://localhost:8000/health) |
| **Swagger UI Docs** | 🟢 Ready | `:8000/docs` | [http://localhost:8000/docs](http://localhost:8000/docs) |
| **MCP Server** | 🟢 Active | `:8001` (SSE) / Stdio | [http://localhost:8001/sse](http://localhost:8001/sse) or stdio |
| **Kafka Broker** | 🟢 Healthy | `:9092` | Internal KRaft cluster |
| **ChromaDB Vector Store** | 🟢 Ready | `:8000` / SQLite | Collection `system-logs` indexed |
| **Ollama LLM** | 🟢 Ready | `:11434` | Model `llama3.2:3b` active |
| **Mock Producer** | 🟢 Emitting | Background | Streaming synthetic logs to Kafka |
| **Ingestion Consumer** | 🟢 Ingesting | Background | Vectorizing & upserting logs |

### 📊 Data Pipeline Verification
- **Kafka Stream**: Confirmed messages published to `system-logs`.
- **Vector Ingestion**: Confirmed log chunks embedded (`all-MiniLM-L6-v2`) and indexed into ChromaDB.

### 🚀 Immediate Next Steps for Testing
1. **Interactive UI**: Open [http://localhost:8000/docs](http://localhost:8000/docs) to test `/api/v1/query` directly.
2. **Instant Semantic Test Query**:
   ```bash
   curl -s -X POST http://localhost:8000/api/v1/query \
     -H "Content-Type: application/json" \
     -d '{"prompt": "What connection timeout errors occurred recently?", "k": 5}'
   ```
3. **Instant Streaming Test Query**:
   ```bash
   curl -N -X POST http://localhost:8000/api/v1/query/stream \
     -H "Content-Type: application/json" \
     -d '{"prompt": "Summarize recent error trends across services", "k": 3}'
   ```
4. **Shutdown**: Run `docker compose down -v` when finished.
```

---

## 7. Testing & Validation Playbook

Guide the user through these test scenarios to demonstrate system capabilities:

### Test 1: Liveness Health Check
```bash
curl http://localhost:8000/health
```
**Expected Response:** `{"status": "ok"}`

### Test 2: Semantic Root-Cause Query (POST `/api/v1/query`)
Ask a question that forces the LangGraph agent to trigger the `vector_search` tool:
```bash
curl -X POST http://localhost:8000/api/v1/query \
  -H "Content-Type: application/json" \
  -d "{\"prompt\": \"What database errors occurred recently in payment-gateway?\", \"k\": 5}"
```
**Evaluation Check:**
- Verify that `"answer"` contains a natural-language diagnosis.
- Verify that `"sources"` contains citations with `event_id`, `service_name`, `log_level`, `timestamp`, `score`, and `excerpt`.

### Test 3: Statistical Query (Aggregate Tool Call)
Ask a question that forces the LangGraph agent to trigger the `log_stats` tool:
```bash
curl -X POST http://localhost:8000/api/v1/query \
  -H "Content-Type: application/json" \
  -d "{\"prompt\": \"How many errors were logged across all services in the last 30 minutes?\", \"k\": 5}"
```
**Evaluation Check:**
- Verify that the agent calls `log_stats` and summarizes the error count and affected services.

### Test 4: Real-Time SSE Token Streaming (POST `/api/v1/query/stream`)
Test token-by-token streaming:
```bash
curl -N -X POST http://localhost:8000/api/v1/query/stream \
  -H "Content-Type: application/json" \
  -d "{\"prompt\": \"Provide a brief summary of system health based on the latest logs.\", \"k\": 3}"
```
**Evaluation Check:**
- Stream chunks must appear as `data: <token>` and terminate with `data: [DONE]`.

### Test 5: MCP Tool Discovery & Inspection
If testing with Model Context Protocol:
1. Open the MCP Inspector:
   ```bash
   npx @modelcontextprotocol/inspector
   ```
2. Connect to `http://localhost:8001/sse`.
3. Verify that tools `vector_search` and `log_stats` are exposed with correct schemas.

### Test 6: Automated Test Suite & Code Quality Gates
Run the full test suite with coverage enforcement:
```bash
# Run unit & integration tests (enforces >=90% test coverage)
pytest -v --cov=src --cov-report=term-missing

# Run code linters & formatters
ruff check .
ruff format --check .
mypy src
```

---

## 8. Agent Troubleshooting Matrix

| Symptom | Root Cause | Solution for the Agent |
|---|---|---|
| `pip: Access is denied` or `ERROR: To modify pip` | Windows locks running `pip.exe` during upgrade | Use `python -m pip install --upgrade pip` |
| `Connection refused: localhost:9092` | Kafka broker is not up or still initializing KRaft | Check `docker ps`. Wait 15s for Kafka healthcheck or run `docker compose -f infrastructure/kafka/docker-compose.yml up -d` |
| `Ollama connection error` or timeout | Ollama service not running or port 11434 blocked | Run `ollama serve` or check `docker compose logs ollama`. Verify model with `ollama list` |
| `ChromaDB empty results` | Producer or Consumer has not run yet | Run `python src/producer.py --burst 20` and `python src/consumer.py` to ingest vectors into `data/chroma/` |
| `Port 8000 already in use` | Another instance of Uvicorn or container running | Check active listeners (`netstat -ano \| findstr :8000` on Windows, `lsof -i :8000` on Linux) |
| `ImportError: cannot import name ...` | `src/` not on Python path | Ensure `--app-dir src` is passed to Uvicorn or run pytest (which configures `pythonpath = src` via `pytest.ini`) |
| `POST /api/v1/query` takes 60–90s or appears frozen | Ollama runs on CPU; model cold start loading into RAM; ReAct agent runs 2 inference passes (tool selection + synthesis); `curl -s` hides all progress | Inform user upfront of expected latency (~3.5 words/s on CPU). Recommend using the streaming endpoint (`/api/v1/query/stream` with `curl -N`), lowering `k` (e.g. 2–3), or testing via Swagger UI. |

---

## 9. Rules for Modifying Code

When assisting the user with code modifications:
1. **Preserve Data Contract**: Any changes to log fields must be updated in `src/model/models.py` (`SystemLog`) and mirrored in `docs/data-contract.md`.
2. **Strict Test Coverage**: Any new code added to `src/` must be backed by tests in `tests/` to satisfy the `fail_under = 90` threshold in `pyproject.toml`.
3. **Async Integrity**: The consumer and FastAPI routes rely on asyncio (`asyncio.to_thread` for blocking embedding/vector store calls). Never introduce blocking synchronous I/O in the async loops.
