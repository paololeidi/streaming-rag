# Step-by-Step Guide: Build, Run & Test

A comprehensive, hands-on guide for building the project, running individual modules and services, and testing the API, LangGraph agent, tools, and MCP server.

---

## 📋 Table of Contents

1. [Prerequisites & System Requirements](#1-prerequisites--system-requirements)
2. [Building the Project](#2-building-the-project)
   - [Method A: Local Python Environment](#method-a-local-python-environment)
   - [Method B: Docker Image Build](#method-b-docker-image-build)
   - [Method C: Kubernetes & Helm (Kind)](#method-c-kubernetes--helm-kind)
3. [Running Components & Classes](#3-running-components--classes)
   - [Option 1: Full-Stack Docker Compose (Recommended)](#option-1-full-stack-docker-compose-recommended)
   - [Option 2: Individual Component Execution (Local Development)](#option-2-individual-component-execution-local-development)
     - [Step 2.1: Start Kafka Broker](#step-21-start-kafka-broker)
     - [Step 2.2: Start Ollama LLM Runtime](#step-22-start-ollama-llm-runtime)
     - [Step 2.3: Run the Log Producer](#step-23-run-the-log-producer)
     - [Step 2.4: Run the Ingestion Consumer](#step-24-run-the-ingestion-consumer)
     - [Step 2.5: Run the FastAPI Backend](#step-25-run-the-fastapi-backend)
     - [Step 2.6: Run the Model Context Protocol (MCP) Server](#step-26-run-the-model-context-protocol-mcp-server)
4. [Testing API, Agent & Tools](#4-testing-api-agent--tools)
   - [4.1 Testing the FastAPI Endpoints](#41-testing-the-fastapi-endpoints)
     - [Health Check (`GET /health`)](#health-check-get-health)
     - [Synchronous Incident Query (`POST /api/v1/query`)](#synchronous-incident-query-post-apiv1query)
     - [Real-Time SSE Streaming (`POST /api/v1/query/stream`)](#real-time-sse-streaming-post-apiv1querystream)
     - [Interactive Swagger UI](#interactive-swagger-ui)
   - [4.2 Testing the LangGraph Agent Graph](#42-testing-the-langgraph-agent-graph)
   - [4.3 Testing the Agent Tools (`vector_search` & `log_stats`)](#43-testing-the-agent-tools-vector_search--log_stats)
   - [4.4 Testing via MCP Clients & Inspector](#44-testing-via-mcp-clients--inspector)
   - [4.5 Running Automated Tests & Code Quality Gates](#45-running-automated-tests--code-quality-gates)
5. [Troubleshooting & FAQ](#5-troubleshooting--faq)

---

## 1. Prerequisites & System Requirements

| Tool | Minimum Version | Purpose |
|---|---|---|
| **Python** | `3.11+` | Application runtimes, testing, tools |
| **Docker & Docker Compose** | `24.0+` / Compose `v2+` | Containerized stack orchestration |
| **Ollama** | Latest | Local LLM runtime (`llama3.2:3b`) |
| **Git** | `2.30+` | Source control |
| **RAM** | `8 GB+` (16 GB for Kind) | Required for Ollama model and Kafka KRaft |

---

## 2. Building the Project

### Method A: Local Python Environment

Use this method when developing or debugging application code without container rebuilds.

```bash
# 1. Clone repository and navigate to root
cd streaming-rag

# 2. Create and activate virtual environment
python -m venv .venv

# On Linux / macOS / Git Bash:
source .venv/bin/activate    # Linux/macOS
# or: source .venv/Scripts/activate # Windows Git Bash

# On Windows PowerShell:
.\.venv\Scripts\Activate.ps1

# 3. Upgrade pip and install application dependencies
pip install --upgrade pip
pip install -r requirements.txt

# 4. Install development and testing dependencies (pytest, ruff, mypy)
pip install -r requirements-dev.txt

# 5. Set up local configuration
cp .env.example .env
```

Ensure the Ollama model is downloaded locally:
```bash
ollama pull llama3.2:3b
```

---

### Method B: Docker Image Build

Build the optimized multi-stage application image (bundles the Hugging Face sentence-transformers model directly in the image layer for rapid cold starts):

```bash
docker build -t streaming-rag:local .
```

---

### Method C: Kubernetes & Helm (Kind)

For local Kubernetes validation using Kind:

```bash
# Automated single-script deployment (Git Bash / WSL / Linux / macOS):
./scripts/kind-deploy.sh
```

Or step-by-step:
```bash
# 1. Create a Kind cluster
kind create cluster --name streaming-rag

# 2. Build and load the Docker image into the Kind cluster nodes
docker build -t streaming-rag:local .
kind load docker-image streaming-rag:local --name streaming-rag

# 3. Install the Helm chart
helm upgrade --install streaming-rag ./charts/streaming-rag -n streaming-rag --create-namespace

# 4. Port forward services for access
kubectl port-forward -n streaming-rag svc/api 8000:8000
kubectl port-forward -n streaming-rag svc/mcp 8001:8001
```

---

## 3. Running Components & Classes

### Option 1: Full-Stack Docker Compose (Recommended)

To bring up the entire system (Kafka broker, ChromaDB vector store, Ollama with pre-pulled `llama3.2:3b`, Producer, Consumer, FastAPI app, and MCP Server) in one command:

```bash
docker compose up --build
```

To run detached in background:
```bash
docker compose up --build -d
```

To view logs from a specific container:
```bash
docker compose logs -f api
docker compose logs -f consumer
docker compose logs -f producer
```

To stop all services:
```bash
docker compose down -v
```

---

### Option 2: Individual Component Execution (Local Development)

When iterating on Python modules, run the infrastructure dependencies in Docker and the Python components locally.

> [!NOTE] Where is ChromaDB running in local development?
> In this local mode, **ChromaDB does not run as a separate server or container**.
> Because `CHROMA_HOST` is left unset in `.env`, `src/vector_store.py` automatically initializes an **embedded in-process client** (`chromadb.PersistentClient`) backed by SQLite files persisted to `data/chroma/`.
> Both the consumer and the API / MCP server interact directly with this local directory on disk, so you do **not** need to start a Chroma container or server process.

#### Step 2.1: Start Kafka Broker

Start local Kafka in KRaft mode (no Zookeeper required):
```bash
docker compose -f infrastructure/kafka/docker-compose.yml up -d
```
*Kafka will listen on `localhost:9092`.*

#### Step 2.2: Start Ollama LLM Runtime

Ensure Ollama is running on your host machine and has the model loaded:
```bash
ollama run llama3.2:3b
```
*Ollama listens on `http://localhost:11434`.*

#### Step 2.3: Run the Log Producer

The producer generates synthetic system logs using `Faker` conforming to the Pydantic `SystemLog` data contract:

```bash
# Terminal 1
.venv/Scripts/python src/producer.py
```
Output:
```text
Starting log stream... (Press Ctrl+C to stop)
Log sent to system-logs [0]
Log sent to system-logs [1]
```

#### Step 2.4: Run the Ingestion Consumer

The consumer reads logs from Kafka, applies chunking (`chunk_log`), computes dense vector embeddings using `all-MiniLM-L6-v2`, writes vectors and metadata to ChromaDB (`data/chroma/`), and executes manual offset commits:

```bash
# Terminal 2
.venv/Scripts/python src/consumer.py
```
Output:
```text
Consumer subscribed to ['system-logs']. Ingestion pipeline running...
Upserted log kafka-0-101 into ChromaDB (service=payment-gateway, level=INFO)
```

#### Step 2.5: Run the FastAPI Backend

The FastAPI application initializes the LangGraph ReAct agent graph, warms up the Hugging Face embedder during lifespan startup, and exposes REST + SSE endpoints:

```bash
# Terminal 3
.venv/Scripts/uvicorn api.main:app --reload --app-dir src --port 8000
```
*Swagger UI is available at `http://localhost:8000/docs`.*

#### Step 2.6: Run the Model Context Protocol (MCP) Server

The MCP server exposes `vector_search` and `log_stats` tools via the Model Context Protocol:

**Stdio Mode (Default - standard input/output for local desktop clients like Cursor and Claude Desktop):**
```bash
.venv/Scripts/python src/mcp_server/server.py
```
*(Desktop clients typically spawn this command automatically via their MCP configuration file).*

**SSE Mode (HTTP/SSE transport on port 8001 - for MCP Inspector or external agents):**
```bash
# Terminal 4
.venv/Scripts/python src/mcp_server/server.py --sse
```

---

## 4. Testing API, Agent & Tools

### 4.1 Testing the FastAPI Endpoints

#### Health Check (`GET /health`)
Verify application liveness:
```bash
curl http://localhost:8000/health
```
Response:
```json
{"status": "ok"}
```

#### Synchronous Incident Query (`POST /api/v1/query`)
Submit natural-language incident questions to the agent.

**Semantic Retrieval Query:**
```bash
curl -s -X POST http://localhost:8000/api/v1/query \
  -H "Content-Type: application/json" \
  -d '{"prompt": "what connection errors occurred recently?", "k": 5}'
```

**Statistical Aggregation Query:**
```bash
curl -s -X POST http://localhost:8000/api/v1/query \
  -H "Content-Type: application/json" \
  -d '{"prompt": "how many errors were logged in the last 30 minutes?", "k": 5}'
```

Sample JSON Response:
```json
{
  "answer": "In the last 30 minutes, 4 connection timeout errors occurred in payment-gateway...",
  "sources": [
    {
      "service_name": "payment-gateway",
      "log_level": "ERROR",
      "timestamp": "2026-10-12T10:00:00+00:00",
      "excerpt": "[ERROR] payment-gateway @ 2026-10-12T10:00:00: Connection timeout to database"
    }
  ]
}
```

#### Real-Time SSE Streaming (`POST /api/v1/query/stream`)
Test token-by-token streaming over Server-Sent Events (SSE):

```bash
curl -N -X POST http://localhost:8000/api/v1/query/stream \
  -H "Content-Type: application/json" \
  -d '{"prompt": "Summarize all recent issues across our services", "k": 5}'
```
Response stream:
```text
data: {"token": "Based"}

data: {"token": " on"}

data: {"token": " the"}

data: {"token": " logs"}
...
data: [DONE]
```

#### Interactive Swagger UI
Open your browser at [http://localhost:8000/docs](http://localhost:8000/docs) to test the endpoints with the interactive API client.

---

### 4.2 Testing the LangGraph Agent Graph

You can test the LangGraph agent graph (`src/agent/graph.py`) directly from Python without starting the FastAPI server:

Create a quick scratch script or run in Python interactive shell:

```python
import asyncio
from langchain_core.messages import HumanMessage
from agent.graph import build_agent_graph


async def main():
    # Build graph with native Chroma tools
    graph = build_agent_graph()

    # Send a prompt to the ReAct agent
    result = await graph.ainvoke(
        {"messages": [HumanMessage(content="Which services logged errors recently?")]}
    )

    # Print the agent's final reasoning
    print("Agent Response:\n", result["messages"][-1].content)


if __name__ == "__main__":
    asyncio.run(main())
```

---

### 4.3 Testing the Agent Tools (`vector_search` & `log_stats`)

You can test the tool functions directly to ensure vector search and metadata queries operate properly:

```python
from agent.tools import vector_search, log_stats

# 1. Test semantic vector search against ChromaDB
print("--- Vector Search ---")
search_results = vector_search.invoke({"query": "database timeout", "k": 3})
print(search_results)

# 2. Test metadata statistical aggregations
print("--- Log Statistics ---")
stats = log_stats.invoke({"log_level": "ERROR", "minutes": 60})
print(stats)
```

---

### 4.4 Testing via MCP Clients & Inspector

When the MCP server is running (`src/mcp_server/server.py --sse` on port 8001), any MCP client can consume the tools.

#### Using MCP Settings in an AI Editor (e.g., Cursor / Claude Desktop)
Add the server entry to your MCP configuration:
```json
{
  "mcpServers": {
    "streaming-rag-logs": {
      "url": "http://localhost:8001/sse"
    }
  }
}
```
**Prompts to verify:**
1. *"What tools do you have available?"* $\rightarrow$ agent lists `vector_search` and `log_stats`.
2. *"How many errors were logged in the last hour?"* $\rightarrow$ invokes `log_stats`.
3. *"Find logs related to database timeouts"* $\rightarrow$ invokes `vector_search`.

---

### 4.5 Running Automated Tests & Code Quality Gates

The project contains a comprehensive automated test suite and strict code quality verification.

#### Run Pytest with Coverage Enforcement ($\ge 90\%$)
```bash
pytest --cov=src --cov-report=term-missing --cov-fail-under=90 tests/
```
*Current test suite achieves **98% coverage** across all source files.*

#### Run Unit Tests Only
```bash
pytest tests/unit/ -v
```

#### Run Integration Tests Only
```bash
pytest tests/integration/ -v
```

#### Run Code Quality & Linting Checks
```bash
# Check code style and formatting rules
ruff check .

# Check formatting compliance
ruff format --check .

# Run static type checking
mypy src/
```

---

## 5. Troubleshooting & FAQ

### Kafka connection fails (`NoBrokersAvailable`)
- Verify that the Kafka broker is running: `docker ps`
- If running locally outside Docker, ensure `KAFKA_BOOTSTRAP_SERVERS=localhost:9092` in `.env`.
- If running inside Docker Compose, services communicate via the internal hostname `kafka:29092`.

### Ollama returns connection refused or model not found
- Ensure Ollama is running: `curl http://localhost:11434/api/tags`
- Pull the required model: `ollama pull llama3.2:3b`
- For Docker Compose, the `ollama-init` service automatically pulls the model on the first boot.

### ChromaDB returns empty search results
- Make sure the consumer has been running and processing messages from the producer:
  ```bash
  python src/producer.py   # produce events
  python src/consumer.py   # ingest & vectorize events
  ```
- Check that the `data/chroma/` directory exists and has write permissions.
