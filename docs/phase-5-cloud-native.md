# Phase 5: Cloud-Native Deployment (Compose)

Phase **5.1–5.2** packages the Streaming RAG stack so a tester can run the full system with:

```bash
docker compose up --build
```

No local Python venv, host Ollama install, or manual producer/consumer seeding is required for the demo path. Kubernetes/Helm (5.3–5.5) is deferred.

## What was added

| Artifact | Role |
| -------- | ---- |
| [`Dockerfile`](../Dockerfile) | Single multi-stage image for producer, consumer, FastAPI, and MCP |
| [`docker-compose.yml`](../docker-compose.yml) | Full local stack |
| [`.dockerignore`](../.dockerignore) | Keeps build context small |
| Env-driven [`src/config.py`](../src/config.py) | Kafka bootstrap + Chroma host/port for containers |
| Dual-mode [`src/vector_store.py`](../src/vector_store.py) | `HttpClient` when `CHROMA_HOST` is set; else embedded `PersistentClient` |

## Architecture (Compose)

### Dockerfile vs Compose

| Piece | Responsibility |
| ----- | -------------- |
| [`Dockerfile`](../Dockerfile) | Builds **one** app image: Python deps + your `src/` + baked embedding model. Does **not** include Kafka, Chroma, or Ollama. |
| [`docker-compose.yml`](../docker-compose.yml) | Starts the **whole stack**: pulls infra images, builds the app image once, runs multiple containers, wires env/ports/volumes and startup order. |

Four services (`producer`, `consumer`, `api`, `mcp`) share that same app image. Compose overrides each container’s `command` so they run different entrypoints. Infra services use ready-made images from Docker Hub (`image: ...`) and never touch your Dockerfile.

### Service map

```
 ┌───────────────────── Ingestion pipeline ─────────────────────┐
 │                                                              │
 │   producer ──► kafka ──► consumer ──► chroma (HTTP server)   │
 │   (mock logs)   (broker)  (embed +    (vector store)         │
 │                            upsert)                           │
 └──────────────────────────────────────────────┬───────────────┘
                                                │ read
 ┌───────────────────── Query layer ────────────▼───────────────┐
 │                                                              │
 │   api  ──► ollama (LLM)     mcp ──► chroma                   │
 │    │         ▲               │      (same server)            │
 │    └─────────┘               │                               │
 │    also reads chroma         SSE :8001 → Cursor / clients    │
 │    HTTP :8000 → you / curl                                   │
 │                                                              │
 │   ollama-init ──(pull model once)──► ollama   then exits     │
 └──────────────────────────────────────────────────────────────┘
```

### What each service does

| Service | Image source | Role |
| ------- | ------------ | ---- |
| `kafka` | `confluentinc/cp-kafka` | Message broker. Producer writes log events; consumer reads them. |
| `chroma` | `chromadb/chroma` | Vector DB **server**. Consumer writes embeddings; API and MCP query them. |
| `ollama` | `ollama/ollama` | Local LLM runtime. Only the API talks to it for agent reasoning. |
| `ollama-init` | `ollama/ollama` | One-shot job: wait for Ollama, `ollama pull` the model, exit. Not a long-running service. |
| `producer` | Dockerfile | Continuously publishes mock system logs to Kafka. |
| `consumer` | Dockerfile | Reads Kafka, embeds with `all-MiniLM-L6-v2`, upserts into Chroma. |
| `api` | Dockerfile | FastAPI + LangGraph agent. Uses Chroma tools + Ollama. Exposed on host `:8000`. |
| `mcp` | Dockerfile | Same Chroma tools over MCP SSE. Exposed on host `:8001` for Cursor. |

### Data and request flows

1. **Ingest:** `producer` → Kafka topic → `consumer` embeds each log → Chroma stores vectors + metadata.
2. **Ask via API:** Client `POST /api/v1/query` → agent may call `vector_search` / `log_stats` on Chroma → Ollama generates the answer.
3. **Ask via MCP:** Cursor connects to `http://localhost:8001/sse` → same tools hit Chroma directly (no FastAPI in the middle).

### Networking and ports

All services share one Compose network and reach each other by **service name** (e.g. `http://ollama:11434`, `CHROMA_HOST=chroma`).

| Port on your machine | Service | Why published |
| -------------------- | ------- | ------------- |
| `8000` | `api` | Swagger / curl / browsers |
| `8001` | `mcp` | Cursor MCP SSE URL |
| `9092` | `kafka` | Optional: host tools talking to Kafka |
| `11434` | `ollama` | Optional: host `ollama` CLI against the container |
| *(none)* | `chroma` | Internal only — avoids clashing with API on `:8000` |

### Startup order (why `depends_on` exists)

1. Kafka and Chroma become healthy.
2. Producer can start (needs Kafka). Consumer starts (needs Kafka + Chroma).
3. Ollama becomes healthy → `ollama-init` pulls the model → exits successfully.
4. API starts only after Chroma is up **and** `ollama-init` finished (so the model exists before the first query).
5. MCP starts once Chroma is healthy (it does not need Ollama).

### Design choices worth noting

- **App image stays lean:** LLM runtime is a peer (`ollama`), not baked into the Dockerfile — rebuilds stay smaller and Ollama can be scaled/replaced independently.
- **One image, four processes:** avoids four nearly identical Dockerfiles; behavior differences are Compose `command` + env only.
- **Chroma as a server under Compose:** shared by consumer (write) and API/MCP (read) over HTTP. Embedded `PersistentClient` remains available for non-Docker local runs when `CHROMA_HOST` is unset.

## Chroma dual mode

| Mode | When | Client |
| ---- | ---- | ------ |
| Server | `CHROMA_HOST` set (Compose: `chroma`) | `chromadb.HttpClient` |
| Embedded | `CHROMA_HOST` empty (local non-Docker) | `PersistentClient` → `data/chroma/` |

## Environment variables

Compose sets internal URLs on each service. Host `.env` is optional (LangSmith, model name). Important keys:

| Variable | Host default | Compose |
| -------- | ------------ | ------- |
| `KAFKA_BOOTSTRAP_SERVERS` | `localhost:9092` | `kafka:29092` |
| `CHROMA_HOST` / `CHROMA_PORT` | unset / `8000` | `chroma` / `8000` |
| `OLLAMA_BASE_URL` | `http://localhost:11434` | `http://ollama:11434` |
| `OLLAMA_MODEL` | `llama3.2:3b` | same (used by `ollama-init` + API) |

## Resource notes

- First start: image build + model pull are slow; later runs reuse layers and `ollama_data`.
- Recommend **~8 GB+** Docker Desktop memory for Ollama (`llama3.2:3b` ~2 GB) plus Kafka, Chroma, and the embedding model in consumer/API/MCP.

## Deferred (5.3–5.5)

- Kubernetes Deployments/Services/ConfigMaps/Secrets (including Ollama + PVC)
- Helm chart parameterization
- Minikube/Kind end-to-end validation

Kafka-only Compose for local Python development remains at [`infrastructure/kafka/docker-compose.yml`](../infrastructure/kafka/docker-compose.yml).
