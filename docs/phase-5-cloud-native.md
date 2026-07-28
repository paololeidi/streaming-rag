# Phase 5: Cloud-Native Deployment

Phase 5 packages the Streaming RAG stack for local demos (Compose) and Kubernetes (Helm + Kind).

| Sub-phase | Deliverable | Status |
| --------- | ----------- | ------ |
| 5.1 | Multi-stage [`Dockerfile`](../Dockerfile) | Done |
| 5.2 | Root [`docker-compose.yml`](../docker-compose.yml) | Done |
| 5.3–5.4 | Helm chart [`charts/streaming-rag/`](../charts/streaming-rag/) | Done |
| 5.5 | Kind deploy docs + [`scripts/kind-deploy.sh`](../scripts/kind-deploy.sh) | Done |

**Day-to-day on a low-RAM laptop:** Compose or local Python (see README).  
**Kind validation:** work laptop (Kind already installed); prefer ~16 GB+ host RAM for the full stack.

---

## What was added

| Artifact | Role |
| -------- | ---- |
| [`Dockerfile`](../Dockerfile) | Single multi-stage image for producer, consumer, FastAPI, and MCP |
| [`docker-compose.yml`](../docker-compose.yml) | Full local Compose stack |
| [`.dockerignore`](../.dockerignore) | Keeps build context small |
| Env-driven [`src/config.py`](../src/config.py) | Kafka bootstrap + Chroma host/port for containers |
| Dual-mode [`src/vector_store.py`](../src/vector_store.py) | `HttpClient` when `CHROMA_HOST` is set; else embedded `PersistentClient` |
| [`charts/streaming-rag/`](../charts/streaming-rag/) | Helm chart (K8s Deployments, Services, PVCs, Job, ConfigMap, Secret) |
| [`scripts/kind-deploy.sh`](../scripts/kind-deploy.sh) | Build image → `kind load` → `helm upgrade --install` |

---

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

---

## Architecture (Kubernetes / Helm)

Same logical stack as Compose. The Helm chart under [`charts/streaming-rag/`](../charts/streaming-rag/) is the **single source of truth** for manifests (no parallel plain-YAML tree).

| Compose service | Kubernetes resource |
| --------------- | ------------------- |
| `kafka` | Deployment + ClusterIP Service (`kafka:29092`, single-node KRaft — not Bitnami/Strimzi) |
| `chroma` | Deployment + Service + PVC `chroma-data` |
| `ollama` | Deployment + Service + PVC `ollama-data` |
| `ollama-init` | Job (Helm post-install/upgrade hook) that `ollama pull`s the model |
| `producer` / `consumer` / `api` / `mcp` | Deployments from image `streaming-rag:local`; different `command` + ConfigMap env |
| host ports | `kubectl port-forward` to `svc/api` and `svc/mcp` (default ClusterIP) |

API pods use an **initContainer** that waits until `http://ollama:11434/api/tags` lists the configured model, so the agent does not start before the pull finishes.

### Kind deploy (work laptop)

Prerequisites: Docker, Kind, kubectl, Helm 3.

```bash
# From project root — or: ./scripts/kind-deploy.sh
kind create cluster --name streaming-rag   # skip if cluster exists
docker build -t streaming-rag:local .
kind load docker-image streaming-rag:local --name streaming-rag
helm upgrade --install streaming-rag ./charts/streaming-rag \
  -n streaming-rag --create-namespace
kubectl get pods,jobs -n streaming-rag -w
```

### Validation checklist

1. `kubectl port-forward -n streaming-rag svc/api 8000:8000`
2. `curl http://localhost:8000/health` → `{"status":"ok"}`
3. `POST /api/v1/query` with a simple prompt after consumer has ingested logs
4. `kubectl logs -n streaming-rag deploy/consumer --tail=50` shows ingest
5. `kubectl port-forward -n streaming-rag svc/mcp 8001:8001` → Cursor MCP URL `http://localhost:8001/sse`

Full chart reference: [`charts/streaming-rag/README.md`](../charts/streaming-rag/README.md).

---

## Chroma dual mode

| Mode | When | Client |
| ---- | ---- | ------ |
| Server | `CHROMA_HOST` set (Compose / K8s: `chroma`) | `chromadb.HttpClient` |
| Embedded | `CHROMA_HOST` empty (local non-Docker) | `PersistentClient` → `data/chroma/` |

## Environment variables

| Variable | Host default | Compose / K8s |
| -------- | ------------ | ------------- |
| `KAFKA_BOOTSTRAP_SERVERS` | `localhost:9092` | `kafka:29092` |
| `CHROMA_HOST` / `CHROMA_PORT` | unset / `8000` | `chroma` / `8000` |
| `OLLAMA_BASE_URL` | `http://localhost:11434` | `http://ollama:11434` |
| `OLLAMA_MODEL` | `llama3.2:3b` | same (init Job + API) |

## Resource notes

- **Compose:** first start builds the image and pulls the Ollama model (slow); later runs reuse layers/volumes. Partial stacks help on ~8 GB machines.
- **Kind:** heavier than Compose (control plane + same workloads). Target validation on a work laptop with enough RAM (~16 GB+ recommended).

Kafka-only Compose for local Python development remains at [`infrastructure/kafka/docker-compose.yml`](../infrastructure/kafka/docker-compose.yml).
