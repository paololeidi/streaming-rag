# streaming-rag Helm chart

Deploys the full Streaming RAG stack to Kubernetes: Kafka (single-node KRaft), Chroma, Ollama (+ model pull Job), producer, consumer, API, and MCP.

## Prerequisites

- Docker
- [Kind](https://kind.sigs.k8s.io/) (work laptop validation target)
- kubectl
- Helm 3

Host RAM: prefer **~16 GB+** for Kind + Ollama + Kafka + embeddings.

## Kind: build, load, install

From the **project root**:

```bash
# 1. Cluster (skip if you already have one)
kind create cluster --name streaming-rag

# 2. Build the app image and load it into Kind
docker build -t streaming-rag:local .
kind load docker-image streaming-rag:local --name streaming-rag

# 3. Install / upgrade
helm upgrade --install streaming-rag ./charts/streaming-rag \
  -n streaming-rag --create-namespace

# 4. Watch rollout (ollama-init Job pulls the model — can take several minutes)
kubectl get pods,jobs -n streaming-rag -w
```

Or use the helper script:

```bash
./scripts/kind-deploy.sh
```

## Validate

```bash
kubectl port-forward -n streaming-rag svc/api 8000:8000
# other terminal
curl http://localhost:8000/health
curl -s -X POST http://localhost:8000/api/v1/query \
  -H "Content-Type: application/json" \
  -d '{"prompt": "what errors occurred recently?", "k": 5}'

# MCP SSE
kubectl port-forward -n streaming-rag svc/mcp 8001:8001
# Cursor MCP url: http://localhost:8001/sse
```

Check ingest:

```bash
kubectl logs -n streaming-rag deploy/consumer --tail=50
```

## Useful values

| Key | Default | Notes |
| --- | ------- | ----- |
| `image.repository` / `tag` | `streaming-rag` / `local` | Must match `kind load` |
| `image.pullPolicy` | `IfNotPresent` | Required for locally loaded images |
| `ollama.model` | `llama3.2:3b` | Pulled by `ollama-init` Job |
| `ollama.keepAlive` | `30m` | Passed to the API |
| `api.serviceType` | `ClusterIP` | Use `NodePort` if you prefer not to port-forward |
| `langsmith.enabled` | `false` | Set `true` + `langsmith.apiKey` to enable tracing |

Example:

```bash
helm upgrade --install streaming-rag ./charts/streaming-rag \
  -n streaming-rag --create-namespace \
  --set ollama.model=qwen2.5:3b \
  --set langsmith.enabled=true \
  --set-string langsmith.apiKey=lsv2_...
```

## Uninstall

```bash
helm uninstall streaming-rag -n streaming-rag
kubectl delete namespace streaming-rag
# optional: kind delete cluster --name streaming-rag
```

PVCs may remain depending on reclaim policy; delete `chroma-data` / `ollama-data` PVCs if you want a clean slate.
