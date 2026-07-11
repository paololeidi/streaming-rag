# End-to-End Streaming RAG

Cloud-native, event-driven Streaming RAG for real-time log ingestion, vectorization, and intelligent incident analysis.

## Overview

This project ingests application logs via Kafka, embeds them into a vector database (ChromaDB), and will expose an agentic API for root-cause analysis and troubleshooting.

## Quick start

```bash
# Start Kafka
docker compose -f infrastructure/kafka/docker-compose.yml up -d

# Set up Python environment
python -m venv .venv
.\.venv\Scripts\Activate.ps1   # PowerShell
pip install -r requirements.txt

# Run producer and consumer (separate terminals)
cd src
python producer.py
python consumer.py
```

See [docs/README.md](docs/README.md) for full setup instructions and architecture details.

## Project status

| Phase | Status |
|-------|--------|
| 1 — Ingestion & Streaming | Complete |
| 2 — Vector Processing | Complete |
| 3 — Agentic RAG & API | Planned |
| 4 — Cloud-Native Deployment | Planned |

See [roadmap.md](roadmap.md) for the full development plan.
