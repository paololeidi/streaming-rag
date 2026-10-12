# Streaming RAG Documentation

Central index for the technical specifications, architectural guides, and development playbooks of the Streaming RAG project.

## Documentation Index

| Document | Description |
|---|---|
| [System Architecture & Technical Guide](./technical-guide.md) | **Comprehensive end-to-end architecture, design rationale, and implementation details** |
| [Build, Run & Test Guide](./run-and-test-guide.md) | **Step-by-step developer playbook for local setup, Docker, Kind, and testing** |
| [Data Contract](./data-contract.md) | Formal `SystemLog` schema, Pydantic validation rules, and serialization format |
| [Production Improvements](./improvements.md) | Engineering roadmap and production improvements backlog |

---

## Repository Layout

```text
streaming-rag/
├── src/
│   ├── config.py          # Centralized configuration (Kafka, Chroma, Ollama, MCP)
│   ├── producer.py        # Synthetic microservice log event producer
│   ├── consumer.py        # Stream consumer, vector embedding pipeline, offset commit
│   ├── embeddings.py      # Hugging Face all-MiniLM-L6-v2 in-process embedder
│   ├── chunking.py        # Granular log-to-chunk serialization
│   ├── vector_store.py    # Dual-mode ChromaDB client wrapper (embedded & HTTP)
│   ├── model/             # Typed Pydantic data contracts (SystemLog, LogLevel)
│   ├── agent/             # LangGraph ReAct agent graph, prompts, native tools
│   ├── api/               # FastAPI REST & SSE streaming server
│   └── mcp_server/        # FastMCP server exposing log tools over SSE & stdio
├── tests/
│   ├── unit/              # Isolated unit test suites (98% coverage)
│   └── integration/       # End-to-end integration test suites
├── infrastructure/        # Infrastructure compose definitions
├── charts/                # Helm chart for Kubernetes deployment
├── scripts/               # Automation scripts (e.g., Kind deployment)
└── docs/                  # Documentation suite
```
