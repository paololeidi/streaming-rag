# End-to-End Streaming RAG: Intelligent Incident & Log Analysis

## 📌 Project Overview

This project implements a Cloud-Native, Event-Driven Streaming RAG (Retrieval-Augmented Generation) architecture. It is designed to ingest real-time application logs, vectorize them on the fly, and expose an Agentic API capable of dynamically querying the live context to perform root-cause analysis and system troubleshooting.

---

## 🚀 Phase 1: Ingestion & Streaming (The Data Engine)

**Goal:** Establish a robust, high-throughput message broker and simulate a real-time stream of system events.

- [x] **1.1 Kafka KRaft Setup:** Configure and deploy a local Kafka cluster without Zookeeper using Docker Compose.
- [x] **1.2 Data Contract Definition:** Define strictly typed log schemas using Pydantic (e.g., timestamps, log levels, service names, stack traces).
- [x] **1.3 Python Producer:** Develop a mock service generating realistic application logs and pushing them to Kafka topics with proper partitioning keys.
- [x] **1.4 Python Consumer:** Implement a real-time consumer listener capable of reading the data stream and handling offsets reliably.

---

## 🧠 Phase 2: Vector Processing (The AI Core)

**Goal:** Transform the raw, real-time log stream into spatial embeddings and persist them in a Vector Database for similarity search.

- [x] **2.1 Vector Database Setup:** Deploy a local Vector DB instance (e.g., ChromaDB, Qdrant, or Milvus).
- [x] **2.2 Chunking Strategy:** Define how streaming text should be chunked/batched based on time windows or token limits to optimize retrieval.
- [x] **2.3 Embedding Generation:** Integrate a Text Embedding model (Hugging Face local or OpenAI API) to vectorize incoming Kafka messages.
- [x] **2.4 Asynchronous Ingestion:** Update the Kafka consumer to asynchronously write embeddings and metadata into the Vector DB without blocking the event loop.

---

## 🤖 Phase 3: Agentic RAG & API (The Intelligent Interface)

**Goal:** Build a reasoning LLM Agent capable of answering user queries by deciding when to query the streaming vector context vs. when to use other tools.

- [ ] **3.1 LangSmith Observability:** Configure LangSmith for UI tracing, token monitoring, and debugging the LLM's decision-making loops.
- [ ] **3.2 FastAPI Backend:** Set up a lightweight, asynchronous API to handle incoming user prompts.
- [ ] **3.3 Agent Architecture (LangGraph/LangChain):** Develop a Router Agent equipped with specific "Tools".
- [ ] **3.4 Tool Implementation:** 
  - *Vector DB Tool:* For contextual log retrieval.
  - *Auxiliary Tools:* e.g., math calculations or static internal documentation search.
- [ ] **3.5 Prompt Engineering:** Design robust system prompts focusing on function calling, tool usage, and hallucination mitigation.

---

## ☁️ Phase 4: Cloud-Native Deployment & LLMOps (The Infrastructure)

**Goal:** Package the entire system into production-ready containers and deploy it to a Kubernetes cluster using Helm.

- [ ] **4.1 Dockerization:** Write optimized, multi-stage `Dockerfile`s for the Producer, Consumer, and FastAPI backend.
- [ ] **4.2 Unified Compose:** Create a master `docker-compose.yml` to spin up the entire stack locally (Kafka, Vector DB, App Services).
- [ ] **4.3 Kubernetes Manifests:** Translate the architecture into standard K8s resources (Deployments, Services, ConfigMaps, Secrets).
- [ ] **4.4 Helm Chart Creation:** Package the manifests into a custom Helm Chart for parameterized, declarative deployments.
- [ ] **4.5 Local K8s Deployment:** Deploy the entire stack to a local cluster (Minikube/Kind) and validate end-to-end functionality.