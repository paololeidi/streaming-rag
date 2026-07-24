# Multi-stage image for producer, consumer, FastAPI, and MCP server.
# Compose overrides the command per service. Ollama is NOT included — it runs
# as a peer service.

FROM python:3.11-slim AS builder

RUN apt-get update && apt-get install -y --no-install-recommends \
        build-essential \
        gcc \
        g++ \
        librdkafka-dev \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /build
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Bake the embedding model into the image so first start does not download it.
ENV HF_HOME=/models/huggingface
ENV TRANSFORMERS_CACHE=/models/huggingface
ENV SENTENCE_TRANSFORMERS_HOME=/models/sentence-transformers
RUN python -c "from sentence_transformers import SentenceTransformer; SentenceTransformer('all-MiniLM-L6-v2')"


FROM python:3.11-slim AS runtime

RUN apt-get update && apt-get install -y --no-install-recommends \
        librdkafka1 \
        libgomp1 \
        curl \
    && rm -rf /var/lib/apt/lists/*

COPY --from=builder /usr/local /usr/local
COPY --from=builder /models /models

ENV HF_HOME=/models/huggingface
ENV TRANSFORMERS_CACHE=/models/huggingface
ENV SENTENCE_TRANSFORMERS_HOME=/models/sentence-transformers
ENV PYTHONPATH=/app/src
ENV PYTHONUNBUFFERED=1

WORKDIR /app
COPY src/ /app/src/

# Default is overridden by Compose per service.
CMD ["uvicorn", "api.main:app", "--host", "0.0.0.0", "--port", "8000"]
