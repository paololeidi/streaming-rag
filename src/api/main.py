"""FastAPI application entry point.

Run with:
    uvicorn api.main:app --reload --app-dir src
"""

from __future__ import annotations

import sys
from contextlib import asynccontextmanager
from pathlib import Path
from typing import AsyncIterator

import uvicorn
from fastapi import FastAPI

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from api.routes import router
from embeddings import HuggingFaceEmbedder


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncIterator[None]:
    """Warm up the embedding model at startup so the first request is not slow."""
    print("Loading embedding model...")
    embedder = HuggingFaceEmbedder()
    embedder.embed("warmup")
    print("Embedding model ready.")
    yield
    print("API shutting down.")


app = FastAPI(
    title="Streaming RAG — SRE Analyst API",
    description=(
        "An agentic API backed by a LangGraph ReAct agent. "
        "Send a natural-language question about your system logs and receive a "
        "grounded answer with source citations from the live vector store."
    ),
    version="0.3.0",
    lifespan=lifespan,
)

app.include_router(router, prefix="/api/v1")


@app.get("/health", tags=["Health"])
async def health() -> dict[str, str]:
    return {"status": "ok"}


if __name__ == "__main__":
    uvicorn.run("api.main:app", host="0.0.0.0", port=8000, reload=True, app_dir="src")
