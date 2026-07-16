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

from agent.graph import build_agent_graph
from api.routes import router
from config import MCP_SERVER_URL
from embeddings import HuggingFaceEmbedder


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncIterator[None]:
    """Initialise shared resources at startup and release them on shutdown.

    Startup steps:
    1. Warm up the embedding model so the first request is not slow.
    2. If MCP_SERVER_URL is set, connect to the MCP server, load its tools,
       and include them alongside the native tools in the agent graph.
    3. Build the compiled LangGraph agent and store it in app.state so all
       request handlers share a single instance without rebuilding on each call.
    """
    print("Loading embedding model...")
    embedder = HuggingFaceEmbedder()
    embedder.embed("warmup")
    print("Embedding model ready.")

    if MCP_SERVER_URL:
        from langchain_mcp_adapters.client import MultiServerMCPClient

        print(f"Connecting to MCP server at {MCP_SERVER_URL} ...")
        client = MultiServerMCPClient(
            {"log-analysis": {"url": MCP_SERVER_URL, "transport": "sse"}}
        )
        extra_tools = await client.get_tools()
        print(f"Loaded {len(extra_tools)} tool(s) from MCP server.")
        app.state.mcp_client = client  # keep reference alive for the duration of the app
        app.state.agent_graph = await build_agent_graph(extra_tools)
        print("Agent ready (native + MCP tools).")
    else:
        app.state.agent_graph = await build_agent_graph()
        print("Agent ready (native tools only).")

    yield

    print("API shutting down.")


app = FastAPI(
    title="Streaming RAG — SRE Analyst API",
    description=(
        "An agentic API backed by a LangGraph ReAct agent. "
        "Send a natural-language question about your system logs and receive a "
        "grounded answer with source citations from the live vector store."
    ),
    version="0.4.0",
    lifespan=lifespan,
)

app.include_router(router, prefix="/api/v1")


@app.get("/health", tags=["Health"])
async def health() -> dict[str, str]:
    return {"status": "ok"}


if __name__ == "__main__":
    uvicorn.run("api.main:app", host="0.0.0.0", port=8000, reload=True, app_dir="src")
