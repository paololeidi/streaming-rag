"""LangGraph ReAct agent wiring.

The compiled graph is built once at FastAPI startup via build_agent_graph() and
stored in app.state. This async factory pattern replaces the old module-level
singleton so the MCP client lifecycle (an async context manager that must stay
open for the duration of the app) can be managed inside the FastAPI lifespan.
"""

from __future__ import annotations

import sys
from pathlib import Path
from typing import Any

from langchain_ollama import ChatOllama
from langgraph.prebuilt import create_react_agent

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from config import OLLAMA_BASE_URL, OLLAMA_KEEP_ALIVE, OLLAMA_MODEL
from agent.prompts import SYSTEM_PROMPT
from agent.tools import log_stats, vector_search

llm = ChatOllama(
    model=OLLAMA_MODEL,
    base_url=OLLAMA_BASE_URL,
    temperature=0,
    keep_alive=OLLAMA_KEEP_ALIVE,
    num_ctx=2048,
)


async def build_agent_graph(extra_tools: list[Any] | None = None):
    """Build and return a compiled LangGraph ReAct agent.

    Args:
        extra_tools: Optional list of additional LangChain-compatible tools
                     loaded from an external MCP server. These are appended to
                     the native vector_search and log_stats tools.
    """
    tools = [vector_search, log_stats] + (extra_tools or [])
    return create_react_agent(
        model=llm,
        tools=tools,
        prompt=SYSTEM_PROMPT,
        name="sre-analyst",
    )
