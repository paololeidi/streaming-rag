"""LangGraph ReAct agent wiring.

The compiled graph is a singleton created at import time so it is shared across
all FastAPI requests without rebuilding on every call.
"""

from __future__ import annotations

import sys
from pathlib import Path

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

agent_graph = create_react_agent(
    model=llm,
    tools=[vector_search, log_stats],
    prompt=SYSTEM_PROMPT,
    name="sre-analyst",
)
