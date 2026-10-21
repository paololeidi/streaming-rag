"""API route definitions."""

from __future__ import annotations

import re
import sys
from pathlib import Path

from fastapi import APIRouter, HTTPException, Request
from fastapi.responses import StreamingResponse
from langchain_core.messages import AIMessage, HumanMessage

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from api.schemas import LogSource, QueryRequest, QueryResponse

router = APIRouter()

# Pattern to detect lines produced by the vector_search tool output.
# Supports both enriched headers ("[N] LEVEL | service @ ts | event_id=... | score=...\n<doc>")
# and legacy headers ("[N] LEVEL | service @ ts\n<doc>").
_SOURCE_HEADER = re.compile(
    r"\[(\d+)\]\s+(\w+)\s+\|\s+([\w\-]+)\s+@\s+([^\n|]+)"
    r"(?:\s+\|\s+event_id=([^\s|\n]+))?"
    r"(?:\s+\|\s+score=([0-9.]+))?"
    r"\n(.*?)(?=\[\d+\]|\Z)",
    re.DOTALL,
)


def _extract_sources(tool_outputs: list[str]) -> list[LogSource]:
    sources: list[LogSource] = []
    for output in tool_outputs:
        for m in _SOURCE_HEADER.finditer(output):
            raw_event_id = m.group(5)
            raw_score = m.group(6)
            sources.append(
                LogSource(
                    event_id=raw_event_id.strip() if raw_event_id else None,
                    log_level=m.group(2),
                    service_name=m.group(3),
                    timestamp=m.group(4).strip(),
                    score=float(raw_score) if raw_score is not None else None,
                    excerpt=m.group(7).strip()[:300],
                )
            )
    return sources


@router.post("/query", response_model=QueryResponse, summary="Query the SRE analyst agent")
async def query(request: Request, body: QueryRequest) -> QueryResponse:
    """Send a natural-language question to the ReAct agent.

    The agent will decide which tools to invoke (vector search and/or log
    statistics) and return a grounded answer with source citations.
    """
    try:
        result = await request.app.state.agent_graph.ainvoke(
            {"messages": [HumanMessage(content=body.prompt)]},
            config={"configurable": {"k": body.k}},
        )
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc

    messages = result.get("messages", [])

    # Collect tool output text from ToolMessage nodes for source extraction.
    tool_outputs: list[str] = [
        m.content
        for m in messages
        if hasattr(m, "type") and m.type == "tool" and isinstance(m.content, str)
    ]

    # The final AI message holds the agent's answer.
    final_answer = ""
    for m in reversed(messages):
        if isinstance(m, AIMessage) and isinstance(m.content, str) and m.content.strip():
            final_answer = m.content.strip()
            break

    if not final_answer:
        final_answer = "The agent did not produce a final answer."

    sources = _extract_sources(tool_outputs)

    return QueryResponse(answer=final_answer, sources=sources)


@router.post("/query/stream", summary="Stream the agent answer token by token")
async def query_stream(request: Request, body: QueryRequest) -> StreamingResponse:
    """Stream the agent's answer as Server-Sent Events (SSE).

    Each frame contains one token of the LLM's output:
        data: <token text>

    A final frame signals completion:
        data: [DONE]

    Useful for chat-style UIs that should display text as it is generated
    rather than waiting for the full response.
    """
    agent_graph = request.app.state.agent_graph

    async def event_generator():
        try:
            async for event in agent_graph.astream_events(
                {"messages": [HumanMessage(content=body.prompt)]},
                version="v2",
            ):
                if event["event"] == "on_chat_model_stream":
                    chunk = event["data"]["chunk"]
                    if isinstance(chunk.content, str) and chunk.content:
                        yield f"data: {chunk.content}\n\n"
        except Exception as exc:
            yield f"data: [ERROR] {exc}\n\n"
        finally:
            yield "data: [DONE]\n\n"

    return StreamingResponse(event_generator(), media_type="text/event-stream")
