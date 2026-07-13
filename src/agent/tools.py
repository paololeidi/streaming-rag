"""LangChain tools available to the ReAct agent."""

from __future__ import annotations

import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Optional

from langchain_core.tools import tool

# Allow imports from the sibling src/ modules when running from the repo root.
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from embeddings import HuggingFaceEmbedder
from vector_store import get_collection, query_similar

_embedder = HuggingFaceEmbedder()


@tool
def vector_search(query: str, k: int = 5) -> str:
    """Search the vector store for log entries semantically similar to *query*.

    Use this tool for questions about specific errors, root-cause analysis, or
    any query that benefits from finding contextually similar past log events.

    Args:
        query: Natural-language description of what you are looking for.
        k:     Maximum number of results to return (default 5).

    Returns:
        A formatted list of the most relevant log entries with their metadata,
        or a message indicating that no results were found.
    """
    embedding = _embedder.embed(query)
    results = query_similar(embedding, k=k)

    documents: list[str] = results.get("documents", [[]])[0]
    metadatas: list[dict] = results.get("metadatas", [[]])[0]

    if not documents:
        return "No similar log entries found in the vector store."

    lines: list[str] = []
    for i, (doc, meta) in enumerate(zip(documents, metadatas), start=1):
        service = meta.get("service_name", "unknown")
        level = meta.get("log_level", "?")
        ts = meta.get("timestamp", "?")
        doc_preview = doc[:400] + "..." if len(doc) > 400 else doc
        lines.append(f"[{i}] {level} | {service} @ {ts}\n{doc_preview}")

    return "\n\n".join(lines)


@tool
def log_stats(
    log_level: Optional[str] = None,
    service_name: Optional[str] = None,
    minutes: int = 60,
) -> str:
    """Return live counts and distributions of log entries stored in ChromaDB.

    Use this tool for questions about trends, frequencies, or distributions —
    e.g. "how many errors in the last hour?" or "which services are logging the most?".

    Args:
        log_level:    Filter by log level ("INFO", "WARN", "ERROR").
                      Pass None to include all levels.
        service_name: Filter by a specific service name.
                      Pass None to include all services.
        minutes:      Look-back window in minutes from now (default 60).

    Returns:
        A summary showing total count, breakdown by log level, and breakdown
        by service within the requested time window.
    """
    collection = get_collection()

    where_clause: dict = {}
    if log_level and service_name:
        where_clause = {
            "$and": [
                {"log_level": {"$eq": log_level.upper()}},
                {"service_name": {"$eq": service_name}},
            ]
        }
    elif log_level:
        where_clause = {"log_level": {"$eq": log_level.upper()}}
    elif service_name:
        where_clause = {"service_name": {"$eq": service_name}}

    get_kwargs: dict = {"include": ["metadatas"]}
    if where_clause:
        get_kwargs["where"] = where_clause

    result = collection.get(**get_kwargs)
    metadatas: list[dict] = result.get("metadatas") or []

    cutoff = datetime.now(timezone.utc) - timedelta(minutes=minutes)

    filtered: list[dict] = []
    for meta in metadatas:
        ts_str = meta.get("timestamp", "")
        try:
            ts = datetime.fromisoformat(ts_str)
            if ts.tzinfo is None:
                ts = ts.replace(tzinfo=timezone.utc)
            if ts >= cutoff:
                filtered.append(meta)
        except ValueError:
            pass

    if not filtered:
        return (
            f"No log entries found in the last {minutes} minute(s)"
            + (f" for level={log_level}" if log_level else "")
            + (f", service={service_name}" if service_name else "")
            + "."
        )

    level_counts: dict[str, int] = {}
    service_counts: dict[str, int] = {}
    for meta in filtered:
        lvl = meta.get("log_level", "UNKNOWN")
        svc = meta.get("service_name", "unknown")
        level_counts[lvl] = level_counts.get(lvl, 0) + 1
        service_counts[svc] = service_counts.get(svc, 0) + 1

    level_breakdown = ", ".join(
        f"{lvl}: {cnt}" for lvl, cnt in sorted(level_counts.items())
    )
    service_breakdown = "\n".join(
        f"  {svc}: {cnt}" for svc, cnt in sorted(service_counts.items(), key=lambda x: -x[1])
    )

    return (
        f"Log statistics for the last {minutes} minute(s):\n"
        f"  Total entries: {len(filtered)}\n"
        f"  By level: {level_breakdown}\n"
        f"  By service:\n{service_breakdown}"
    )
