"""Unit tests for MCP server tools and lifecycle."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from unittest.mock import MagicMock

import mcp_server.server as server


def test_mcp_get_embedder_caching(monkeypatch):
    """Test _get_embedder lazily creates and caches embedder instance."""
    server._embedder = None
    mock_instance = MagicMock()
    mock_class = MagicMock(return_value=mock_instance)
    monkeypatch.setattr(server, "HuggingFaceEmbedder", mock_class)

    emb1 = server._get_embedder()
    emb2 = server._get_embedder()

    assert emb1 is mock_instance
    assert emb2 is mock_instance
    mock_class.assert_called_once()


def test_mcp_vector_search_with_results(monkeypatch):
    """Test MCP vector_search tool formats matching log entries with event_id and score."""
    mock_emb = MagicMock()
    mock_emb.embed.return_value = [0.1, 0.2]
    monkeypatch.setattr(server, "_get_embedder", lambda: mock_emb)

    fake_results = {
        "ids": [["evt-999"]],
        "distances": [[0.25]],
        "documents": [
            [
                "Connection reset by peer on auth-service",
            ]
        ],
        "metadatas": [
            [
                {
                    "event_id": "evt-999",
                    "service_name": "auth-service",
                    "log_level": "ERROR",
                    "timestamp": "2026-10-12T10:00:00+00:00",
                }
            ]
        ],
    }
    monkeypatch.setattr(server, "query_similar", lambda emb, k: fake_results)

    result = server.vector_search.fn("auth reset", k=1)
    assert (
        "[1] ERROR | auth-service @ 2026-10-12T10:00:00+00:00 | event_id=evt-999 | score=0.800"
        in result
    )
    assert "Connection reset by peer on auth-service" in result


def test_mcp_vector_search_no_results(monkeypatch):
    """Test MCP vector_search tool returns clean message when vector DB returns nothing."""
    mock_emb = MagicMock()
    mock_emb.embed.return_value = [0.1, 0.2]
    monkeypatch.setattr(server, "_get_embedder", lambda: mock_emb)
    monkeypatch.setattr(
        server, "query_similar", lambda emb, k: {"documents": [[]], "metadatas": [[]]}
    )

    result = server.vector_search.fn("non-existent query")
    assert result == "No similar log entries found in the vector store."


def test_mcp_log_stats_filters(monkeypatch):
    """Test MCP log_stats with both log_level and service_name filters."""
    now = datetime.now(timezone.utc)
    mock_coll = MagicMock()
    mock_coll.get.return_value = {
        "metadatas": [
            {
                "service_name": "payment-service",
                "log_level": "ERROR",
                "timestamp": now.replace(tzinfo=None).isoformat(),  # naive datetime
            },
            {
                "service_name": "payment-service",
                "log_level": "ERROR",
                "timestamp": (now - timedelta(minutes=10)).isoformat(),
            },
            {
                "service_name": "payment-service",
                "log_level": "ERROR",
                "timestamp": (now - timedelta(minutes=120)).isoformat(),  # out of window
            },
            {
                "service_name": "payment-service",
                "log_level": "ERROR",
                "timestamp": "corrupt-iso-ts",  # invalid ts
            },
        ]
    }
    monkeypatch.setattr(server, "get_collection", lambda: mock_coll)

    result = server.log_stats.fn(log_level="error", service_name="payment-service", minutes=60)
    assert "Total entries: 2" in result
    assert "ERROR: 2" in result
    assert "payment-service: 2" in result

    _, kwargs = mock_coll.get.call_args
    assert "$and" in kwargs["where"]


def test_mcp_log_stats_single_level(monkeypatch):
    """Test MCP log_stats when filtering only by log_level."""
    now = datetime.now(timezone.utc)
    mock_coll = MagicMock()
    mock_coll.get.return_value = {
        "metadatas": [{"service_name": "db", "log_level": "WARN", "timestamp": now.isoformat()}]
    }
    monkeypatch.setattr(server, "get_collection", lambda: mock_coll)

    result = server.log_stats.fn(log_level="warn")
    assert "WARN: 1" in result
    _, kwargs = mock_coll.get.call_args
    assert kwargs["where"] == {"log_level": {"$eq": "WARN"}}


def test_mcp_log_stats_single_service(monkeypatch):
    """Test MCP log_stats when filtering only by service_name."""
    now = datetime.now(timezone.utc)
    mock_coll = MagicMock()
    mock_coll.get.return_value = {
        "metadatas": [{"service_name": "auth", "log_level": "INFO", "timestamp": now.isoformat()}]
    }
    monkeypatch.setattr(server, "get_collection", lambda: mock_coll)

    result = server.log_stats.fn(service_name="auth")
    assert "auth: 1" in result
    _, kwargs = mock_coll.get.call_args
    assert kwargs["where"] == {"service_name": {"$eq": "auth"}}


def test_mcp_log_stats_empty_window(monkeypatch):
    """Test MCP log_stats returns clean message when no logs fall in look-back window."""
    mock_coll = MagicMock()
    mock_coll.get.return_value = {"metadatas": []}
    monkeypatch.setattr(server, "get_collection", lambda: mock_coll)

    result = server.log_stats.fn(log_level="ERROR", service_name="gateway", minutes=10)
    assert "No log entries found in the last 10 minute(s)" in result
    assert "level=ERROR" in result
    assert "service=gateway" in result
