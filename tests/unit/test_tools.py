"""Unit tests for LangChain tools (vector_search and log_stats)."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from unittest.mock import MagicMock

from agent.tools import log_stats, vector_search


def test_vector_search_with_results(monkeypatch):
    """Test vector_search formatting when ChromaDB returns matched documents."""
    monkeypatch.setattr("agent.tools._embedder.embed", lambda query: [0.1, 0.2, 0.3])

    fake_results = {
        "documents": [
            [
                "Connection timeout to database for payment-gateway",
                "Authentication token expired for user-auth",
            ]
        ],
        "metadatas": [
            [
                {
                    "service_name": "payment-gateway",
                    "log_level": "ERROR",
                    "timestamp": "2026-10-10T12:00:00+00:00",
                },
                {
                    "service_name": "user-auth",
                    "log_level": "WARN",
                    "timestamp": "2026-10-10T12:05:00+00:00",
                },
            ]
        ],
    }
    monkeypatch.setattr("agent.tools.query_similar", lambda emb, k: fake_results)

    result = vector_search.invoke({"query": "database timeout", "k": 2})

    assert "[1] ERROR | payment-gateway @ 2026-10-10T12:00:00+00:00" in result
    assert "Connection timeout to database for payment-gateway" in result
    assert "[2] WARN | user-auth @ 2026-10-10T12:05:00+00:00" in result


def test_vector_search_no_results(monkeypatch):
    """Test vector_search behavior when no documents match."""
    monkeypatch.setattr("agent.tools._embedder.embed", lambda query: [0.1, 0.2, 0.3])
    monkeypatch.setattr(
        "agent.tools.query_similar", lambda emb, k: {"documents": [[]], "metadatas": [[]]}
    )

    result = vector_search.invoke({"query": "non-existent issue"})
    assert result == "No similar log entries found in the vector store."


def test_log_stats_with_level_and_service_filters(monkeypatch):
    """Test log_stats with both log_level and service_name filters applied."""
    now = datetime.now(timezone.utc)
    mock_coll = MagicMock()
    mock_coll.get.return_value = {
        "metadatas": [
            {
                "service_name": "payment-service",
                "log_level": "ERROR",
                "timestamp": now.isoformat(),
            },
            {
                "service_name": "payment-service",
                "log_level": "ERROR",
                "timestamp": (now - timedelta(minutes=5)).isoformat(),
            },
        ]
    }
    monkeypatch.setattr("agent.tools.get_collection", lambda: mock_coll)

    result = log_stats.invoke(
        {
            "log_level": "error",
            "service_name": "payment-service",
            "minutes": 30,
        }
    )

    assert "Log statistics for the last 30 minute(s):" in result
    assert "Total entries: 2" in result
    assert "ERROR: 2" in result
    assert "payment-service: 2" in result

    # Verify Chroma get where_clause construction
    _, kwargs = mock_coll.get.call_args
    assert "$and" in kwargs["where"]
    assert {"log_level": {"$eq": "ERROR"}} in kwargs["where"]["$and"]
    assert {"service_name": {"$eq": "payment-service"}} in kwargs["where"]["$and"]


def test_log_stats_filters_old_timestamps(monkeypatch):
    """Test log_stats filters out logs older than the look-back window."""
    now = datetime.now(timezone.utc)
    mock_coll = MagicMock()
    mock_coll.get.return_value = {
        "metadatas": [
            {
                "service_name": "recent-service",
                "log_level": "INFO",
                "timestamp": now.isoformat(),
            },
            {
                "service_name": "old-service",
                "log_level": "ERROR",
                "timestamp": (now - timedelta(minutes=120)).isoformat(),  # 2 hours old
            },
            {
                "service_name": "corrupt-service",
                "log_level": "WARN",
                "timestamp": "invalid-timestamp",
            },
        ]
    }
    monkeypatch.setattr("agent.tools.get_collection", lambda: mock_coll)

    result = log_stats.invoke({"minutes": 60})

    assert "Total entries: 1" in result
    assert "recent-service" in result
    assert "old-service" not in result


def test_log_stats_empty_window(monkeypatch):
    """Test log_stats returns clean message when no records fall within the window."""
    mock_coll = MagicMock()
    mock_coll.get.return_value = {"metadatas": []}
    monkeypatch.setattr("agent.tools.get_collection", lambda: mock_coll)

    result = log_stats.invoke({"minutes": 15, "service_name": "billing"})
    assert "No log entries found in the last 15 minute(s)" in result
    assert "service=billing" in result


def test_log_stats_single_filter_level(monkeypatch):
    """Test log_stats where-clause when filtering only by log_level."""
    now = datetime.now(timezone.utc)
    mock_coll = MagicMock()
    mock_coll.get.return_value = {
        "metadatas": [{"service_name": "auth", "log_level": "WARN", "timestamp": now.isoformat()}]
    }
    monkeypatch.setattr("agent.tools.get_collection", lambda: mock_coll)

    result = log_stats.invoke({"log_level": "warn"})
    assert "WARN: 1" in result
    _, kwargs = mock_coll.get.call_args
    assert kwargs["where"] == {"log_level": {"$eq": "WARN"}}


def test_log_stats_single_filter_service(monkeypatch):
    """Test log_stats where-clause when filtering only by service_name."""
    now = datetime.now(timezone.utc)
    mock_coll = MagicMock()
    mock_coll.get.return_value = {
        "metadatas": [{"service_name": "db", "log_level": "INFO", "timestamp": now.isoformat()}]
    }
    monkeypatch.setattr("agent.tools.get_collection", lambda: mock_coll)

    result = log_stats.invoke({"service_name": "db"})
    assert "db: 1" in result
    _, kwargs = mock_coll.get.call_args
    assert kwargs["where"] == {"service_name": {"$eq": "db"}}
