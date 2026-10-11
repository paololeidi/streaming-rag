"""Shared pytest fixtures for unit and integration tests."""

from __future__ import annotations

import uuid
from datetime import datetime, timezone
from unittest.mock import AsyncMock, MagicMock

import pytest
from fastapi.testclient import TestClient
from langchain_core.messages import AIMessage, ToolMessage

from api.main import app
from model.models import LogLevel, SystemLog


@pytest.fixture
def sample_info_log() -> SystemLog:
    """Fixture providing a valid INFO level SystemLog."""
    return SystemLog(
        event_id=uuid.UUID("11111111-1111-1111-1111-111111111111"),
        timestamp="2026-10-10T12:00:00+00:00",
        service_name="payment-service",
        log_level=LogLevel.INFO,
        message="Payment processed successfully for order #12345",
        stack_trace=None,
    )


@pytest.fixture
def sample_error_log() -> SystemLog:
    """Fixture providing a valid ERROR level SystemLog with stack trace."""
    return SystemLog(
        event_id=uuid.UUID("22222222-2222-2222-2222-222222222222"),
        timestamp="2026-10-10T12:05:00+00:00",
        service_name="payment-service",
        log_level=LogLevel.ERROR,
        message="Database connection timeout while charging customer",
        stack_trace="Traceback (most recent call last):\n  File 'db.py', line 42: TimeoutError",
    )


@pytest.fixture
def sample_raw_kafka_payload() -> bytes:
    """Fixture providing a valid raw JSON byte string for Kafka message."""
    return b"""{
        "event_id": "33333333-3333-3333-3333-333333333333",
        "timestamp": "2026-10-10T12:10:00+00:00",
        "service_name": "user-auth",
        "log_level": "WARN",
        "message": "Token refresh threshold exceeded",
        "stack_trace": null
    }"""


@pytest.fixture
def mock_chroma_collection(monkeypatch):
    """Fixture providing a mocked ChromaDB Collection."""
    mock_coll = MagicMock()
    mock_coll.count.return_value = 10
    mock_coll.get.return_value = {
        "metadatas": [
            {
                "service_name": "payment-service",
                "log_level": "ERROR",
                "timestamp": datetime.now(timezone.utc).isoformat(),
            },
            {
                "service_name": "payment-service",
                "log_level": "INFO",
                "timestamp": datetime.now(timezone.utc).isoformat(),
            },
            {
                "service_name": "user-auth",
                "log_level": "WARN",
                "timestamp": datetime.now(timezone.utc).isoformat(),
            },
        ]
    }
    mock_coll.query.return_value = {
        "documents": [
            [
                "[ERROR] payment-service @ 2026-10-10T12:05:00\nDatabase connection timeout",
                "[WARN] user-auth @ 2026-10-10T12:10:00\nToken refresh threshold exceeded",
            ]
        ],
        "metadatas": [
            [
                {
                    "service_name": "payment-service",
                    "log_level": "ERROR",
                    "timestamp": "2026-10-10T12:05:00+00:00",
                },
                {
                    "service_name": "user-auth",
                    "log_level": "WARN",
                    "timestamp": "2026-10-10T12:10:00+00:00",
                },
            ]
        ],
    }

    monkeypatch.setattr("vector_store.get_collection", lambda: mock_coll)
    monkeypatch.setattr("agent.tools.get_collection", lambda: mock_coll)
    return mock_coll


@pytest.fixture
def mock_agent_graph():
    """Mock LangGraph ReAct agent graph for testing API endpoints."""
    mock_graph = MagicMock()

    # Default ainvoke behavior: returns tool call output and an AI answer
    async def fake_ainvoke(inputs, config=None):
        return {
            "messages": [
                ToolMessage(
                    content="[1] ERROR | payment-service @ 2026-10-10T12:05:00\nDatabase connection timeout",
                    tool_call_id="call_1",
                ),
                AIMessage(content="Payment service crashed due to a database connection timeout."),
            ]
        }

    mock_graph.ainvoke = AsyncMock(side_effect=fake_ainvoke)

    # Default astream_events behavior: yields streaming chunks
    async def fake_astream_events(inputs, version="v2"):
        chunks = ["Payment ", "service ", "timeout."]
        for chunk in chunks:
            yield {
                "event": "on_chat_model_stream",
                "data": {"chunk": MagicMock(content=chunk)},
            }

    mock_graph.astream_events = fake_astream_events

    return mock_graph


@pytest.fixture
def client(mock_agent_graph, monkeypatch) -> TestClient:
    """FastAPI TestClient with mocked background lifespan and agent graph."""
    # Prevent real SentenceTransformer embedding and Ollama graph compile during lifespan
    monkeypatch.setattr("embeddings.HuggingFaceEmbedder.embed", lambda self, text: [0.1] * 384)
    monkeypatch.setattr("api.main.MCP_SERVER_URL", "")

    async def fake_build_agent_graph(extra_tools=None):
        return mock_agent_graph

    monkeypatch.setattr("api.main.build_agent_graph", fake_build_agent_graph)

    with TestClient(app) as test_client:
        test_client.app.state.agent_graph = mock_agent_graph
        yield test_client
