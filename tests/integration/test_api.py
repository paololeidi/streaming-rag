"""Integration tests for FastAPI endpoints (/health, /api/v1/query, /api/v1/query/stream)."""

from __future__ import annotations

from unittest.mock import AsyncMock


def test_health_endpoint(client):
    """Test GET /health returns status ok."""
    response = client.get("/health")
    assert response.status_code == 200
    assert response.json() == {"status": "ok"}


def test_query_endpoint_success(client):
    """Test POST /api/v1/query parses agent output and returns structured response."""
    payload = {"prompt": "What caused payment service to fail?", "k": 5}
    response = client.post("/api/v1/query", json=payload)

    assert response.status_code == 200
    data = response.json()
    assert "answer" in data
    assert "payment" in data["answer"].lower()
    assert "sources" in data
    assert len(data["sources"]) == 1

    source = data["sources"][0]
    assert source["service_name"] == "payment-service"
    assert source["log_level"] == "ERROR"
    assert source["timestamp"] == "2026-10-10T12:05:00"
    assert "Database connection timeout" in source["excerpt"]


def test_query_endpoint_validation_errors(client):
    """Test POST /api/v1/query validates input constraints (empty prompt, invalid k)."""
    # Empty prompt
    resp_empty = client.post("/api/v1/query", json={"prompt": ""})
    assert resp_empty.status_code == 422

    # k too small (min 1)
    resp_k_zero = client.post("/api/v1/query", json={"prompt": "valid prompt", "k": 0})
    assert resp_k_zero.status_code == 422

    # k too large (max 20)
    resp_k_large = client.post("/api/v1/query", json={"prompt": "valid prompt", "k": 50})
    assert resp_k_large.status_code == 422


def test_query_endpoint_agent_exception(client):
    """Test POST /api/v1/query returns 500 when agent graph raises an exception."""
    client.app.state.agent_graph.ainvoke = AsyncMock(
        side_effect=RuntimeError("Ollama connection failed")
    )

    response = client.post(
        "/api/v1/query",
        json={"prompt": "System health status?"},
    )
    assert response.status_code == 500
    assert "Ollama connection failed" in response.json()["detail"]


def test_query_stream_endpoint_success(client):
    """Test POST /api/v1/query/stream streams tokens via SSE."""
    payload = {"prompt": "Stream payment errors"}
    response = client.post("/api/v1/query/stream", json=payload)

    assert response.status_code == 200
    assert "text/event-stream" in response.headers["content-type"]

    content = response.text
    assert "data: Payment " in content
    assert "data: service " in content
    assert "data: timeout." in content
    assert "data: [DONE]" in content


def test_query_stream_endpoint_error_handling(client):
    """Test POST /api/v1/query/stream handles stream errors gracefully."""

    async def failing_stream(inputs, version="v2"):
        raise RuntimeError("Stream interrupted")
        yield  # make it a generator

    client.app.state.agent_graph.astream_events = failing_stream

    response = client.post("/api/v1/query/stream", json={"prompt": "Test failure"})
    assert response.status_code == 200
    content = response.text
    assert "data: [ERROR] Stream interrupted" in content
    assert "data: [DONE]" in content
