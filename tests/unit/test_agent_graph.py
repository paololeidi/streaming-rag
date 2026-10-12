"""Unit tests for LangGraph agent builder."""

from __future__ import annotations

from unittest.mock import MagicMock

import pytest

import agent.graph as graph
from agent.tools import log_stats, vector_search


@pytest.mark.asyncio
async def test_build_agent_graph_native_tools(monkeypatch):
    """Test build_agent_graph compiles agent with native tools when extra_tools is None."""
    mock_create = MagicMock(return_value="compiled_graph")
    monkeypatch.setattr(graph, "create_react_agent", mock_create)

    result = await graph.build_agent_graph()

    assert result == "compiled_graph"
    _, kwargs = mock_create.call_args
    assert kwargs["tools"] == [vector_search, log_stats]
    assert kwargs["name"] == "sre-analyst"


@pytest.mark.asyncio
async def test_build_agent_graph_extra_tools(monkeypatch):
    """Test build_agent_graph appends extra MCP tools to the agent toolbox."""
    mock_create = MagicMock(return_value="compiled_graph")
    monkeypatch.setattr(graph, "create_react_agent", mock_create)

    dummy_mcp_tool = MagicMock(name="github_pr_tool")
    result = await graph.build_agent_graph(extra_tools=[dummy_mcp_tool])

    assert result == "compiled_graph"
    _, kwargs = mock_create.call_args
    assert kwargs["tools"] == [vector_search, log_stats, dummy_mcp_tool]
