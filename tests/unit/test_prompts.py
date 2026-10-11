"""Unit tests for agent system prompt structure and constraints."""

from __future__ import annotations

from agent.prompts import SYSTEM_PROMPT


def test_system_prompt_structure_and_tools():
    """Verify system prompt specifies core tools and reasoning instructions."""
    assert isinstance(SYSTEM_PROMPT, str)
    assert len(SYSTEM_PROMPT) > 200

    # Verify tool mentions
    assert "vector_search" in SYSTEM_PROMPT
    assert "log_stats" in SYSTEM_PROMPT


def test_system_prompt_guardrails():
    """Verify prompt includes essential grounding and anti-hallucination guardrails."""
    # Source citation requirements
    assert "service_name" in SYSTEM_PROMPT
    assert "log_level" in SYSTEM_PROMPT
    assert "timestamp" in SYSTEM_PROMPT

    # Hallucination guardrails
    assert "NEVER invent, extrapolate, or hallucinate" in SYSTEM_PROMPT
    assert "Do not answer from memory alone" in SYSTEM_PROMPT
