"""Integration tests for Kafka consumer message processing and ingestion."""

from __future__ import annotations

from unittest.mock import MagicMock

import pytest

from consumer import process_message
from model.models import LogLevel, SystemLog


@pytest.mark.asyncio
async def test_process_message_pipeline(sample_info_log, monkeypatch):
    """Test full message processing: chunking, embedding, vector DB upsert, and commit."""
    # 1. Mock Kafka message
    mock_msg = MagicMock()
    mock_msg.partition.return_value = 1
    mock_msg.offset.return_value = 100

    # 2. Mock consumer
    mock_consumer = MagicMock()

    # 3. Mock embedder
    mock_embedder = MagicMock()
    mock_embedder.embed.return_value = [0.1, 0.2, 0.3]

    # 4. Mock vector store upsert
    mock_upsert = MagicMock()
    monkeypatch.setattr("consumer.upsert_log", mock_upsert)

    # Execute
    await process_message(mock_msg, sample_info_log, mock_consumer, mock_embedder)

    # Assertions
    mock_embedder.embed.assert_called_once_with(sample_info_log.to_embedding_text())

    mock_upsert.assert_called_once()
    event_id, embedding, metadata, text = mock_upsert.call_args[0]
    assert event_id == str(sample_info_log.event_id)
    assert embedding == [0.1, 0.2, 0.3]
    assert metadata["service_name"] == sample_info_log.service_name
    assert metadata["log_level"] == sample_info_log.log_level.value
    assert metadata["partition"] == 1
    assert metadata["offset"] == 100
    assert text == sample_info_log.to_embedding_text()

    # Verify commit
    mock_consumer.commit.assert_called_once_with(message=mock_msg, asynchronous=False)


@pytest.mark.asyncio
async def test_process_message_fallback_id(monkeypatch):
    """Test message processing uses kafka-partition-offset id when event_id is missing."""
    log_without_id = SystemLog(
        service_name="payment-service",
        log_level=LogLevel.ERROR,
        message="Gateway unreachable",
        event_id=None,
    )

    mock_msg = MagicMock()
    mock_msg.partition.return_value = 0
    mock_msg.offset.return_value = 450

    mock_consumer = MagicMock()
    mock_embedder = MagicMock()
    mock_embedder.embed.return_value = [0.5, 0.6]

    mock_upsert = MagicMock()
    monkeypatch.setattr("consumer.upsert_log", mock_upsert)

    await process_message(mock_msg, log_without_id, mock_consumer, mock_embedder)

    event_id, _, metadata, _ = mock_upsert.call_args[0]
    assert event_id == "kafka-0-450"
    assert metadata["partition"] == 0
    assert metadata["offset"] == 450
