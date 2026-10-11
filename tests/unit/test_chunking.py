"""Unit tests for chunking module."""

from __future__ import annotations

from chunking import LogChunk, chunk_log
from model.models import LogLevel, SystemLog


def test_chunk_log_with_explicit_event_id(sample_info_log):
    """Test chunk_log when event_id is explicitly set on SystemLog."""
    chunk = chunk_log(sample_info_log, partition=2, offset=105)

    assert isinstance(chunk, LogChunk)
    assert chunk.event_id == str(sample_info_log.event_id)
    assert chunk.text == sample_info_log.to_embedding_text()
    assert chunk.metadata == {
        "service_name": sample_info_log.service_name,
        "log_level": sample_info_log.log_level.value,
        "timestamp": sample_info_log.timestamp,
        "partition": 2,
        "offset": 105,
    }


def test_chunk_log_fallback_event_id():
    """Test chunk_log fallback to kafka partition-offset when event_id is None."""
    log = SystemLog(
        service_name="inventory-service",
        log_level=LogLevel.WARN,
        message="Stock level critically low",
        event_id=None,
    )

    chunk = chunk_log(log, partition=3, offset=999)

    assert chunk.event_id == "kafka-3-999"
    assert chunk.metadata["partition"] == 3
    assert chunk.metadata["offset"] == 999
    assert chunk.metadata["service_name"] == "inventory-service"
