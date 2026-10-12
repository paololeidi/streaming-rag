"""Integration tests for Kafka consumer message processing and ingestion."""

from __future__ import annotations

from unittest.mock import MagicMock

import pytest
from confluent_kafka import KafkaError, KafkaException

import consumer
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
    await consumer.process_message(mock_msg, sample_info_log, mock_consumer, mock_embedder)

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

    await consumer.process_message(mock_msg, log_without_id, mock_consumer, mock_embedder)

    event_id, _, metadata, _ = mock_upsert.call_args[0]
    assert event_id == "kafka-0-450"
    assert metadata["partition"] == 0
    assert metadata["offset"] == 450


@pytest.mark.asyncio
async def test_run_consumer_loop(monkeypatch, sample_raw_kafka_payload):
    """Test run_consumer handles timeouts, EOF, tombstones, validation errors, and clean shutdown."""
    mock_kafka_consumer = MagicMock()
    monkeypatch.setattr(consumer, "Consumer", lambda conf: mock_kafka_consumer)

    # Prevent SentenceTransformer model load
    monkeypatch.setattr(
        consumer.HuggingFaceEmbedder,
        "__init__",
        lambda self, model_name=None: None,
    )

    # 1. Message: None (timeout)
    # 2. Message: Partition EOF error
    eof_err = MagicMock()
    eof_err.code.return_value = KafkaError._PARTITION_EOF
    msg_eof = MagicMock()
    msg_eof.error.return_value = eof_err

    # 3. Message: Tombstone (value is None)
    msg_tombstone = MagicMock()
    msg_tombstone.error.return_value = None
    msg_tombstone.value.return_value = None

    # 4. Message: Corrupt payload (ValidationError)
    msg_corrupt = MagicMock()
    msg_corrupt.error.return_value = None
    msg_corrupt.value.return_value = b'{"service_name": ""}'
    msg_corrupt.offset.return_value = 10
    msg_corrupt.partition.return_value = 0

    # 5. Message: Valid payload
    msg_valid = MagicMock()
    msg_valid.error.return_value = None
    msg_valid.value.return_value = sample_raw_kafka_payload

    # 6. Message: Valid payload where process_message raises Exception
    msg_fail = MagicMock()
    msg_fail.error.return_value = None
    msg_fail.value.return_value = sample_raw_kafka_payload
    msg_fail.offset.return_value = 12
    msg_fail.partition.return_value = 0

    call_count = 0

    async def mock_process(msg, log, c, emb):
        nonlocal call_count
        call_count += 1
        if call_count == 2:
            raise RuntimeError("Database unavailable")

    monkeypatch.setattr(consumer, "process_message", mock_process)

    mock_kafka_consumer.poll.side_effect = [
        None,
        msg_eof,
        msg_tombstone,
        msg_corrupt,
        msg_valid,
        msg_fail,
        KeyboardInterrupt(),
    ]

    await consumer.run_consumer()

    # Verify corrupt message was committed
    mock_kafka_consumer.commit.assert_called_with(message=msg_corrupt, asynchronous=False)
    # Verify consumer was closed
    mock_kafka_consumer.close.assert_called_once()


@pytest.mark.asyncio
async def test_run_consumer_unrecoverable_error(monkeypatch):
    """Test run_consumer raises KafkaException on critical broker error."""
    mock_kafka_consumer = MagicMock()
    monkeypatch.setattr(consumer, "Consumer", lambda conf: mock_kafka_consumer)
    monkeypatch.setattr(
        consumer.HuggingFaceEmbedder,
        "__init__",
        lambda self, model_name=None: None,
    )

    crit_err = MagicMock()
    crit_err.code.return_value = -199  # non-EOF error
    msg_err = MagicMock()
    msg_err.error.return_value = crit_err

    mock_kafka_consumer.poll.return_value = msg_err

    with pytest.raises(KafkaException):
        await consumer.run_consumer()

    mock_kafka_consumer.close.assert_called_once()


def test_consumer_main(monkeypatch):
    """Test consumer main() calls asyncio.run with run_consumer."""
    mock_run = MagicMock(side_effect=lambda coro: coro.close())
    monkeypatch.setattr(consumer.asyncio, "run", mock_run)

    consumer.main()
    mock_run.assert_called_once()
