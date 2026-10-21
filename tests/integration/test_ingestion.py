"""Integration tests for Kafka consumer message processing, retry resilience, and DLQ routing."""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock

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
    assert metadata["event_id"] == str(sample_info_log.event_id)
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
    assert metadata["event_id"] == "kafka-0-450"
    assert metadata["partition"] == 0
    assert metadata["offset"] == 450


@pytest.mark.asyncio
async def test_process_message_retry_then_succeed(sample_info_log, monkeypatch):
    """Test process_message retries on transient failure and succeeds on subsequent attempt."""
    mock_msg = MagicMock()
    mock_msg.partition.return_value = 0
    mock_msg.offset.return_value = 200

    mock_consumer = MagicMock()
    mock_embedder = MagicMock()
    mock_embedder.embed.return_value = [0.1, 0.2]

    attempts = 0

    def flaky_upsert(*args):
        nonlocal attempts
        attempts += 1
        if attempts == 1:
            raise RuntimeError("Transient Chroma HTTP timeout")

    monkeypatch.setattr("consumer.upsert_log", flaky_upsert)
    mock_sleep = AsyncMock()
    monkeypatch.setattr("consumer.asyncio.sleep", mock_sleep)

    await consumer.process_message(
        mock_msg,
        sample_info_log,
        mock_consumer,
        mock_embedder,
        max_retries=2,
        retry_backoff_sec=0.5,
    )

    assert attempts == 2
    mock_sleep.assert_called_once_with(0.5)
    mock_consumer.commit.assert_called_once_with(message=mock_msg, asynchronous=False)


@pytest.mark.asyncio
async def test_process_message_exhausts_retries_routes_to_dlq(sample_info_log, monkeypatch):
    """Test process_message routes to DLQ and commits offset when all retries are exhausted."""
    mock_msg = MagicMock()
    mock_msg.topic.return_value = "system-logs"
    mock_msg.partition.return_value = 2
    mock_msg.offset.return_value = 305
    mock_msg.key.return_value = b"payment-service"
    mock_msg.value.return_value = b'{"message": "raw"}'

    mock_consumer = MagicMock()
    mock_embedder = MagicMock()
    mock_embedder.embed.side_effect = RuntimeError("Persistent embedding failure")
    mock_dlq_producer = MagicMock()

    mock_sleep = AsyncMock()
    monkeypatch.setattr("consumer.asyncio.sleep", mock_sleep)

    await consumer.process_message(
        mock_msg,
        sample_info_log,
        mock_consumer,
        mock_embedder,
        dlq_producer=mock_dlq_producer,
        max_retries=2,
        retry_backoff_sec=0.1,
    )

    assert mock_embedder.embed.call_count == 3
    assert mock_sleep.call_count == 2
    mock_dlq_producer.produce.assert_called_once()
    _, kwargs = mock_dlq_producer.produce.call_args
    assert kwargs["key"] == b"payment-service"
    assert kwargs["value"] == b'{"message": "raw"}'
    header_dict = dict(kwargs["headers"])
    assert b"Persistent embedding failure" in header_dict["dlq_reason"]
    assert header_dict["original_partition"] == b"2"
    assert header_dict["original_offset"] == b"305"
    mock_consumer.commit.assert_called_once_with(message=mock_msg, asynchronous=False)


@pytest.mark.asyncio
async def test_process_message_exhausts_retries_raises_when_no_dlq(sample_info_log, monkeypatch):
    """Test process_message re-raises exception when retries exhaust and dlq_producer is None."""
    mock_msg = MagicMock()
    mock_msg.partition.return_value = 0
    mock_msg.offset.return_value = 10

    mock_consumer = MagicMock()
    mock_embedder = MagicMock()
    mock_embedder.embed.side_effect = RuntimeError("Fatal error")
    monkeypatch.setattr("consumer.asyncio.sleep", AsyncMock())

    with pytest.raises(RuntimeError, match="Fatal error"):
        await consumer.process_message(
            mock_msg,
            sample_info_log,
            mock_consumer,
            mock_embedder,
            dlq_producer=None,
            max_retries=1,
        )


@pytest.mark.asyncio
async def test_run_consumer_loop(monkeypatch, sample_raw_kafka_payload):
    """Test run_consumer handles timeouts, EOF, tombstones, validation errors, DLQ, and shutdown."""
    mock_kafka_consumer = MagicMock()
    mock_dlq_producer = MagicMock()
    monkeypatch.setattr(consumer, "Consumer", lambda conf: mock_kafka_consumer)
    monkeypatch.setattr(consumer, "Producer", lambda conf: mock_dlq_producer)

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
    msg_corrupt.topic.return_value = "system-logs"
    msg_corrupt.key.return_value = b"corrupt-key"
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
    msg_fail.topic.return_value = "system-logs"
    msg_fail.key.return_value = b"user-auth"
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

    # Verify corrupt message was routed to DLQ and committed
    assert mock_dlq_producer.produce.call_count == 2
    mock_kafka_consumer.commit.assert_called_with(message=msg_corrupt, asynchronous=False)
    mock_dlq_producer.flush.assert_called_once()
    # Verify consumer was closed
    mock_kafka_consumer.close.assert_called_once()


@pytest.mark.asyncio
async def test_run_consumer_unrecoverable_error(monkeypatch):
    """Test run_consumer raises KafkaException on critical broker error."""
    mock_kafka_consumer = MagicMock()
    mock_dlq_producer = MagicMock()
    monkeypatch.setattr(consumer, "Consumer", lambda conf: mock_kafka_consumer)
    monkeypatch.setattr(consumer, "Producer", lambda conf: mock_dlq_producer)
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
