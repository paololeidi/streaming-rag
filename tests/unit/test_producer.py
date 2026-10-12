"""Unit tests for Kafka log producer module."""

from __future__ import annotations

from unittest.mock import MagicMock

import producer
from model.models import LogLevel, SystemLog


def test_delivery_report_success(capsys):
    """Test delivery_report callback when message delivery succeeds."""
    mock_msg = MagicMock()
    mock_msg.topic.return_value = "system-logs"
    mock_msg.partition.return_value = 2

    producer.delivery_report(None, mock_msg)
    captured = capsys.readouterr().out
    assert "Log sent to system-logs [2]" in captured


def test_delivery_report_error(capsys):
    """Test delivery_report callback when message delivery fails."""
    mock_msg = MagicMock()
    producer.delivery_report("Broker connection timeout", mock_msg)
    captured = capsys.readouterr().out
    assert "Delivery error: Broker connection timeout" in captured


def test_generate_mock_log_structure(monkeypatch):
    """Test generate_mock_log returns valid SystemLog instances."""
    log = producer.generate_mock_log()
    assert isinstance(log, SystemLog)
    assert log.service_name in [
        "payment-gateway",
        "user-auth",
        "inventory-service",
        "frontend-bff",
    ]
    assert log.log_level in [LogLevel.INFO, LogLevel.WARN, LogLevel.ERROR]
    assert log.event_id is not None
    assert len(log.message) > 0


def test_generate_mock_log_error_branch(monkeypatch):
    """Test generate_mock_log generates stack trace when level is ERROR."""
    monkeypatch.setattr(
        producer.random,
        "choice",
        lambda items: LogLevel.ERROR if LogLevel.ERROR in items else "payment-gateway",
    )

    log = producer.generate_mock_log()
    assert log.log_level == LogLevel.ERROR
    assert log.stack_trace is not None
    assert "Connection timeout to database for payment-gateway" in log.message


def test_generate_mock_log_info_branch(monkeypatch):
    """Test generate_mock_log omits stack trace when level is INFO."""
    monkeypatch.setattr(
        producer.random,
        "choice",
        lambda items: LogLevel.INFO if LogLevel.INFO in items else "user-auth",
    )

    log = producer.generate_mock_log()
    assert log.log_level == LogLevel.INFO
    assert log.stack_trace is None
    assert "Standard operation executed" in log.message


def test_producer_main_loop(monkeypatch, capsys):
    """Test producer main loop produces message, handles KeyboardInterrupt, and flushes."""
    mock_kafka_producer = MagicMock()
    monkeypatch.setattr(producer, "Producer", lambda conf: mock_kafka_producer)

    # Allow one loop iteration, then interrupt
    def mock_sleep(seconds):
        raise KeyboardInterrupt()

    monkeypatch.setattr(producer.time, "sleep", mock_sleep)

    producer.main()

    # Verify produce call
    mock_kafka_producer.produce.assert_called_once()
    mock_kafka_producer.poll.assert_called_once_with(0)
    mock_kafka_producer.flush.assert_called_once()

    captured = capsys.readouterr().out
    assert "Starting log stream..." in captured
    assert "Stopping producer..." in captured
