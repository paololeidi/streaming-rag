"""Unit tests for Kafka log producer module."""

from __future__ import annotations

from unittest.mock import MagicMock

import pytest

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


def test_generate_mock_log_structure():
    """Test generate_mock_log returns valid SystemLog instances."""
    log = producer.generate_mock_log()
    assert isinstance(log, SystemLog)
    assert log.service_name in producer.SERVICES
    assert log.log_level in [LogLevel.INFO, LogLevel.WARN, LogLevel.ERROR]
    assert log.event_id is not None
    assert len(log.message) > 0


def test_generate_mock_log_error_branch(monkeypatch):
    """Test generate_mock_log generates realistic stack trace when level is ERROR."""
    monkeypatch.setattr(
        producer.random,
        "choice",
        lambda items: LogLevel.ERROR if LogLevel.ERROR in items else "payment-gateway",
    )

    log = producer.generate_mock_log()
    assert log.log_level == LogLevel.ERROR
    assert log.service_name == "payment-gateway"
    assert log.stack_trace is not None
    assert "java.net.SocketTimeoutException" in log.stack_trace
    assert "StripeClient.charge" in log.stack_trace


def test_generate_mock_log_info_branch(monkeypatch):
    """Test generate_mock_log omits stack trace when level is INFO."""
    monkeypatch.setattr(
        producer.random,
        "choice",
        lambda items: LogLevel.INFO if LogLevel.INFO in items else "user-auth",
    )

    log = producer.generate_mock_log()
    assert log.log_level == LogLevel.INFO
    assert log.service_name == "user-auth"
    assert log.stack_trace is None
    assert "usr_" in log.message


@pytest.mark.parametrize("service", producer.SERVICES)
@pytest.mark.parametrize("level", [LogLevel.INFO, LogLevel.WARN, LogLevel.ERROR])
def test_generate_mock_log_all_archetypes(service: str, level: LogLevel):
    """Test all service and log level combinations produce realistic messages and traces."""
    log = producer.generate_mock_log(service_name=service, level=level)
    assert log.service_name == service
    assert log.log_level == level
    assert len(log.message) > 10
    if level == LogLevel.ERROR:
        assert log.stack_trace is not None
        assert "at com.streamingrag." in log.stack_trace
    else:
        assert log.stack_trace is None


def test_generate_mock_log_unknown_service_fallback():
    """Test fallback message and stack trace when an unlisted service is provided."""
    info_log = producer.generate_mock_log(service_name="custom-service", level=LogLevel.INFO)
    assert info_log.message == "INFO log event from custom-service"
    assert info_log.stack_trace is None

    err_log = producer.generate_mock_log(service_name="custom-service", level=LogLevel.ERROR)
    assert err_log.message == "ERROR log event from custom-service"
    assert err_log.stack_trace is not None
    assert "DefaultHandler.execute" in err_log.stack_trace


def test_send_log_with_owned_and_shared_producer(monkeypatch, sample_info_log):
    """Test send_log creates/flushes producer when None, or reuses passed producer."""
    mock_kafka_producer = MagicMock()
    monkeypatch.setattr(producer, "Producer", lambda conf: mock_kafka_producer)

    # 1. Owned producer (producer=None)
    res1 = producer.send_log(sample_info_log)
    assert res1 == sample_info_log
    mock_kafka_producer.produce.assert_called_once()
    mock_kafka_producer.poll.assert_called_once_with(0)
    mock_kafka_producer.flush.assert_called_once()

    # 2. Shared producer
    mock_kafka_producer.reset_mock()
    res2 = producer.send_log(sample_info_log, producer=mock_kafka_producer)
    assert res2 == sample_info_log
    mock_kafka_producer.produce.assert_called_once()
    mock_kafka_producer.poll.assert_called_once_with(0)
    mock_kafka_producer.flush.assert_not_called()


def test_generate_and_send_burst(monkeypatch):
    """Test generate_and_send_burst sends exact batch count and flushes when owning producer."""
    mock_kafka_producer = MagicMock()
    monkeypatch.setattr(producer, "Producer", lambda conf: mock_kafka_producer)

    logs = producer.generate_and_send_burst(5)
    assert len(logs) == 5
    assert all(isinstance(entry, SystemLog) for entry in logs)
    assert mock_kafka_producer.produce.call_count == 5
    mock_kafka_producer.flush.assert_called_once()

    # Non-positive count returns empty list immediately
    mock_kafka_producer.reset_mock()
    empty_logs = producer.generate_and_send_burst(0)
    assert empty_logs == []
    mock_kafka_producer.produce.assert_not_called()


def test_producer_main_burst_cli(monkeypatch, capsys):
    """Test producer main() with --burst CLI flag sends burst and exits cleanly."""
    mock_kafka_producer = MagicMock()
    monkeypatch.setattr(producer, "Producer", lambda conf: mock_kafka_producer)

    producer.main(["--burst", "3"])

    assert mock_kafka_producer.produce.call_count == 3
    mock_kafka_producer.flush.assert_called_once()
    captured = capsys.readouterr().out
    assert "Sending burst of 3 logs..." in captured


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
