"""Unit tests for SystemLog data model and contract validation."""

from __future__ import annotations

import json
import uuid
from datetime import datetime

import pytest
from pydantic import ValidationError

from model.models import LogLevel, SystemLog


def test_system_log_valid_instantiation(sample_info_log):
    """Test valid model instantiation and property access."""
    assert sample_info_log.service_name == "payment-service"
    assert sample_info_log.log_level == LogLevel.INFO
    assert sample_info_log.log_level.value == "INFO"
    assert sample_info_log.stack_trace is None
    assert str(sample_info_log.event_id) == "11111111-1111-1111-1111-111111111111"


def test_system_log_defaults():
    """Test default values for event_id, stack_trace, and generated timestamp."""
    log = SystemLog(
        service_name="auth-service",
        log_level=LogLevel.WARN,
        message="Slow query detected",
    )
    assert log.event_id is None
    assert log.stack_trace is None
    assert log.timestamp is not None
    # Validate timestamp format
    parsed_dt = datetime.fromisoformat(log.timestamp)
    assert parsed_dt is not None


def test_from_kafka_payload_success(sample_raw_kafka_payload):
    """Test deserialization from raw Kafka payload bytes."""
    log = SystemLog.from_kafka_payload(sample_raw_kafka_payload)
    assert log.service_name == "user-auth"
    assert log.log_level == LogLevel.WARN
    assert log.message == "Token refresh threshold exceeded"
    assert log.event_id == uuid.UUID("33333333-3333-3333-3333-333333333333")


def test_from_kafka_payload_invalid_json():
    """Test handling of invalid JSON bytes."""
    invalid_bytes = b"not a json payload"
    with pytest.raises(ValidationError):
        SystemLog.from_kafka_payload(invalid_bytes)


@pytest.mark.parametrize(
    "invalid_data,expected_error_field",
    [
        (
            {"log_level": "INFO", "message": "msg"},
            "service_name",
        ),  # missing service_name
        (
            {"service_name": "", "log_level": "INFO", "message": "msg"},
            "service_name",
        ),  # empty service_name
        (
            {"service_name": "srv", "log_level": "INVALID_LEVEL", "message": "msg"},
            "log_level",
        ),  # invalid enum
        (
            {"service_name": "srv", "log_level": "INFO", "message": ""},
            "message",
        ),  # empty message
    ],
)
def test_system_log_validation_errors(invalid_data, expected_error_field):
    """Test schema constraints and validation errors on missing/malformed attributes."""
    payload = json.dumps(invalid_data).encode("utf-8")
    with pytest.raises(ValidationError) as exc_info:
        SystemLog.from_kafka_payload(payload)

    errors = exc_info.value.errors()
    assert any(err["loc"][0] == expected_error_field for err in errors)


def test_to_embedding_text_without_stack_trace(sample_info_log):
    """Test embedding text representation when stack_trace is omitted."""
    text = sample_info_log.to_embedding_text()
    expected = (
        f"[{sample_info_log.log_level.value}] {sample_info_log.service_name} @ {sample_info_log.timestamp}\n"
        f"{sample_info_log.message}"
    )
    assert text == expected
    assert "stack_trace" not in text


def test_to_embedding_text_with_stack_trace(sample_error_log):
    """Test embedding text representation when stack_trace is populated."""
    text = sample_error_log.to_embedding_text()
    assert f"[{sample_error_log.log_level.value}] {sample_error_log.service_name}" in text
    assert sample_error_log.message in text
    assert f"stack_trace: {sample_error_log.stack_trace}" in text
