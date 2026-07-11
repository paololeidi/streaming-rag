from datetime import datetime, timezone
from enum import Enum
from uuid import UUID

from pydantic import BaseModel, Field


class LogLevel(str, Enum):
    INFO = "INFO"
    WARN = "WARN"
    ERROR = "ERROR"


class SystemLog(BaseModel):
    """Shared data contract between the Kafka producer and consumer."""

    event_id: UUID | None = Field(
        default=None,
        description="Stable identifier for idempotent vector DB writes.",
    )
    timestamp: str = Field(
        default_factory=lambda: datetime.now(timezone.utc).isoformat(),
        description="UTC ISO-8601 timestamp generated at produce time.",
    )
    service_name: str = Field(
        min_length=1,
        description="Originating microservice identifier; also used as the Kafka partition key.",
    )
    log_level: LogLevel
    message: str = Field(min_length=1)
    stack_trace: str | None = Field(
        default=None,
        description="Optional stack trace; populated for ERROR-level events in the mock producer.",
    )

    def to_embedding_text(self) -> str:
        lines = [
            f"[{self.log_level.value}] {self.service_name} @ {self.timestamp}",
            self.message,
        ]
        if self.stack_trace:
            lines.append(f"stack_trace: {self.stack_trace}")
        return "\n".join(lines)

    @classmethod
    def from_kafka_payload(cls, payload: bytes) -> "SystemLog":
        return cls.model_validate_json(payload.decode("utf-8"))
