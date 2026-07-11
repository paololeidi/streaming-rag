from dataclasses import dataclass

from model import SystemLog


@dataclass
class LogChunk:
    event_id: str
    text: str
    metadata: dict


def chunk_log(log: SystemLog, *, partition: int, offset: int) -> LogChunk:
    event_id = str(log.event_id) if log.event_id else f"kafka-{partition}-{offset}"

    return LogChunk(
        event_id=event_id,
        text=log.to_embedding_text(),
        metadata={
            "service_name": log.service_name,
            "log_level": log.log_level.value,
            "timestamp": log.timestamp,
            "partition": partition,
            "offset": offset,
        },
    )
