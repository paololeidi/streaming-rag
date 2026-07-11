# Data Contract: SystemLog

The `SystemLog` model in `src/models.py` is the shared contract between the Kafka producer and consumer. Both services import the same Pydantic class so the schema cannot drift between write and read paths.

## Why a shared schema?

Phase 1.2 requires strictly typed log schemas. Defining the model once and reusing it gives:

- **Single source of truth** — field names, types, and constraints live in one file.
- **Runtime validation** — the consumer rejects malformed messages before processing.
- **JSON interoperability** — Pydantic serializes to standard JSON that any future service (embedder, API) can consume.

## Fields

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `event_id` | `UUID \| null` | No | Stable identifier for idempotent vector DB writes; set by the producer |
| `timestamp` | `string` | Yes (auto) | UTC ISO-8601 timestamp set at produce time via `datetime.now(timezone.utc).isoformat()` |
| `service_name` | `string` | Yes | Microservice identifier (e.g. `payment-gateway`). Minimum length 1. |
| `log_level` | `LogLevel` enum | Yes | One of `INFO`, `WARN`, `ERROR` |
| `message` | `string` | Yes | Human-readable log line. Minimum length 1. |
| `stack_trace` | `string \| null` | No | Stack trace text; the mock producer sets this on `ERROR` events |

## LogLevel enum

`LogLevel` is a `str` enum so values serialize as plain strings in JSON (`"ERROR"`, not `2`). This keeps Kafka payloads readable and avoids coupling consumers to Python-specific enum encoding.

```python
class LogLevel(str, Enum):
    INFO = "INFO"
    WARN = "WARN"
    ERROR = "ERROR"
```

## Example payload

```json
{
  "timestamp": "2026-07-11T13:05:12.345678+00:00",
  "service_name": "payment-gateway",
  "log_level": "ERROR",
  "message": "Connection timeout to database for payment-gateway",
  "stack_trace": "Traceback (most recent call last):\n  File ..."
}
```

Non-error events omit `stack_trace` or set it to `null`:

```json
{
  "timestamp": "2026-07-11T13:05:14.112233+00:00",
  "service_name": "user-auth",
  "log_level": "INFO",
  "message": "Standard operation executed in 142ms",
  "stack_trace": null
}
```

## Serialization

| Direction | Method |
|-----------|--------|
| Producer → Kafka | `log_entry.model_dump_json().encode("utf-8")` |
| Kafka → Consumer | `SystemLog.from_kafka_payload(msg.value())` |

`from_kafka_payload` is a convenience wrapper around `model_validate_json` that handles UTF-8 decoding.

## Validation on the consumer

When a message fails Pydantic validation, the consumer:

1. Logs the validation error with partition and offset.
2. **Commits the offset** to skip the poison-pill message.
3. Continues processing the next message.

This prevents a single malformed record from blocking the entire consumer group. In production you would typically route invalid messages to a **dead-letter topic (DLT)** instead of only logging them. That pattern will be relevant in later phases.

## Design choices not enforced (yet)

These are intentional simplifications for Phase 1:

- **`stack_trace` is optional even for `ERROR`** — real systems do not always attach a trace; keeping it optional avoids rejecting valid production logs.
- **No `trace_id` / `span_id`** — OpenTelemetry fields can be added when observability becomes a requirement.
- **Timestamp is a string, not `datetime`** — ISO strings serialize cleanly to JSON without custom encoders; typed datetime parsing can be added in Phase 2 if needed.

## Extending the contract

When moving to Phase 2 (vector ingestion), consider adding:

- `event_id: UUID` for idempotent writes to the vector DB
- `host: str` and `environment: str` for filtered retrieval
- A `to_embedding_text()` method that formats fields into a single string for the embedding model

Keep new fields optional with defaults so existing Kafka messages remain valid during rollout.
