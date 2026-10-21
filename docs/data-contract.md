# Data Contract: SystemLog

The `SystemLog` model in `src/model/models.py` is the shared contract between the Kafka producer and consumer. Both services import the same Pydantic class so the schema cannot drift between write and read paths.

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
| `stack_trace` | `string \| null` | No | Stack trace text; the mock producer sets realistic multi-line traces on `ERROR` events |

## LogLevel enum

`LogLevel` is a `StrEnum` so values serialize as plain strings in JSON (`"ERROR"`, not `2`). This keeps Kafka payloads readable and avoids coupling consumers to Python-specific enum encoding.

```python
class LogLevel(StrEnum):
    INFO = "INFO"
    WARN = "WARN"
    ERROR = "ERROR"
```

## Example payload

```json
{
  "event_id": "22222222-2222-2222-2222-222222222222",
  "timestamp": "2026-07-11T13:05:12.345678+00:00",
  "service_name": "payment-gateway",
  "log_level": "ERROR",
  "message": "Stripe API unreachable after 3 exponential backoff retries (HTTP 502)",
  "stack_trace": "java.net.SocketTimeoutException: Read timed out connecting to https://api.stripe.com/v1/charges\n    at com.streamingrag.payment.StripeClient.charge(StripeClient.java:112)"
}
```

Non-error events omit `stack_trace` or set it to `null`:

```json
{
  "event_id": "11111111-1111-1111-1111-111111111111",
  "timestamp": "2026-07-11T13:05:14.112233+00:00",
  "service_name": "user-auth",
  "log_level": "INFO",
  "message": "User authentication successful for usr_482910 via oauth2-github",
  "stack_trace": null
}
```

## Serialization

| Direction | Method |
|-----------|--------|
| Producer → Kafka | `log_entry.model_dump_json().encode("utf-8")` |
| Kafka → Consumer | `SystemLog.from_kafka_payload(msg.value())` |

`from_kafka_payload` is a convenience wrapper around `model_validate_json` that handles UTF-8 decoding.

## Validation & Dead Letter Queue (DLQ) on the consumer

When a message fails Pydantic validation (or UTF-8 decoding), the consumer:

1. Logs the validation error with partition and offset.
2. **Routes the raw message to the Dead Letter Queue (`system-logs-dlq`)** via `publish_to_dlq()` with diagnostic Kafka headers (`dlq_reason`, `original_topic`, `original_partition`, `original_offset`).
3. **Commits the offset** to skip the poison-pill message without blocking the consumer group.
4. Continues processing the next message.

Similarly, if vector embedding or ChromaDB upsert fails persistently after exponential backoff retries (`max_retries=2`), the message is routed to `system-logs-dlq` for forensic inspection and replay.

## Vector Store Metadata & API Citation Contract (`LogSource`)

During ingestion, `chunk_log()` produces metadata stored in ChromaDB alongside each vector:
- `event_id`: `str(log.event_id)` (or fallback `kafka-{partition}-{offset}`)
- `service_name`: originating microservice name
- `log_level`: `INFO`, `WARN`, or `ERROR`
- `timestamp`: ISO-8601 UTC timestamp string
- `partition` and `offset`: Kafka coordinates

When `POST /api/v1/query` returns cited sources in `QueryResponse.sources`, each `LogSource` includes:
- `event_id: str | None` — unique identifier of the cited log event
- `service_name: str`
- `log_level: str`
- `timestamp: str`
- `score: float | None` — normalized vector similarity confidence score (`0.0` to `1.0`)
- `excerpt: str` — relevant portion of the log document
