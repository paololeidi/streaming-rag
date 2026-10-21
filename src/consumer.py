import asyncio
from typing import Any

from confluent_kafka import Consumer, KafkaError, KafkaException, Producer
from pydantic import ValidationError

from chunking import chunk_log
from config import (
    KAFKA_BOOTSTRAP_SERVERS,
    KAFKA_CONSUMER_GROUP,
    KAFKA_TOPIC_SYSTEM_LOGS,
    KAFKA_TOPIC_SYSTEM_LOGS_DLQ,
)
from embeddings import HuggingFaceEmbedder
from model import SystemLog
from vector_store import upsert_log


def publish_to_dlq(
    dlq_producer: Producer,
    msg: Any,
    error_reason: str,
    dlq_topic: str = KAFKA_TOPIC_SYSTEM_LOGS_DLQ,
) -> None:
    """Publish a failed or corrupted Kafka message to the Dead Letter Queue (DLQ)."""
    orig_topic = (
        msg.topic()
        if hasattr(msg, "topic") and callable(msg.topic) and isinstance(msg.topic(), str)
        else KAFKA_TOPIC_SYSTEM_LOGS
    )
    orig_partition = (
        msg.partition() if hasattr(msg, "partition") and callable(msg.partition) else -1
    )
    orig_offset = msg.offset() if hasattr(msg, "offset") and callable(msg.offset) else -1
    raw_key = (
        msg.key()
        if hasattr(msg, "key") and callable(msg.key) and isinstance(msg.key(), (bytes, str))
        else None
    )
    raw_value = msg.value() if hasattr(msg, "value") and callable(msg.value) else None

    headers = [
        ("dlq_reason", str(error_reason).encode("utf-8")),
        ("original_topic", str(orig_topic).encode("utf-8")),
        ("original_partition", str(orig_partition).encode("utf-8")),
        ("original_offset", str(orig_offset).encode("utf-8")),
    ]

    dlq_producer.produce(
        dlq_topic,
        key=raw_key,
        value=raw_value,
        headers=headers,
    )
    dlq_producer.poll(0)
    print(
        f"Routed message at offset {orig_offset} on partition {orig_partition} "
        f"to DLQ '{dlq_topic}': {error_reason}"
    )


async def process_message(
    msg: Any,
    log_entry: SystemLog,
    consumer: Consumer,
    embedder: HuggingFaceEmbedder,
    dlq_producer: Producer | None = None,
    max_retries: int = 2,
    retry_backoff_sec: float = 1.0,
) -> None:
    chunk = chunk_log(log_entry, partition=msg.partition(), offset=msg.offset())

    for attempt in range(max_retries + 1):
        try:
            embedding = await asyncio.to_thread(embedder.embed, chunk.text)
            await asyncio.to_thread(
                upsert_log,
                chunk.event_id,
                embedding,
                chunk.metadata,
                chunk.text,
            )
            break
        except Exception as exc:
            if attempt < max_retries:
                backoff = retry_backoff_sec * (2**attempt)
                await asyncio.sleep(backoff)
            else:
                if dlq_producer is not None:
                    publish_to_dlq(
                        dlq_producer,
                        msg,
                        f"Processing failed after {max_retries + 1} attempts: {exc}",
                    )
                    consumer.commit(message=msg, asynchronous=False)
                    return
                raise

    consumer.commit(message=msg, asynchronous=False)
    print(
        f"Ingested {chunk.event_id} "
        f"[{log_entry.log_level.value}] {log_entry.service_name}: {log_entry.message}"
    )


async def run_consumer() -> None:
    conf = {
        "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
        "group.id": KAFKA_CONSUMER_GROUP,
        "auto.offset.reset": "earliest",
        "enable.auto.commit": False,
    }

    consumer = Consumer(conf)
    dlq_producer = Producer({"bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS})
    consumer.subscribe([KAFKA_TOPIC_SYSTEM_LOGS])
    embedder = HuggingFaceEmbedder()

    print(f"Listening on topic '{KAFKA_TOPIC_SYSTEM_LOGS}'...")

    try:
        while True:
            msg = consumer.poll(timeout=1.0)
            if msg is None:
                await asyncio.sleep(0)
                continue
            err = msg.error()
            if err:
                if err.code() == KafkaError._PARTITION_EOF:
                    continue
                raise KafkaException(err)

            raw_value = msg.value()
            if raw_value is None:
                continue

            try:
                log_entry = SystemLog.from_kafka_payload(raw_value)
            except (ValidationError, UnicodeDecodeError) as exc:
                print(
                    f"Skipping invalid message at offset {msg.offset()} "
                    f"on partition {msg.partition()}: {exc}"
                )
                publish_to_dlq(dlq_producer, msg, f"ValidationError: {exc}")
                consumer.commit(message=msg, asynchronous=False)
                continue

            try:
                await process_message(msg, log_entry, consumer, embedder)
            except Exception as exc:
                print(
                    f"Failed to process message at offset {msg.offset()} "
                    f"on partition {msg.partition()}: {exc}"
                )
                publish_to_dlq(dlq_producer, msg, f"ProcessingError: {exc}")

    except KeyboardInterrupt:
        print("\nStopping consumer...")
    finally:
        dlq_producer.flush(timeout=1.0)
        consumer.close()


def main() -> None:
    asyncio.run(run_consumer())


if __name__ == "__main__":
    main()
