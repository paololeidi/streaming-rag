import asyncio

from confluent_kafka import Consumer, KafkaError, KafkaException
from pydantic import ValidationError

from chunking import chunk_log
from config import KAFKA_BOOTSTRAP_SERVERS, KAFKA_CONSUMER_GROUP, KAFKA_TOPIC_SYSTEM_LOGS
from embeddings import HuggingFaceEmbedder
from model import SystemLog
from vector_store import upsert_log


async def process_message(
    msg,
    log_entry: SystemLog,
    consumer: Consumer,
    embedder: HuggingFaceEmbedder,
) -> None:
    chunk = chunk_log(log_entry, partition=msg.partition(), offset=msg.offset())

    embedding = await asyncio.to_thread(embedder.embed, chunk.text)
    await asyncio.to_thread(
        upsert_log,
        chunk.event_id,
        embedding,
        chunk.metadata,
        chunk.text,
    )

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
    consumer.subscribe([KAFKA_TOPIC_SYSTEM_LOGS])
    embedder = HuggingFaceEmbedder()

    print(f"Listening on topic '{KAFKA_TOPIC_SYSTEM_LOGS}'...")

    try:
        while True:
            msg = consumer.poll(timeout=1.0)
            if msg is None:
                await asyncio.sleep(0)
                continue
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                raise KafkaException(msg.error())

            try:
                log_entry = SystemLog.from_kafka_payload(msg.value())
            except ValidationError as exc:
                print(
                    f"Skipping invalid message at offset {msg.offset()} "
                    f"on partition {msg.partition()}: {exc}"
                )
                consumer.commit(message=msg, asynchronous=False)
                continue

            try:
                await process_message(msg, log_entry, consumer, embedder)
            except Exception as exc:
                print(
                    f"Failed to process message at offset {msg.offset()} "
                    f"on partition {msg.partition()}: {exc}"
                )

    except KeyboardInterrupt:
        print("\nStopping consumer...")
    finally:
        consumer.close()


def main():
    asyncio.run(run_consumer())


if __name__ == "__main__":
    main()
