import time
import random
from uuid import uuid4

from confluent_kafka import Producer
from faker import Faker

from config import KAFKA_BOOTSTRAP_SERVERS, KAFKA_TOPIC_SYSTEM_LOGS
from model import LogLevel, SystemLog

fake = Faker()


def delivery_report(err, msg):
    """Callback to confirm message delivery."""
    if err is not None:
        print(f"Delivery error: {err}")
    else:
        print(f"Log sent to {msg.topic()} [{msg.partition()}]")


def generate_mock_log() -> SystemLog:
    services = ["payment-gateway", "user-auth", "inventory-service", "frontend-bff"]
    levels = [LogLevel.INFO, LogLevel.INFO, LogLevel.WARN, LogLevel.ERROR]

    level = random.choice(levels)
    service = random.choice(services)

    if level == LogLevel.ERROR:
        message = f"Connection timeout to database for {service}"
        stack_trace = fake.text(max_nb_chars=200)
    else:
        message = f"Standard operation executed in {random.randint(10, 500)}ms"
        stack_trace = None

    return SystemLog(
        event_id=uuid4(),
        service_name=service,
        log_level=level,
        message=message,
        stack_trace=stack_trace,
    )


def main():
    conf = {"bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS}
    producer = Producer(conf)

    print("Starting log stream... (Press Ctrl+C to stop)")
    try:
        while True:
            log_entry = generate_mock_log()
            producer.produce(
                KAFKA_TOPIC_SYSTEM_LOGS,
                key=log_entry.service_name.encode("utf-8"),
                value=log_entry.model_dump_json().encode("utf-8"),
                callback=delivery_report,
            )
            producer.poll(0)
            time.sleep(random.uniform(0.5, 2.0))

    except KeyboardInterrupt:
        print("\nStopping producer...")
    finally:
        producer.flush()


if __name__ == "__main__":
    main()
