from __future__ import annotations

import logging
import os
import signal
from collections.abc import Callable

from confluent_kafka import Consumer, KafkaError, KafkaException, Producer
from confluent_kafka.admin import AdminClient, NewTopic

from poc.events import EventEnvelope, TOPICS


logger = logging.getLogger(__name__)
BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")


def ensure_topics() -> None:
    admin = AdminClient({"bootstrap.servers": BOOTSTRAP_SERVERS})
    metadata = admin.list_topics(timeout=10)
    missing = [name for name in TOPICS if name not in metadata.topics]
    if not missing:
        return
    futures = admin.create_topics(
        [NewTopic(name, num_partitions=1, replication_factor=1) for name in missing]
    )
    for name, future in futures.items():
        try:
            future.result()
            logger.info("Created Kafka topic %s", name)
        except KafkaException as exc:
            if "TOPIC_ALREADY_EXISTS" not in str(exc):
                raise


def publish(event: EventEnvelope, producer: Producer | None = None) -> None:
    target = producer or Producer({"bootstrap.servers": BOOTSTRAP_SERVERS})
    errors: list[str] = []

    def delivered(error: KafkaError | None, _message: object) -> None:
        if error:
            errors.append(str(error))

    target.produce(
        event.topic,
        key=event.correlation_id.encode(),
        value=event.to_json().encode(),
        on_delivery=delivered,
    )
    target.flush(10)
    if errors:
        raise RuntimeError(f"Kafka publish failed: {errors[0]}")


def run_consumer(
    *,
    name: str,
    topics: list[str],
    handler: Callable[[EventEnvelope], None],
) -> None:
    consumer = Consumer(
        {
            "bootstrap.servers": BOOTSTRAP_SERVERS,
            "group.id": name,
            "auto.offset.reset": "earliest",
            "enable.auto.commit": False,
        }
    )
    stopping = False

    def stop(_signum: int, _frame: object) -> None:
        nonlocal stopping
        stopping = True

    signal.signal(signal.SIGTERM, stop)
    signal.signal(signal.SIGINT, stop)
    consumer.subscribe(topics)
    logger.info("%s consuming %s", name, topics)
    try:
        while not stopping:
            message = consumer.poll(1.0)
            if message is None:
                continue
            if message.error():
                if message.error().code() != KafkaError._PARTITION_EOF:
                    raise KafkaException(message.error())
                continue
            event = EventEnvelope.from_json(message.value())
            handler(event)
            consumer.commit(message=message, asynchronous=False)
    finally:
        consumer.close()
