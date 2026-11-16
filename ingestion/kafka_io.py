"""Kafka producer/consumer factories (aiokafka)."""
from __future__ import annotations

import json
from typing import Any

from aiokafka import AIOKafkaConsumer, AIOKafkaProducer

from api.config import settings


async def make_producer() -> AIOKafkaProducer:
    p = AIOKafkaProducer(
        bootstrap_servers=settings().kafka_bootstrap_servers,
        value_serializer=lambda v: json.dumps(v, default=str, ensure_ascii=False).encode(),
        compression_type="gzip",
        # Raw payloads are archived separately; messages contain rows and an object key.
        max_request_size=5_000_000,
    )
    await p.start()
    return p


async def make_consumer(
    topic: str, group_id: str, *, enable_auto_commit: bool = True
) -> AIOKafkaConsumer:
    # Consumers writing to Postgres can disable automatic commits and commit
    # offsets only after their database transaction succeeds.
    c = AIOKafkaConsumer(
        topic,
        bootstrap_servers=settings().kafka_bootstrap_servers,
        group_id=group_id,
        value_deserializer=lambda v: json.loads(v.decode()),
        auto_offset_reset="earliest",
        enable_auto_commit=enable_auto_commit,
    )
    await c.start()
    return c


async def publish(producer: AIOKafkaProducer, message: dict[str, Any]) -> None:
    await producer.send_and_wait(settings().kafka_topic_raw, message)
