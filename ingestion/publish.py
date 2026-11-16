"""Archive raw crawl responses and publish parsed rows with their archive reference.

Rows are split into batches of at most 800 to limit Kafka message size.
"""
from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

from ingestion.kafka_io import make_producer, publish
from ingestion.minio_io import put_raw

CHUNK = 800


async def publish_crawl(
    *,
    dataset: str,
    raw: Any,
    rows: list[dict[str, Any]],
    key_parts: list[str],
    source_url: str,
    **meta: Any,
) -> str:
    """Persist `raw` to MinIO, publish `{dataset, rows, raw_object_key, ...}`
    to Kafka. Long row lists are split into ≤CHUNK-sized messages so we
    stay under Kafka's max_request_size. Returns the MinIO object key.
    """
    raw_object_key = put_raw(dataset, key_parts, raw)
    fetched_at = datetime.now(timezone.utc).isoformat()
    producer = await make_producer()
    try:
        # Empty row list → still send one message (consumer can record the
        # crawl attempt). Non-empty → chunk.
        chunks = [rows[i:i + CHUNK] for i in range(0, len(rows), CHUNK)] or [[]]
        for i, chunk in enumerate(chunks):
            await publish(
                producer,
                {
                    "dataset": dataset,
                    "rows": chunk,
                    "raw_object_key": raw_object_key,
                    "source_url": source_url,
                    "fetched_at": fetched_at,
                    "chunk_index": i,
                    "chunk_count": len(chunks),
                    **meta,
                },
            )
    finally:
        await producer.stop()
    return raw_object_key
