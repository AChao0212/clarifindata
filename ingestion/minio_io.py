"""Persist raw crawl payloads in MinIO for later processing."""
from __future__ import annotations

import io
import json
from datetime import datetime, timezone
from typing import Any

from minio import Minio

from api.config import settings

_client: Minio | None = None


def client() -> Minio:
    global _client
    if _client is None:
        s = settings()
        _client = Minio(
            s.minio_endpoint,
            access_key=s.minio_access_key,
            secret_key=s.minio_secret_key,
            secure=s.minio_secure,
        )
    return _client


def put_raw(dataset: str, key_parts: list[str], payload: Any) -> str:
    """Store a raw payload as JSON. Returns the object key."""
    ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    parts = key_parts if key_parts else ["misc"]
    object_key = f"{dataset}/{'/'.join(parts)}/{ts}.json"
    body = json.dumps(payload, ensure_ascii=False, default=str).encode()
    client().put_object(
        settings().minio_bucket_raw,
        object_key,
        io.BytesIO(body),
        length=len(body),
        content_type="application/json",
    )
    return object_key
