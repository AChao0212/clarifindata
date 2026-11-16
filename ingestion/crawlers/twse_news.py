"""Collect current listed-company material disclosures from TWSE OpenAPI.

Each announcement produces a company ID, timestamp, title, and source.
The endpoint has no date parameter, so collect its current feed once per run.

Run: python -m ingestion.crawlers.twse_news
"""
from __future__ import annotations

import asyncio
import logging
from datetime import datetime, date, time, timezone, timedelta
from typing import Any

import httpx
from tenacity import retry, stop_after_attempt, wait_exponential

from ingestion.collector import Collector
from ingestion.publish import publish_crawl

log = logging.getLogger(__name__)

URL = "https://openapi.twse.com.tw/v1/opendata/t187ap04_L"
USER_AGENT = "Mozilla/5.0 (clarifindata data crawler)"
TPE = timezone(timedelta(hours=8))


def _roc_yyyymmdd(s: str) -> date | None:
    if not s or len(s) < 6:
        return None
    try:
        y = int(s[:-4]) + 1911
        m = int(s[-4:-2])
        d = int(s[-2:])
        return date(y, m, d)
    except ValueError:
        return None


def _hhmmss(s: str) -> time:
    s = (s or "").rjust(6, "0")
    try:
        return time(int(s[:2]), int(s[2:4]), int(s[4:6]))
    except ValueError:
        return time(0, 0, 0)


@retry(stop=stop_after_attempt(3), wait=wait_exponential(min=1, max=10))
async def fetch() -> list[dict[str, Any]]:
    async with httpx.AsyncClient(timeout=30, headers={"User-Agent": USER_AGENT}) as cx:
        r = await cx.get(URL)
        r.raise_for_status()
        return r.json()


def parse_rows(raw: list[dict[str, Any]]) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for r in raw:
        sid = (r.get("公司代號") or "").strip() or None
        title = (r.get("主旨 ") or r.get("主旨") or "").strip()
        if not title:
            continue
        d = _roc_yyyymmdd((r.get("發言日期") or "").strip())
        t = _hhmmss((r.get("發言時間") or "").strip())
        if d is None:
            continue
        rows.append({
            "stock_id":     sid,
            "published_at": datetime.combine(d, t, tzinfo=TPE).isoformat(),
            "title":        title[:500],  # cap to avoid pathological rows
            "source":       "TWSE 重大訊息",
        })
    return rows


async def crawl_and_publish() -> int:
    raw = await fetch()
    rows = parse_rows(raw)
    if not rows:
        return 0
    await publish_crawl(
        dataset="TaiwanRelatedNews",
        raw={"source": "t187ap04_L", "row_count": len(raw)},
        rows=rows,
        key_parts=["L", date.today().isoformat()], source_url=URL,
    )
    return len(rows)


def _fetch(_day: date | None) -> int:
    # Snapshot source: no date param, ingest the current feed once per run.
    return asyncio.run(crawl_and_publish())


# Announcements are not limited to trading days. Collect one snapshot and
# allow an empty feed; source errors propagate.
COLLECTOR = Collector(
    "twse_news", _fetch, market="none", per_date=False, min_rows=0,
    label="TaiwanRelatedNews",
)

if __name__ == "__main__":
    raise SystemExit(COLLECTOR.run())
