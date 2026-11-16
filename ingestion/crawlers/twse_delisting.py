"""Collect the full delisted-company snapshot from TWSE OpenAPI.

The endpoint supplies company codes, names, and delisting dates without a
date parameter. Convert ROC dates and retain names; no reason is supplied.

Run: python -m ingestion.crawlers.twse_delisting
"""
from __future__ import annotations

import asyncio
import logging
from datetime import date
from typing import Any, Optional

import httpx
from tenacity import retry, stop_after_attempt, wait_exponential

from ingestion.collector import Collector
from ingestion.publish import publish_crawl

log = logging.getLogger(__name__)
URL = "https://openapi.twse.com.tw/v1/company/suspendListingCsvAndHtml"
UA = "Mozilla/5.0 (clarifindata data crawler)"


def _roc_iso(s: str | None) -> str | None:
    if not s: return None
    s = s.strip().replace(".", "/")
    if "/" not in s: return None
    try:
        y, m, d = s.split("/")
        yi = int(y)
        if yi < 1911: yi += 1911
        return date(yi, int(m), int(d)).isoformat()
    except (ValueError, IndexError):
        return None


@retry(stop=stop_after_attempt(3), wait=wait_exponential(min=1, max=10))
async def _get() -> list[dict[str, Any]]:
    async with httpx.AsyncClient(timeout=30, headers={"User-Agent": UA}) as cx:
        r = await cx.get(URL)
        r.raise_for_status()
        return r.json()


async def crawl_and_publish() -> int:
    raw = await _get()
    rows: list[dict[str, Any]] = []
    for r in raw:
        code = (r.get("Code") or "").strip()
        d = _roc_iso(r.get("DelistingDate"))
        if not code or not d: continue
        rows.append({
            "stock_id":       code,
            "delisting_date": d,
            # The endpoint supplies a company name but no delisting reason.
            "stock_name":     (r.get("Company") or "").strip() or None,
            "reason":         None,
        })
    if not rows: return 0
    await publish_crawl(
        dataset="TaiwanStockDelisting",
        raw={"src": URL, "n": len(raw)},
        rows=rows, key_parts=["all", date.today().isoformat()],
        source_url=URL,
    )
    return len(rows)


def _fetch(day: Optional[date]) -> int:
    # The endpoint has no date parameter; collect one full snapshot per run.
    return asyncio.run(crawl_and_publish())


# Run once without a trading-calendar gate; source errors propagate.
COLLECTOR = Collector(
    "twse_delisting", _fetch, market="none", per_date=False,
    min_rows=0, label="TaiwanStockDelisting",
)

if __name__ == "__main__":
    raise SystemExit(COLLECTOR.run())
