"""Collect daily valuation ratios from TWSE's BWIBBU_d endpoint.

Rows contain stock_id, trade_date, per, pbr, and dividend_yield. Column names
support historical layouts; date and row-count guards reject incomplete data.

Run: python -m ingestion.crawlers.twse_per --date 2026-05-22
"""
from __future__ import annotations

import asyncio
import logging
from datetime import date
from decimal import Decimal, InvalidOperation
from typing import Any

import httpx
from tenacity import retry, stop_after_attempt, wait_exponential

from ingestion.completeness_guard import assert_captured_floor, served_real_data
from ingestion.date_guard import response_date_matches
from ingestion.collector import Collector
from ingestion.publish import publish_crawl

log = logging.getLogger(__name__)

BASE_URL = "https://www.twse.com.tw/rwd/zh/afterTrading/BWIBBU_d"
USER_AGENT = "Mozilla/5.0 (clarifindata data crawler; +https://clarifindata.com; support@clarifindata.com)"
# Minimum row counts distinguish incomplete captures from valid daily data.
FLOOR_PRE_2012 = 500
FLOOR = 700


def _decimal(s: Any) -> Decimal | None:
    if s is None:
        return None
    s = str(s).replace(",", "").replace("+", "").strip()
    if s in {"", "--", "X", "N/A"}:
        return None
    try:
        return Decimal(s)
    except (InvalidOperation, ValueError):
        return None


@retry(stop=stop_after_attempt(3), wait=wait_exponential(min=1, max=10))
async def fetch_date(target: date) -> dict[str, Any]:
    params = {
        "date": target.strftime("%Y%m%d"),
        "selectType": "ALL",
        "response": "json",
    }
    async with httpx.AsyncClient(timeout=20.0, headers={"User-Agent": USER_AGENT}) as cx:
        r = await cx.get(BASE_URL, params=params)
        r.raise_for_status()
        return r.json()


def parse_rows(raw: dict[str, Any], target: date) -> list[dict[str, Any]]:
    if raw.get("stat") != "OK":
        return []
    # Publish only data for the requested date.
    if not response_date_matches(raw, target):
        log.warning("PER date mismatch for %s; skipping response", target)
        return []
    iso = target.isoformat()
    # Resolve column names across historical and current response layouts.
    fields = [str(f).strip() for f in (raw.get("fields") or [])]

    def at(prefix: str) -> int | None:
        return next((i for i, f in enumerate(fields) if f.startswith(prefix)), None)

    c_id, c_yield, c_per, c_pbr = at("證券代號"), at("殖利率"), at("本益比"), at("股價淨值比")
    if None in (c_id, c_yield, c_per, c_pbr):
        return []          # The capture guard handles missing required columns.
    width = max(c_id, c_yield, c_per, c_pbr)
    rows: list[dict[str, Any]] = []
    for row in raw.get("data") or []:
        if len(row) <= width:
            continue
        stock_id = (row[c_id] or "").strip()
        if not stock_id:
            continue
        rows.append(
            {
                "stock_id": stock_id,
                "trade_date": iso,
                "dividend_yield": _decimal(row[c_yield]),
                "per": _decimal(row[c_per]),
                "pbr": _decimal(row[c_pbr]),
            }
        )
    return rows


async def crawl_and_publish(target: date) -> int:
    raw = await fetch_date(target)
    rows = parse_rows(raw, target)
    if not rows:
        # Empty responses are valid unless the source supplied matching data.
        if served_real_data(raw) and response_date_matches(raw, target):
            assert_captured_floor(
                dataset="TaiwanStockPER", captured=0, floor=FLOOR, target=target,
                detail="stat=OK + date matched but ZERO rows parsed",
            )
        log.warning("no PER rows for %s (stat=%s)", target, raw.get("stat"))
        return 0
    # Earlier years use a lower floor for the smaller listed-company universe.
    assert_captured_floor(
        dataset="TaiwanStockPER", captured=len(rows),
        floor=FLOOR if target.year >= 2012 else FLOOR_PRE_2012, target=target,
    )
    src = f"{BASE_URL}?date={target.strftime('%Y%m%d')}&selectType=ALL&response=json"
    await publish_crawl(
        dataset="TaiwanStockPER", raw=raw, rows=rows,
        key_parts=["ALL", target.isoformat()],
        source_url=src, trade_date=target.isoformat(),
    )
    return len(rows)


def _fetch(day: date) -> int:
    return asyncio.run(crawl_and_publish(day))


# A valid trading day must produce at least one row; the capture guard also
# enforces the date-dependent minimum above.
COLLECTOR = Collector("twse_per", _fetch, market="tw", min_rows=1, label="TaiwanStockPER")

if __name__ == "__main__":
    raise SystemExit(COLLECTOR.run())
