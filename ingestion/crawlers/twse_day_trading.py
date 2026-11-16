"""Collect per-security day-trading activity from TWSE's TWTB4U endpoint.

Rows contain the date, security ID, buy and sell volumes, and buy and sell
amounts. The source volume is one-sided, so both volume fields use that value.
Date and row-count guards reject incomplete captures.

Run: python -m ingestion.crawlers.twse_day_trading --date 2026-05-20
"""
from __future__ import annotations

import asyncio
import logging
from datetime import date
from decimal import Decimal, InvalidOperation
from typing import Any

import httpx
from tenacity import retry, stop_after_attempt, wait_exponential

from ingestion.collector import Collector
from ingestion.completeness_guard import assert_captured_floor, served_real_data
from ingestion.date_guard import response_date_matches
from ingestion.publish import publish_crawl

log = logging.getLogger(__name__)
URL = "https://www.twse.com.tw/rwd/zh/dayTrading/TWTB4U"
UA = "Mozilla/5.0 (clarifindata data crawler)"
# Minimum row count for the current eligible-security universe.
FLOOR = 800


def _floor_for(target) -> int:
    """Use smaller capture floors for the earlier eligible-security universe."""
    return 100 if target.year <= 2015 else 250 if target.year == 2016 else FLOOR


def _dec(s: Any) -> str | None:
    if s is None: return None
    s = str(s).replace(",", "").strip()
    if s in {"", "--", "X", "N/A", "-"}: return None
    try: return str(Decimal(s))
    except (InvalidOperation, ValueError): return None


def _int(s: Any) -> int | None:
    d = _dec(s)
    return int(Decimal(d)) if d is not None else None


@retry(stop=stop_after_attempt(3), wait=wait_exponential(min=1, max=10))
async def _get(cx: httpx.AsyncClient, d: date) -> dict[str, Any]:
    r = await cx.get(URL, params={
        "date": d.strftime("%Y%m%d"),
        "selectType": "All",
        "response": "json",
    })
    r.raise_for_status()
    return r.json()


def parse(payload: dict[str, Any], target: date) -> list[dict[str, Any]]:
    if payload.get("stat") != "OK": return []
    if not response_date_matches(payload, target): return []
    tables = payload.get("tables") or []
    # Locate the security-level table by its first column name.
    table = None
    for t in tables:
        fields = t.get("fields") or []
        if fields and fields[0].startswith("證券代號"):
            table = t
            break
    if table is None: return []
    iso = target.isoformat()
    out: list[dict[str, Any]] = []
    for row in table.get("data") or []:
        if len(row) < 6: continue
        sid = (row[0] or "").strip()
        if not sid: continue
        # The source reports one-sided volume; buy and sell volumes are equal.
        vol = _int(row[3])
        out.append({
            "stock_id":    sid,
            "trade_date":  iso,
            "buy_volume":  vol,
            "sell_volume": vol,
            "buy_amount":  _dec(row[4]),
            "sell_amount": _dec(row[5]),
        })
    return out


async def crawl_day(d: date) -> int:
    async with httpx.AsyncClient(timeout=20, headers={"User-Agent": UA}) as cx:
        payload = await _get(cx, d)
    rows = parse(payload, d)
    if not rows:
        # An empty response is valid unless the source supplied matching data.
        if served_real_data(payload) and response_date_matches(payload, d):
            assert_captured_floor(
                dataset="TaiwanStockDayTradingInfo", captured=0, floor=_floor_for(d),
                target=d, detail="stat=OK + date matched but ZERO rows parsed",
            )
        return 0
    # Matching source data must meet the date-dependent capture floor.
    assert_captured_floor(
        dataset="TaiwanStockDayTradingInfo", captured=len(rows), floor=_floor_for(d),
        target=d,
    )
    await publish_crawl(
        dataset="TaiwanStockDayTradingInfo",
        raw=payload, rows=rows,
        key_parts=["dt", d.isoformat()],
        source_url=f"{URL}?date={d.strftime('%Y%m%d')}",
    )
    return len(rows)


def _fetch(day: date) -> int:
    return asyncio.run(crawl_day(day))


# The capture guard checks matching source data; empty holidays and
# date-mismatched responses may return zero rows.
COLLECTOR = Collector(
    "twse_day_trading", _fetch, market="tw", min_rows=0,
    label="TaiwanStockDayTradingInfo",
)

if __name__ == "__main__":
    raise SystemExit(COLLECTOR.run())
