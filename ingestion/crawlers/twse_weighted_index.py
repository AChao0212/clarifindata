"""Collect monthly TAIEX prices and volume from TWSE.

MI_5MINS_HIST supplies daily open, high, low, and close values. FMTQIK supplies
market volume. Each index date also produces a TaiwanStockTradingDate row.
The requested trading day selects the calendar month to collect.

Run: python -m ingestion.crawlers.twse_weighted_index --date 2026-04-15
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
from ingestion.publish import publish_crawl

log = logging.getLogger(__name__)

BASE_URL  = "https://www.twse.com.tw/rwd/zh/TAIEX/MI_5MINS_HIST"
FMTQIK_URL = "https://www.twse.com.tw/rwd/zh/afterTrading/FMTQIK"
USER_AGENT = "Mozilla/5.0 (clarifindata data crawler; +https://github.com/clarifindata)"


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


def _int(s: Any) -> int | None:
    d = _decimal(s)
    return int(d) if d is not None else None


def _roc_to_iso(s: str) -> str:
    y, m, d = s.split("/")
    return date(int(y) + 1911, int(m), int(d)).isoformat()


@retry(stop=stop_after_attempt(3), wait=wait_exponential(min=1, max=10))
async def fetch_month(year: int, month: int) -> dict[str, Any]:
    params = {"date": f"{year:04d}{month:02d}01", "response": "json"}
    async with httpx.AsyncClient(timeout=15.0, headers={"User-Agent": USER_AGENT}) as cx:
        r = await cx.get(BASE_URL, params=params)
        r.raise_for_status()
        return r.json()


@retry(stop=stop_after_attempt(3), wait=wait_exponential(min=1, max=10))
async def fetch_fmtqik(year: int, month: int) -> dict[str, int]:
    """Fetch FMTQIK volume data: returns {iso_date: volume_shares}."""
    params = {"date": f"{year:04d}{month:02d}01", "response": "json"}
    try:
        async with httpx.AsyncClient(timeout=15.0, headers={"User-Agent": USER_AGENT}) as cx:
            r = await cx.get(FMTQIK_URL, params=params)
            r.raise_for_status()
            raw = r.json()
        if raw.get("stat") != "OK":
            return {}
        out: dict[str, int] = {}
        for row in raw.get("data") or []:
            if len(row) < 2:
                continue
            try:
                iso = _roc_to_iso(str(row[0]))
                vol = int(str(row[1]).replace(",", "").strip())
                out[iso] = vol
            except (ValueError, IndexError):
                continue
        return out
    except Exception as e:
        log.warning("FMTQIK %d-%02d fetch failed: %s", year, month, e)
        return {}


def parse_rows(raw: dict[str, Any], volume_map: dict[str, int] | None = None) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    """Returns (weighted_index_rows, trading_date_rows).

    volume_map: optional {iso_date: volume} from FMTQIK to merge in.
    """
    if raw.get("stat") != "OK":
        return [], []
    if volume_map is None:
        volume_map = {}
    idx_rows: list[dict[str, Any]] = []
    cal_rows: list[dict[str, Any]] = []
    for row in raw.get("data") or []:
        if len(row) < 5:
            continue
        iso = _roc_to_iso(row[0])
        idx_rows.append(
            {
                "trade_date": iso,
                "volume": volume_map.get(iso),   # from FMTQIK; None if missing
                "open":  _decimal(row[1]),
                "high":  _decimal(row[2]),
                "low":   _decimal(row[3]),
                "close": _decimal(row[4]),
            }
        )
        cal_rows.append({"trade_date": iso, "is_trading_day": True})
    return idx_rows, cal_rows


async def crawl_and_publish(year: int, month: int) -> int:
    raw = await fetch_month(year, month)
    # Volume is optional when FMTQIK is unavailable.
    await asyncio.sleep(0.3)
    volume_map = await fetch_fmtqik(year, month)
    idx_rows, cal_rows = parse_rows(raw, volume_map)
    if not idx_rows:
        log.warning("no rows for TAIEX %d-%02d (stat=%s)", year, month, raw.get("stat"))
        return 0
    src = f"{BASE_URL}?date={year:04d}{month:02d}01&response=json"

    await publish_crawl(
        dataset="TaiwanWeightedIndex", raw=raw, rows=idx_rows,
        key_parts=["TAIEX", f"{year:04d}{month:02d}"],
        source_url=src, year=year, month=month,
    )
    await publish_crawl(
        dataset="TaiwanStockTradingDate", raw={"derived_from": "TAIEX_5"}, rows=cal_rows,
        key_parts=["CAL", f"{year:04d}{month:02d}"],
        source_url=src, year=year, month=month,
    )
    vol_filled = sum(1 for r in idx_rows if r.get("volume") is not None)
    log.info("TAIEX %d-%02d: %d rows, %d with volume", year, month, len(idx_rows), vol_filled)
    return len(idx_rows)


def _fetch(day: date) -> int:
    # Each daily request republishes its calendar month using the same keys.
    return asyncio.run(crawl_and_publish(day.year, day.month))


# The requested trading day's month must contain at least one index row.
COLLECTOR = Collector(
    "twse_weighted_index", _fetch, market="tw", min_rows=1,
    label="TaiwanWeightedIndex (+ TaiwanStockTradingDate)",
)

if __name__ == "__main__":
    raise SystemExit(COLLECTOR.run())
