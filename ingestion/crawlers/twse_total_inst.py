"""Collect market-wide institutional net flows from TWSE's BFI82U endpoint.

Rows contain trade_date, foreign_net, trust_net, dealer_net, and total_net.
Dealer flows combine proprietary and hedging activity. The reported aggregate
excludes foreign dealers. Date and category guards reject incomplete data.

Run: python -m ingestion.crawlers.twse_total_inst --date 2026-05-22
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
from ingestion.completeness_guard import CompletenessError
from ingestion.date_guard import response_date_matches
from ingestion.publish import publish_crawl

log = logging.getLogger(__name__)

BASE_URL = "https://www.twse.com.tw/rwd/zh/fund/BFI82U"
USER_AGENT = "Mozilla/5.0 (clarifindata data crawler; +https://clarifindata.com; support@clarifindata.com)"


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


@retry(stop=stop_after_attempt(3), wait=wait_exponential(min=1, max=10))
async def fetch_date(target: date) -> dict[str, Any]:
    params = {"dayDate": target.strftime("%Y%m%d"), "type": "day", "response": "json"}
    async with httpx.AsyncClient(timeout=15.0, headers={"User-Agent": USER_AGENT}) as cx:
        r = await cx.get(BASE_URL, params=params)
        r.raise_for_status()
        return r.json()


def parse_rows(raw: dict[str, Any], target: date) -> list[dict[str, Any]]:
    if raw.get("stat") != "OK":
        return []
    # The title carries the trade date; the top-level date is the report date.
    if not response_date_matches(raw, target, prefer_title=True):
        log.warning("BFI82U date mismatch for %s; skipping response", target)
        return []
    dealer_net = 0
    trust_net = 0
    foreign_net = 0
    saw_any = False
    saw_dealer = False
    saw_trust = False
    saw_foreign = False
    for row in raw.get("data") or []:
        if len(row) < 4:
            continue
        name = (row[0] or "").strip()
        net = _int(row[3])
        if net is None:
            continue
        saw_any = True
        # Classify foreign dealers before foreign investors or domestic dealers.
        # TWSE excludes foreign dealers from its institutional aggregate.
        if name.startswith("外資自營商"):
            continue  # Excluded from the official institutional aggregate.
        elif name.startswith("自營商"):
            dealer_net += net
            saw_dealer = True
        elif name.startswith("投信"):
            trust_net = net
            saw_trust = True
        elif name.startswith("外資"):  # 外資及陸資(不含外資自營商)
            foreign_net = net
            saw_foreign = True
        elif name.startswith("合計"):
            continue  # total row — skip, we compute it ourselves
    if not saw_any:
        # No category rows indicates a holiday or unavailable data.
        return []
    # A matching response with data must include all three institutional groups.
    missing = [
        n for n, seen in (
            ("外資", saw_foreign), ("投信", saw_trust), ("自營商", saw_dealer),
        ) if not seen
    ]
    if missing:
        raise CompletenessError(
            f"BFI82U {target}: source served data but these 三大法人 legs are "
            f"MISSING: {', '.join(missing)}; refusing to store a partial total."
        )
    return [{
        "trade_date": target.isoformat(),
        "foreign_net": foreign_net,
        "trust_net": trust_net,
        "dealer_net": dealer_net,
        "total_net": foreign_net + trust_net + dealer_net,
    }]


async def crawl_and_publish(target: date) -> int:
    raw = await fetch_date(target)
    rows = parse_rows(raw, target)
    if not rows:
        log.warning("no BFI82U rows for %s (stat=%s)", target, raw.get("stat"))
        return 0
    src = f"{BASE_URL}?dayDate={target.strftime('%Y%m%d')}&type=day&response=json"
    await publish_crawl(
        dataset="TaiwanStockTotalInstitutionalInvestors", raw=raw, rows=rows,
        key_parts=["MARKET", target.isoformat()], source_url=src,
        trade_date=target.isoformat(),
    )
    return len(rows)


def _fetch(day: date) -> int:
    return asyncio.run(crawl_and_publish(day))


# Category completeness is enforced during parsing. Empty holiday or
# date-mismatched responses may return zero rows.
COLLECTOR = Collector(
    "twse_total_inst", _fetch, market="tw", min_rows=0,
    label="TaiwanStockTotalInstitutionalInvestors",
)

if __name__ == "__main__":
    raise SystemExit(COLLECTOR.run())
