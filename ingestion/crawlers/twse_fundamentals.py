"""Collect dividend resolutions and the current TWSE/TPEX security universe.

TWSE OpenAPI supplies current and archived dividend resolutions. TWSE MI_INDEX
and TPEX daily closing quotes supply active equities, funds, and warrants.
Snapshot feeds do not accept a historical date parameter.

Run: python -m ingestion.crawlers.twse_fundamentals --dataset both
"""
from __future__ import annotations

import argparse
import asyncio
import logging
import sys
from datetime import date
from decimal import Decimal, InvalidOperation
from typing import Any

import httpx
from tenacity import retry, stop_after_attempt, wait_exponential

from api.config import settings
from ingestion.publish import publish_crawl

log = logging.getLogger(__name__)
UA = "Mozilla/5.0 (clarifindata data crawler)"


def _dec(s: Any) -> Decimal | None:
    if s is None: return None
    s = str(s).replace(",", "").strip()
    if s in {"", "--", "X", "N/A"}: return None
    try: return Decimal(s)
    except (InvalidOperation, ValueError): return None


def _roc_iso(s: str | None) -> str | None:
    if not s: return None
    s = s.strip().replace(".", "/")
    if "/" in s:
        parts = s.split("/")
    elif len(s) >= 6:
        parts = [s[:-4], s[-4:-2], s[-2:]]
    else:
        return None
    try:
        y = int(parts[0])
        if y < 1911: y += 1911
        return date(y, int(parts[1]), int(parts[2])).isoformat()
    except (ValueError, IndexError):
        return None


@retry(stop=stop_after_attempt(3), wait=wait_exponential(min=1, max=10))
async def _get(url: str) -> list[dict[str, Any]]:
    async with httpx.AsyncClient(timeout=30, headers={"User-Agent": UA}) as cx:
        r = await cx.get(url)
        if r.status_code in (302, 404):
            return []
        r.raise_for_status()
        return r.json()


async def crawl_dividend() -> int:
    """t187ap45_L gives a snapshot of all 上市 companies' current-year
    dividend resolutions. Schema columns: stock_id, year, cash_dividend,
    stock_dividend, ex_date, payment_date.
    """
    url = "https://openapi.twse.com.tw/v1/opendata/t187ap45_L"
    raw = await _get(url)
    if not raw: return 0
    rows: list[dict[str, Any]] = []
    for r in raw:
        sid = (r.get("公司代號") or "").strip()
        if not sid: continue
        # 股利年度 stored as ROC year string e.g. "114"
        roc_year_str = (r.get("股利年度") or "").strip()
        try:
            year = int(roc_year_str) + 1911 if roc_year_str else None
        except ValueError:
            year = None
        if year is None: continue
        # Current-feed cash and stock dividends each have three components.
        cash_earn = _dec(r.get("股東配發-盈餘分配之現金股利(元/股)")) or Decimal("0")
        cash_lrsv = _dec(r.get("股東配發-法定盈餘公積發放之現金(元/股)")) or Decimal("0")
        cash_crsv = _dec(r.get("股東配發-資本公積發放之現金(元/股)")) or Decimal("0")
        stk_earn  = _dec(r.get("股東配發-盈餘轉增資配股(元/股)")) or Decimal("0")
        stk_lrsv  = _dec(r.get("股東配發-法定盈餘公積轉增資配股(元/股)")) or Decimal("0")
        stk_crsv  = _dec(r.get("股東配發-資本公積轉增資配股(元/股)")) or Decimal("0")
        cash_total = cash_earn + cash_lrsv + cash_crsv
        stock_total = stk_earn + stk_lrsv + stk_crsv
        cash = cash_total if cash_total > 0 else None
        stock = stock_total if stock_total > 0 else None
        # The feed has no ex-date; use the resolution date as an approximation.
        ex_d = _roc_iso(r.get("董事會（擬議）股利分派日") or r.get("股東會日期"))
        pay_d = None
        rows.append({
            "stock_id":       sid,
            "year":           year,
            "cash_dividend":  cash,
            "stock_dividend": stock,
            "ex_date":        ex_d,
            "payment_date":   pay_d,
        })
    if not rows: return 0
    await publish_crawl(
        dataset="TaiwanStockDividend",
        raw={"src": url, "n": len(raw)},
        rows=rows, key_parts=["L", date.today().isoformat()], source_url=url,
    )
    return len(rows)


async def crawl_dividend_history() -> int:
    """Collect archived dividend resolutions with cash and stock components.

    The archive provides neither ex-dividend nor payment dates.
    """
    total = 0
    for market in ("L", "O"):
        url = f"https://openapi.twse.com.tw/v1/opendata/t187ap39_{market}"
        raw = await _get(url)
        if not raw: continue
        rows: list[dict[str, Any]] = []
        for r in raw:
            sid = (r.get("公司代號") or "").strip()
            if not sid: continue
            roc_str = (r.get("股利年度") or "").strip()
            try:
                year = int(roc_str) + 1911 if roc_str else None
            except ValueError:
                year = None
            if year is None: continue
            cash_earn = _dec(r.get("股東配發內容-盈餘分配之現金股利(元/股)")) or Decimal("0")
            cash_rsv  = _dec(r.get("股東配發內容-法定盈餘公積、資本公積發放之現金(元/股)")) or Decimal("0")
            stk_earn  = _dec(r.get("股東配發內容-盈餘轉增資配股(元/股)")) or Decimal("0")
            stk_rsv   = _dec(r.get("股東配發內容-法定盈餘公積、資本公積轉增資配股(元/股)")) or Decimal("0")
            cash = cash_earn + cash_rsv
            stock = stk_earn + stk_rsv
            rows.append({
                "stock_id":       sid,
                "year":           year,
                "cash_dividend":  str(cash) if cash > 0 else None,
                "stock_dividend": str(stock) if stock > 0 else None,
                "ex_date":        None,
                "payment_date":   None,
            })
        if not rows: continue
        await publish_crawl(
            dataset="TaiwanStockDividend",
            raw={"src": url, "n": len(raw)},
            rows=rows, key_parts=[market, "hist", date.today().isoformat()],
            source_url=url,
        )
        total += len(rows)
    return total


def _classify_twse(sid: str) -> str:
    """Classify IDs starting with 00 as funds and other long IDs as warrants."""
    import re
    if re.match(r"^00", sid):
        return "etf"
    if len(sid) >= 6:
        return "warrant"
    return "equity"


def _classify_tpex(sid: str) -> str:
    """Classify fund prefixes and six-digit warrant IDs in the OTC market."""
    import re
    if re.match(r"^00", sid):
        return "etf"
    if re.match(r"^020", sid):
        return "etf"
    if re.match(r"^[67]\d{5}$", sid):
        return "warrant"
    return "equity"


async def crawl_overview_warrant() -> int:
    """Combine current TWSE and TPEX security snapshots with instrument types."""
    today = date.today().isoformat()
    rows: list[dict[str, Any]] = []

    # TWSE MI_INDEX supplies securities listed or traded on the current date.
    twse_url = (
        f"https://www.twse.com.tw/rwd/zh/afterTrading/MI_INDEX"
        f"?type=ALL&date={today.replace('-', '')}&response=json"
    )
    try:
        async with httpx.AsyncClient(timeout=45, headers={"User-Agent": UA}) as cx:
            r = await cx.get(twse_url)
            if r.status_code in (302, 404):
                twse_payload: dict[str, Any] = {}
            else:
                r.raise_for_status()
                twse_payload = r.json()
    except Exception as exc:
        log.warning("TWSE MI_INDEX fetch failed: %s", exc)
        twse_payload = {}

    tables = twse_payload.get("tables") or []
    # Select the individual-security table from the market-wide response.
    twse_table = next(
        (t for t in tables if t.get("data") and len(t.get("data", [])) > 1000),
        None,
    )
    if twse_table:
        for rec in twse_table["data"]:
            sid = (rec[0] if isinstance(rec, list) else rec.get("證券代號", "")).strip()
            name = (rec[1] if isinstance(rec, list) else rec.get("證券名稱", "")).strip()
            if not sid:
                continue
            rows.append({
                "stock_id":        sid,
                "stock_name":      name or None,
                "instrument_type": _classify_twse(sid),
                "trade_date":      today,
            })
    log.info("TWSE MI_INDEX ALL: %d instruments", len(rows))

    # TPEX daily closing quotes supply the current OTC security universe.
    tpex_url = "https://www.tpex.org.tw/openapi/v1/tpex_mainboard_daily_close_quotes"
    try:
        async with httpx.AsyncClient(timeout=45, headers={"User-Agent": UA}) as cx:
            r = await cx.get(tpex_url)
            if r.status_code in (302, 404):
                tpex_raw: list[Any] = []
            else:
                r.raise_for_status()
                tpex_raw = r.json()
    except Exception as exc:
        log.warning("TPEX daily-close fetch failed: %s", exc)
        tpex_raw = []

    tpex_count = 0
    for rec in tpex_raw:
        sid = (rec.get("SecuritiesCompanyCode") or "").strip()
        name = (rec.get("CompanyName") or "").strip()
        if not sid:
            continue
        rows.append({
            "stock_id":        sid,
            "stock_name":      name or None,
            "instrument_type": _classify_tpex(sid),
            "trade_date":      today,
        })
        tpex_count += 1
    log.info("TPEX daily-close: %d instruments", tpex_count)

    if not rows:
        log.warning("crawl_overview_warrant: both sources returned 0 rows — aborting")
        return 0

    # Do not publish a snapshot smaller than the required security universe.
    if len(rows) < 5000:
        log.error(
            "crawl_overview_warrant: only %d rows — looks like a fetch failure, aborting",
            len(rows),
        )
        return 0

    log.info("Total overview_warrant rows to upsert: %d", len(rows))
    for i in range(0, len(rows), 800):
        await publish_crawl(
            dataset="TaiwanStockOverviewIncludeWarrant",
            raw={"src": "MI_INDEX+tpex_daily", "n": len(rows)},
            rows=rows[i:i + 800],
            key_parts=["ALL", today, str(i)],
            source_url=twse_url,
        )
    return len(rows)


async def _main() -> None:
    logging.basicConfig(level=settings().log_level,
                        format="%(asctime)s %(levelname)s %(name)s %(message)s",
                        stream=sys.stdout)
    p = argparse.ArgumentParser()
    p.add_argument("--dataset", required=True,
                   choices=["dividend", "dividend-history", "warrant", "both"])
    args = p.parse_args()
    if args.dataset in ("dividend", "both"):
        n = await crawl_dividend(); print(f"dividend: {n}")
    if args.dataset == "dividend-history":
        n = await crawl_dividend_history(); print(f"dividend-history: {n}")
    if args.dataset in ("warrant", "both"):
        n = await crawl_overview_warrant(); print(f"warrant/overview: {n}")


if __name__ == "__main__":
    asyncio.run(_main())
