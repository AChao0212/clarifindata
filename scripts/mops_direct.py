"""Collect per-company quarterly financial statements from MOPS.

Use ROC years and quarters from 1 through 4. Company codes can be supplied
explicitly or loaded from the local ingestion service.

Usage:
  python mops_direct.py --year 113 --season 3 --workers 4
  python mops_direct.py --year 113 --season 3 --tickers 2330,2317
"""
from __future__ import annotations

import argparse
import asyncio
import logging
import re
import sys
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any

import httpx

UA = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/120.0 Safari/537.36"
INGEST_BASE = "http://localhost:8000/admin/ingest"
MOPS_URL = "https://mopsov.twse.com.tw/server-java/t164sb01"
# Request the form page first to establish a session cookie.
FORM_URL = "https://mopsov.twse.com.tw/mops/web/t164sb01"

log = logging.getLogger("mops_direct")


async def warm_session(cx: httpx.AsyncClient) -> None:
    """Seed the shared cookie jar with a MOPS session cookie."""
    try:
        await cx.get(FORM_URL, timeout=20)
    except Exception as e:
        log.warning("warm_session failed: %s", e)


def _dec(s: str) -> str | None:
    if s is None: return None
    s = str(s).replace(",", "").replace("(", "-").replace(")", "").strip()
    if s in {"", "-", "--", "X", "N/A"}: return None
    try: return str(Decimal(s))
    except (InvalidOperation, ValueError): return None


def _first_num_after(html: str, keyword: str) -> str | None:
    idx = html.find(keyword)
    if idx == -1: return None
    window = html[idx:idx + 2500]
    nums = re.findall(r">\s*(\(?[-\d][\d,.]+\)?)\s*<", window)
    return nums[0] if nums else None


async def fetch_mops(cx: httpx.AsyncClient, stock_id: str, year: int, season: int,
                    typek: str = "sii") -> str | None:
    data = {
        "step": "1", "CO_ID": stock_id, "SYEAR": str(year),
        "SSEASON": str(season), "REPORT_ID": "C", "TYPEK": typek,
        "queryName": "co_id", "inpuType": "co_id", "checkbtn": "",
        "isnew": "true", "firstin": "1",
    }
    for attempt in range(6):
        try:
            r = await cx.post(MOPS_URL, data=data, timeout=30)
            if r.status_code == 200 and len(r.content) > 20000:
                html = r.content.decode("big5", errors="replace")
                if "資產總計" in html or "營業收入" in html:
                    return html
                # Refresh the session when the response lacks statement data.
                await warm_session(cx)
                await asyncio.sleep(0.5)
            elif r.status_code in (403, 429, 503):
                await asyncio.sleep(5 * (attempt + 1))
            else:
                # Refresh the session after an incomplete response.
                await warm_session(cx)
        except Exception:
            await asyncio.sleep(2)
    return None


def parse_mops(html: str, stock_id: str, roc_year: int, season: int) -> dict[str, Any]:
    fiscal_year = roc_year + 1911
    bs_data = {
        "stock_id":          stock_id,
        "fiscal_year":       fiscal_year,
        "fiscal_quarter":    season,
        "total_assets":      _dec(_first_num_after(html, "資產總計") or ""),
        "total_liabilities": _dec(_first_num_after(html, "負債總計") or ""),
        "total_equity":      _dec(_first_num_after(html, "權益總計") or ""),
    }
    has_bs = any(bs_data[k] is not None for k in ("total_assets", "total_liabilities", "total_equity"))
    fs_data = {
        "stock_id":          stock_id,
        "fiscal_year":       fiscal_year,
        "fiscal_quarter":    season,
        "revenue":          _dec(_first_num_after(html, "營業收入合計") or ""),
        "gross_profit":     _dec(_first_num_after(html, "營業毛利") or ""),
        "operating_income": _dec(_first_num_after(html, "營業利益") or ""),
        "net_income":       _dec(_first_num_after(html, "本期淨利（淨損）") or ""),
        "eps":              _dec(_first_num_after(html, "基本每股盈餘") or ""),
    }
    has_fs = any(fs_data[k] is not None for k in ("revenue", "net_income", "eps"))
    cf_data = {
        "stock_id":          stock_id,
        "fiscal_year":       fiscal_year,
        "fiscal_quarter":    season,
        "operating_cf": _dec(_first_num_after(html, "營業活動之淨現金流入（流出）") or ""),
        "investing_cf": _dec(_first_num_after(html, "投資活動之淨現金流入（流出）") or ""),
        "financing_cf": _dec(_first_num_after(html, "籌資活動之淨現金流入（流出）") or ""),
    }
    has_cf = any(cf_data[k] is not None for k in ("operating_cf", "investing_cf", "financing_cf"))
    return {
        "balance_sheet": bs_data if has_bs else None,
        "financial_statement": fs_data if has_fs else None,
        "cash_flow": cf_data if has_cf else None,
    }


async def post(cx: httpx.AsyncClient, ingest: str, token: str, row: dict) -> bool:
    for attempt in range(3):
        try:
            r = await cx.post(f"{INGEST_BASE}/{ingest}", json={"rows": [row]},
                              headers={"Authorization": f"Bearer {token}"},
                              timeout=60)
            if r.status_code == 200:
                return True
        except Exception:
            pass
        await asyncio.sleep(1 + attempt)
    return False


async def worker(q: asyncio.Queue, year: int, season: int, token: str,
                 cx: httpx.AsyncClient, stats: dict, sleep: float) -> None:
    while True:
        try:
            stock_id = q.get_nowait()
        except asyncio.QueueEmpty:
            return
        try:
            await asyncio.sleep(sleep)
            html = await fetch_mops(cx, stock_id, year, season)
            if html is None:
                stats["fail"] += 1
            else:
                parsed = parse_mops(html, stock_id, year, season)
                for kind, row in parsed.items():
                    if row is None: continue
                    if await post(cx, kind, token, row):
                        stats[kind] += 1
                stats["ok"] += 1
            done = stats["ok"] + stats["fail"]
            if done % 50 == 0:
                log.info("p %d/%d ok=%d fail=%d bs=%d fs=%d cf=%d",
                         done, stats["total"], stats["ok"], stats["fail"],
                         stats["balance_sheet"], stats["financial_statement"],
                         stats["cash_flow"])
        except Exception as e:
            stats["fail"] += 1
            log.warning("err %s: %s", stock_id, e)
        finally:
            q.task_done()


async def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("--year",   type=int, required=True)
    p.add_argument("--season", type=int, required=True, choices=[1, 2, 3, 4])
    p.add_argument("--workers", type=int, default=3)
    p.add_argument("--sleep",   type=float, default=1.2)
    p.add_argument("--tickers", default="")
    args = p.parse_args()

    logging.basicConfig(level=logging.INFO,
                        format="%(asctime)s %(levelname)s %(name)s %(message)s",
                        stream=sys.stdout)

    token = (Path.home() / ".secret/cfd_admin_token").read_text().strip()

    async with httpx.AsyncClient(headers={"User-Agent": UA,
                                          "Referer": FORM_URL}) as cx:
        await warm_session(cx)
        if args.tickers:
            tickers = [t.strip() for t in args.tickers.split(",") if t.strip()]
        else:
            r = await cx.get(f"{INGEST_BASE}/list_listed_stocks",
                             headers={"Authorization": f"Bearer {token}"}, timeout=30)
            tickers = r.json()["stock_ids"]

        log.info("mops_direct %d/%d: %d tickers, %d workers, sleep=%.1fs",
                 args.year, args.season, len(tickers), args.workers, args.sleep)

        q: asyncio.Queue = asyncio.Queue()
        for t in tickers:
            q.put_nowait(t)
        stats = {"total": len(tickers), "ok": 0, "fail": 0,
                 "balance_sheet": 0, "financial_statement": 0, "cash_flow": 0}

        await asyncio.gather(*[
            worker(q, args.year, args.season, token, cx, stats, args.sleep)
            for _ in range(args.workers)
        ])

    log.info("DONE ok=%d fail=%d bs=%d fs=%d cf=%d",
             stats["ok"], stats["fail"], stats["balance_sheet"],
             stats["financial_statement"], stats["cash_flow"])


if __name__ == "__main__":
    asyncio.run(main())
