"""Collect listed futures and options contracts from TAIFEX market reports.

The public endpoints require no API key and may return JSON or UTF-8-BOM
CSV. Futures codes combine the contract and contract month. Options codes
also include the strike price and call/put designation.

The current contract snapshot is sent to /admin/ingest/fo_contract_info.

Usage: python taifex_contract_info.py
"""
from __future__ import annotations

import csv
import io
import sys
from datetime import date
from decimal import Decimal, InvalidOperation
from pathlib import Path

import requests

INGEST = "http://localhost:8000/admin/ingest/fo_contract_info"
FUT = "https://openapi.taifex.com.tw/v1/DailyMarketReportFut"
OPT = "https://openapi.taifex.com.tw/v1/DailyMarketReportOpt"
UA = "Mozilla/5.0 (clarifindata data crawler)"


def _num(s):
    s = (str(s) if s is not None else "").replace(",", "").strip()
    if s in {"", "-", "--"}:
        return None
    try:
        return str(Decimal(s))
    except (InvalidOperation, ValueError):
        return None


def _fetch_rows(url):
    """Fetch URL; auto-detect JSON array vs UTF-8-BOM CSV DictReader rows.
    Both response formats are decoded into dictionaries."""
    r = requests.get(url, headers={"User-Agent": UA}, timeout=30)
    r.raise_for_status()
    # Detect JSON: content starts with '[' (array) after stripping BOM/whitespace
    text = r.content.decode("utf-8-sig", errors="replace").lstrip()
    if text.startswith("["):
        import json
        return json.loads(text)
    return list(csv.DictReader(io.StringIO(text)))


def _contract_field(row: dict) -> str:
    """Extract contract name — JSON uses 'Contract', CSV uses '契約'."""
    return (row.get("Contract") or row.get("契約") or "").strip()


def _month_field(row: dict) -> str:
    """Extract contract month — JSON uses 'ContractMonth(Week)', CSV uses '到期月份(週別)'."""
    return (row.get("ContractMonth(Week)") or row.get("到期月份(週別)") or "").strip()


def _strike_field(row: dict):
    """Extract strike price — JSON uses 'StrikePrice', CSV uses '履約價'."""
    return _num(row.get("StrikePrice") or row.get("履約價"))


def _callput_field(row: dict):
    """Extract call/put — JSON uses 'CallPut', CSV uses '買賣權'."""
    return (row.get("CallPut") or row.get("買賣權") or "").strip() or None


def build_rows(today_iso: str):
    rows = {}
    # Futures
    for r in _fetch_rows(FUT):
        c = _contract_field(r)
        cm = _month_field(r)
        if not c or not cm:
            continue
        code = f"{c}{cm}"
        rows[code] = {"code": code, "name": c, "callput": None,
                      "contract_month": cm, "strike_price": None,
                      "listing_date": None, "updated_date": today_iso}
    # Options
    for r in _fetch_rows(OPT):
        c = _contract_field(r)
        cm = _month_field(r)
        if not c or not cm:
            continue
        strike = _strike_field(r)
        cp = _callput_field(r)
        code = f"{c}{cm}_{strike or ''}_{cp or ''}"
        rows[code] = {"code": code, "name": c, "callput": cp,
                      "contract_month": cm, "strike_price": strike,
                      "listing_date": None, "updated_date": today_iso}
    return list(rows.values())


def main() -> int:
    token = (Path.home() / ".secret/cfd_admin_token").read_text().strip()
    rows = build_rows(date.today().isoformat())
    n = 0
    for i in range(0, len(rows), 500):
        chunk = rows[i:i + 500]
        for _ in range(3):
            try:
                resp = requests.post(INGEST, json={"rows": chunk},
                                     headers={"Authorization": f"Bearer {token}"}, timeout=60)
                if resp.status_code == 200:
                    n += len(chunk); break
            except Exception:
                pass
    print(f"fo_contract_info: {n}/{len(rows)} rows", flush=True)
    return 0


if __name__ == "__main__":
    sys.exit(main())
