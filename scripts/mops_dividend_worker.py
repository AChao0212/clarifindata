"""Collect annual dividend policies from MOPS.

The form posts each company and ROC year to ajax_t05st09_1. Distribution
rows are aggregated by company and year:
  cash_dividend  = earnings cash distribution + reserve cash distribution
  stock_dividend = earnings stock distribution + reserve stock distribution

Board and shareholder meeting dates are stored as policy dates. Collection
is sequential and establishes a form-page session before fetching reports.

Usage:
  python3 mops_dividend_worker.py --api http://localhost:8000 \
      --token <admin> --years 94,95,96 --tickers-file stocks.txt
"""
from __future__ import annotations

import argparse
import re
import sys
import time
from decimal import Decimal, InvalidOperation

import requests

UA = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/120.0 Safari/537.36"
BASE = "https://mopsov.twse.com.tw/mops/web"
FORM = f"{BASE}/t05st09_2"
AJAX = f"{BASE}/ajax_t05st09_1"


def _dec(s):
    if s is None:
        return None
    s = str(s).replace(",", "").strip()
    if s in {"", "-", "--", "X", "N/A"}:
        return None
    try:
        return Decimal(s)
    except (InvalidOperation, ValueError):
        return None


def _roc_iso(s):
    """100/02/15 → 2011-02-15."""
    s = (s or "").strip()
    m = re.match(r"(\d{2,3})/(\d{1,2})/(\d{1,2})", s)
    if not m:
        return None
    y, mo, d = int(m.group(1)) + 1911, int(m.group(2)), int(m.group(3))
    return f"{y:04d}-{mo:02d}-{d:02d}"


def _cells(tr):
    return [re.sub(r"<[^>]+>|&nbsp;", "", c).strip()
            for c in re.findall(r"<t[dh][^>]*>(.*?)</t[dh]>", tr, re.S)]


def parse(html: str, ad_year: int):
    """Aggregate all distribution rows for the year into one (stock_id, year)."""
    agg: dict[str, dict] = {}
    for table in re.findall(r"<table.*?</table>", html, re.S):
        trs = re.findall(r"<tr[^>]*>(.*?)</tr>", table, re.S)
        hdr_seen = False
        for tr in trs:
            cells = _cells(tr)
            if cells and cells[0] == "公司代號":
                hdr_seen = True
                continue
            if not hdr_seen or not cells or not re.fullmatch(r"\d{4}", (cells[0] or "")):
                continue
            sid = cells[0]
            def g(i):
                return _dec(cells[i]) if i < len(cells) else None
            cash = (g(5) or Decimal(0)) + (g(6) or Decimal(0))
            stock = (g(8) or Decimal(0)) + (g(9) or Decimal(0))
            board = _roc_iso(cells[11]) if len(cells) > 11 else None
            meeting = _roc_iso(cells[12]) if len(cells) > 12 else None
            slot = agg.setdefault(sid, {"stock_id": sid, "year": ad_year,
                                        "cash": Decimal(0), "stock": Decimal(0),
                                        "ex_date": None, "payment_date": None})
            slot["cash"] += cash
            slot["stock"] += stock
            slot["ex_date"] = slot["ex_date"] or meeting
            slot["payment_date"] = slot["payment_date"] or board
    rows = []
    for s in agg.values():
        if s["cash"] == 0 and s["stock"] == 0:
            continue
        rows.append({"stock_id": s["stock_id"], "year": s["year"],
                     "cash_dividend": str(s["cash"]), "stock_dividend": str(s["stock"]),
                     "ex_date": s["ex_date"], "payment_date": s["payment_date"]})
    return rows


def fetch(sess, co_id, roc_year, attempts=4):
    for _ in range(attempts):
        try:
            r = sess.post(AJAX, timeout=25, data={
                "encodeURIComponent": "1", "step": "1", "firstin": "1",
                "TYPEK": "sii", "year": str(roc_year), "co_id": co_id, "isnew": "false"})
            if r.status_code == 200 and len(r.content) > 12000:
                return r.content.decode("utf-8", errors="replace")
            if r.status_code == 200 and "查無" in r.content.decode("utf-8", "replace"):
                return ""  # No dividend policy was reported.
        except Exception:
            pass
        time.sleep(1.5)
    return None


def post(api, token, rows):
    if not rows:
        return 0
    for _ in range(3):
        try:
            r = requests.post(f"{api}/admin/ingest/dividend", json={"rows": rows},
                              headers={"Authorization": f"Bearer {token}"}, timeout=60)
            if r.status_code == 200:
                return len(rows)
        except Exception:
            pass
        time.sleep(2)
    return 0


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--api", required=True)
    ap.add_argument("--token", required=True)
    ap.add_argument("--years", required=True, help="comma ROC years, e.g. 94,95,96")
    ap.add_argument("--tickers-file", required=True)
    ap.add_argument("--sleep", type=float, default=0.5)
    args = ap.parse_args()
    years = [int(y) for y in args.years.split(",") if y.strip()]
    with open(args.tickers_file) as fh:
        tickers = [ln.strip() for ln in fh if ln.strip()]

    sess = requests.Session()
    sess.headers.update({"User-Agent": UA, "Referer": FORM})
    try:
        sess.get(FORM, timeout=20)  # warm cookie
    except Exception:
        pass

    ok = miss = rows_total = 0
    buf = []
    for yi, roc in enumerate(years):
        ad = roc + 1911
        for ti, co in enumerate(tickers):
            html = fetch(sess, co, roc)
            if html is None:
                miss += 1
            elif html:
                buf.extend(parse(html, ad))
                ok += 1
            if len(buf) >= 200:
                rows_total += post(args.api, args.token, buf); buf = []
            time.sleep(args.sleep)
            done = yi * len(tickers) + ti + 1
            if done % 200 == 0:
                rows_total += post(args.api, args.token, buf); buf = []
                print(f"progress {done}/{len(years)*len(tickers)} ok={ok} miss={miss} rows={rows_total}", flush=True)
        # re-warm cookie each year boundary
        try: sess.get(FORM, timeout=20)
        except Exception: pass
    rows_total += post(args.api, args.token, buf)
    print(f"DONE ok={ok} miss={miss} rows={rows_total}", flush=True)
    return 0


if __name__ == "__main__":
    sys.exit(main())
