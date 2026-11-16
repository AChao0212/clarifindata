"""Collect quarterly financial statements from MOPS summary reports.

The listed and OTC markets provide separate reports for income statements,
balance sheets, and cash flows. Industry-specific columns are matched by
header name.

The form uses ROC years and single-digit quarters. Requests are sequential,
with a form-page request to establish a session before each report.
Historical collection starts at ROC year 102, when IFRS reporting began.

Usage:
  python mops_bulk_financials.py --start-year 102 --end-year 114
  python mops_bulk_financials.py --start-year 113 --end-year 114 --markets sii
"""
from __future__ import annotations

import argparse
import re
import sys
import time
from decimal import Decimal, InvalidOperation
from pathlib import Path

import requests

UA = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/120.0 Safari/537.36"
BASE = "https://mopsov.twse.com.tw/mops/web"
INGEST = "http://localhost:8000/admin/ingest"

REPORTS = {
    "t163sb04": "financial_statement",
    "t163sb05": "balance_sheet",
    "t163sb20": "cash_flow",
}

# value column → list of acceptable header names (industry variants)
INCOME_COLS = {
    "revenue":          ["營業收入", "收益", "利息淨收益", "營業收入合計"],
    "gross_profit":     ["營業毛利（毛損）", "營業毛利"],
    "operating_income": ["營業利益（損失）", "營業利益"],
    "net_income":       ["本期淨利（淨損）", "本期淨利", "本期綜合損益總額"],
    "eps":              ["基本每股盈餘（元）", "基本每股盈餘"],
}
BALANCE_COLS = {
    "total_assets":      ["資產總額", "資產總計"],
    "total_liabilities": ["負債總額", "負債總計"],
    "total_equity":      ["權益總額", "權益總計", "歸屬於母公司業主之權益合計"],
}
CASHFLOW_COLS = {
    "operating_cf": ["營業活動之淨現金流入（流出）", "營業活動之淨現金流入(流出)"],
    "investing_cf": ["投資活動之淨現金流入（流出）", "投資活動之淨現金流入(流出)"],
    "financing_cf": ["籌資活動之淨現金流入（流出）", "籌資活動之淨現金流入(流出)"],
}
COLMAP = {"t163sb04": INCOME_COLS, "t163sb05": BALANCE_COLS, "t163sb20": CASHFLOW_COLS}


def _dec(s):
    if s is None:
        return None
    s = str(s).replace(",", "").replace("(", "-").replace(")", "").strip()
    if s in {"", "-", "--", "X", "N/A"}:
        return None
    try:
        Decimal(s); return s
    except (InvalidOperation, ValueError):
        return None


def _cells(tr):
    return [re.sub(r"<[^>]+>|&nbsp;", "", c).strip()
            for c in re.findall(r"<t[dh][^>]*>(.*?)</t[dh]>", tr, re.S)]


def parse(html: str, report: str, ad_year: int, season: int) -> list[dict]:
    cols = COLMAP[report]
    out: list[dict] = []
    for table in re.findall(r"<table.*?</table>", html, re.S):
        trs = re.findall(r"<tr[^>]*>(.*?)</tr>", table, re.S)
        header = None
        idx: dict[str, int] = {}
        for tr in trs:
            cells = _cells(tr)
            nz = [c for c in cells if c]
            if nz and nz[0] == "公司代號":
                header = [c.strip() for c in cells]
                idx = {}
                for field, names in cols.items():
                    for nm in names:
                        if nm in header:
                            idx[field] = header.index(nm); break
                continue
            if header is None:
                continue
            # data row: first non-empty cell is a stock code
            if not cells or not re.fullmatch(r"\d{4}", (cells[0] or "").strip()):
                continue
            sid = cells[0].strip()
            row = {"stock_id": sid, "fiscal_year": ad_year, "fiscal_quarter": season}
            got = False
            for field in cols:
                i = idx.get(field)
                v = _dec(cells[i]) if (i is not None and i < len(cells)) else None
                row[field] = v
                got = got or v is not None
            if got:
                out.append(row)
    return out


def fetch(sess, report, roc_year, season, attempts=5):
    for _ in range(attempts):
        try:
            sess.get(f"{BASE}/{report}", timeout=20)  # warm cookie
            r = sess.post(f"{BASE}/ajax_{report}", timeout=40, data={
                "encodeURIComponent": "1", "step": "1", "firstin": "1", "off": "1",
                "isQuery": "Y", "TYPEK": sess.headers.get("_typek"),
                "year": str(roc_year), "season": str(season),
            })
            if r.status_code == 200 and len(r.content) > 5000:
                return r.content.decode("utf-8", errors="replace")
        except Exception:
            pass
        time.sleep(2)
    return None


def post(token, kind, rows):
    if not rows:
        return 0
    for _ in range(3):
        try:
            r = requests.post(f"{INGEST}/{kind}", json={"rows": rows},
                              headers={"Authorization": f"Bearer {token}"}, timeout=120)
            if r.status_code == 200:
                return len(rows)
        except Exception:
            pass
        time.sleep(2)
    return 0


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--start-year", type=int, default=102, help="ROC year")
    ap.add_argument("--end-year", type=int, default=114, help="ROC year")
    ap.add_argument("--markets", default="sii,otc")
    ap.add_argument("--sleep", type=float, default=0.8)
    args = ap.parse_args()
    token = (Path.home() / ".secret/cfd_admin_token").read_text().strip()
    markets = [m.strip() for m in args.markets.split(",") if m.strip()]
    stats = {v: 0 for v in REPORTS.values()}

    for roc in range(args.start_year, args.end_year + 1):
        ad = roc + 1911
        for season in (1, 2, 3, 4):
            for mk in markets:
                sess = requests.Session()
                sess.headers.update({"User-Agent": UA, "Referer": f"{BASE}/t163sb04", "_typek": mk})
                for report, kind in REPORTS.items():
                    html = fetch(sess, report, roc, season)
                    if not html:
                        print(f"  MISS {report} {mk} {ad}Q{season}", flush=True)
                        continue
                    rows = parse(html, report, ad, season)
                    n = post(token, kind, rows)
                    stats[kind] += n
                    time.sleep(args.sleep)
                print(f"{ad}Q{season} {mk}: fs={stats['financial_statement']} "
                      f"bs={stats['balance_sheet']} cf={stats['cash_flow']}", flush=True)
    print(f"DONE {stats}", flush=True)
    return 0


if __name__ == "__main__":
    sys.exit(main())
