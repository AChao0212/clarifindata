"""Collect the latest quarterly financial reports from TWSE and TPEX.

t187ap06 provides income statements and t187ap07 provides balance sheets.
Each industry has a separate dataset. This script combines listed and OTC
company reports and sends them to the local ingestion service.

Only the latest published quarter is available from these endpoints.

Usage: python mopsfin_quarterly.py
"""
from __future__ import annotations

import sys
from decimal import Decimal, InvalidOperation
from pathlib import Path

import httpx

INGEST_BS = "http://localhost:8000/admin/ingest/balance_sheet"
INGEST_FS = "http://localhost:8000/admin/ingest/financial_statement"

INDUSTRIES = ("basi", "ci", "fh", "ins", "bd", "mim")

# TWSE openapi (上市) + TPEX openapi (上櫃) — same schemas
TWSE_BASE = "https://openapi.twse.com.tw/v1/opendata"
TPEX_BASE = "https://www.tpex.org.tw/openapi/v1"


def _dec(s) -> str | None:
    if s is None: return None
    s = str(s).replace(",", "").strip()
    if s in {"", "-", "--", "X", "N/A"}: return None
    try: return str(Decimal(s))
    except (InvalidOperation, ValueError): return None


def parse_bs(records: list[dict]) -> list[dict]:
    out = []
    for r in records:
        sid = (r.get("公司代號") or r.get("SecuritiesCompanyCode") or "").strip()
        if not sid: continue
        roc_y = (r.get("年度") or r.get("Year") or "").strip()
        season = (r.get("季別") or r.get("Season") or "").strip()
        if not roc_y or not season: continue
        try:
            year = int(roc_y) + 1911
            q = int(season)
        except ValueError:
            continue
        # Different industries use different keys (資產總計 vs 資產總額)
        total_assets = (_dec(r.get("資產總額")) or _dec(r.get("資產總計"))
                        or _dec(r.get("流動資產")) and _dec(r.get("非流動資產")))
        if isinstance(total_assets, str): pass
        elif total_assets:
            total_assets = None
        total_assets = (_dec(r.get("資產總額")) or _dec(r.get("資產總計")))
        total_liab = (_dec(r.get("負債總額")) or _dec(r.get("負債總計")))
        total_equity = (_dec(r.get("權益總額")) or _dec(r.get("權益總計"))
                        or _dec(r.get("歸屬於母公司業主之權益合計")))
        if not any([total_assets, total_liab, total_equity]):
            continue
        out.append({
            "stock_id":          sid,
            "fiscal_year":       year,
            "fiscal_quarter":    q,
            "total_assets":      total_assets,
            "total_liabilities": total_liab,
            "total_equity":      total_equity,
        })
    return out


def parse_fs(records: list[dict]) -> list[dict]:
    out = []
    for r in records:
        sid = (r.get("公司代號") or r.get("SecuritiesCompanyCode") or "").strip()
        if not sid: continue
        roc_y = (r.get("年度") or r.get("Year") or "").strip()
        season = (r.get("季別") or r.get("Season") or "").strip()
        if not roc_y or not season: continue
        try:
            year = int(roc_y) + 1911
            q = int(season)
        except ValueError:
            continue
        revenue = (_dec(r.get("營業收入")) or _dec(r.get("收益"))
                   or _dec(r.get("收入")) or _dec(r.get("營業收入合計")))
        gross_profit = _dec(r.get("營業毛利"))
        op_income = _dec(r.get("營業利益"))
        net_income = (_dec(r.get("本期淨利（淨損）")) or _dec(r.get("本期淨利"))
                      or _dec(r.get("本期綜合損益總額")))
        eps = _dec(r.get("基本每股盈餘(元)") or r.get("基本每股盈餘"))
        if not any([revenue, gross_profit, op_income, net_income, eps]):
            continue
        out.append({
            "stock_id":         sid,
            "fiscal_year":      year,
            "fiscal_quarter":   q,
            "revenue":          revenue,
            "gross_profit":     gross_profit,
            "operating_income": op_income,
            "net_income":       net_income,
            "eps":              eps,
        })
    return out


def fetch(cx: httpx.Client, url: str) -> list[dict]:
    r = cx.get(url, timeout=60)
    if r.status_code != 200: return []
    try: return r.json()
    except Exception: return []


def main() -> None:
    token = (Path.home() / ".secret/cfd_admin_token").read_text().strip()
    bs_rows: list[dict] = []
    fs_rows: list[dict] = []
    with httpx.Client(headers={"User-Agent": "Mozilla/5.0"}) as cx:
        for ind in INDUSTRIES:
            # 上市 (TWSE)
            url_bs = f"{TWSE_BASE}/t187ap07_L_{ind}"
            url_fs = f"{TWSE_BASE}/t187ap06_L_{ind}"
            bs_data = fetch(cx, url_bs)
            fs_data = fetch(cx, url_fs)
            print(f"L_{ind}: bs={len(bs_data)} fs={len(fs_data)}")
            bs_rows.extend(parse_bs(bs_data))
            fs_rows.extend(parse_fs(fs_data))
            # 上櫃 (TPEX)
            url_bs_o = f"{TPEX_BASE}/mopsfin_t187ap07_O_{ind}"
            url_fs_o = f"{TPEX_BASE}/mopsfin_t187ap06_O_{ind}"
            bs_data_o = fetch(cx, url_bs_o)
            fs_data_o = fetch(cx, url_fs_o)
            print(f"O_{ind}: bs={len(bs_data_o)} fs={len(fs_data_o)}")
            bs_rows.extend(parse_bs(bs_data_o))
            fs_rows.extend(parse_fs(fs_data_o))
    print(f"\nTOTAL bs rows: {len(bs_rows)}, fs rows: {len(fs_rows)}")
    with httpx.Client(timeout=120) as cx:
        for label, url, rows in (("BS", INGEST_BS, bs_rows), ("FS", INGEST_FS, fs_rows)):
            if not rows: continue
            r = cx.post(url, json={"rows": rows},
                        headers={"Authorization": f"Bearer {token}"})
            print(f"{label}: status={r.status_code} body={r.text[:200]}")


if __name__ == "__main__":
    main()
