"""Collect monthly US currency in circulation from FRED.

CURRCIR observations are extended with monthly averages of weekly WCURCIR
observations after validating the sources' overlapping months. Only complete
months are included in the extension.

Each run sends the full history to /admin/ingest/currency_circulation.
Incomplete, inconsistent, or stale observations cause a nonzero exit.

Usage:
  python macro_extra.py currency
"""
from __future__ import annotations

import argparse
import csv
import io
import sys
from datetime import date
from decimal import Decimal
from pathlib import Path

import requests

INGEST = "http://localhost:8000/admin/ingest"
FRED = "https://fred.stlouisfed.org/graph/fredgraph.csv"

# FRED series settings: country, series identifier, start date, minimum row count.
_CURRENCY = [
    ("US",    "CURRCIR", "1990-01-01", 300),  # currency in circulation (since 1990)
]


class CrawlIncomplete(RuntimeError):
    """Signal a failed or incomplete currency collection to the scheduler."""


def _token() -> str:
    return (Path.home() / ".secret/cfd_admin_token").read_text().strip()


def _post(kind: str, rows: list[dict], token: str) -> int:
    if not rows:
        return 0
    last_err = "unknown"
    for _ in range(3):
        try:
            r = requests.post(f"{INGEST}/{kind}", json={"rows": rows},
                              headers={"Authorization": f"Bearer {token}"}, timeout=90)
            if r.status_code == 200:
                return len(rows)
            last_err = f"HTTP {r.status_code}: {r.text[:200]}"
        except Exception as e:  # Preserve the final error after retries.
            last_err = str(e)
    # Exhausted ingestion attempts are reported as collection failures.
    raise CrawlIncomplete(f"ingest {kind} failed after 3 attempts: {last_err}")


# CURRCIR is measured in billions of dollars. Average weekly WCURCIR values
# within each complete month and convert from millions to billions.
# Validate the overlapping months before extending the monthly series.
WEEKLY_SERIES = "WCURCIR"          # Millions of USD, observed on Wednesdays.
_OVERLAP_TOL = Decimal("0.02")     # Allowed rounding difference in billions.
_MAX_STALE_DAYS = 75              # Maximum age of the latest monthly observation.


def _fetch_fred(series: str, start_date: str) -> list[tuple[str, str]]:
    resp = requests.get(FRED, params={"id": series, "cosd": start_date}, timeout=120)
    resp.raise_for_status()
    out: list[tuple[str, str]] = []
    rd = csv.reader(io.StringIO(resp.text))
    next(rd, None)
    for line in rd:
        if len(line) < 2:
            continue
        d, v = line[0].strip(), line[1].strip()
        if not d or v in {"", "."}:
            continue
        out.append((d, v))
    return out


def _monthly_from_weekly(weekly: list[tuple[str, str]]) -> dict[str, Decimal]:
    """Average weekly observations by complete month and convert to billions.

    The newest observed month is excluded until a later month is available.
    """
    buckets: dict[str, list[Decimal]] = {}
    for d, v in weekly:
        buckets.setdefault(d[:7], []).append(Decimal(v))
    if not buckets:
        return {}
    newest = max(buckets)
    return {
        f"{month}-01": (sum(vals) / len(vals) / 1000).quantize(Decimal("0.001"))
        for month, vals in buckets.items()
        if month != newest
    }


def _validate_overlap(official: dict[str, Decimal], derived: dict[str, Decimal]) -> list[str]:
    """Compare the most recent 24 months shared by both monthly series."""
    shared = sorted(set(official) & set(derived))[-24:]
    if len(shared) < 12:
        return [f"{WEEKLY_SERIES}: only {len(shared)} months overlap CURRCIR — "
                f"cannot prove the monthly-average identity"]
    return [f"{m}: CURRCIR {official[m]} vs {WEEKLY_SERIES}-avg {derived[m]}"
            for m in shared if abs(official[m] - derived[m]) > _OVERLAP_TOL]


def crawl_currency(token: str) -> int:
    total = 0
    errors: list[str] = []
    for country, series, start_date, min_rows in _CURRENCY:
        try:
            rows_raw = _fetch_fred(series, start_date)
        except Exception as e:
            # Continue collecting other series, then report accumulated failures.
            print(f"fred {series}: {e}", flush=True)
            errors.append(f"FRED:{series}:{e}")
            continue
        rows = [{"country": country, "stat_date": d, "value": v} for d, v in rows_raw]
        # Do not ingest a full-history response below the expected minimum size.
        if len(rows) < min_rows:
            errors.append(f"{series}:parsed {len(rows)} < floor {min_rows}")
            print(f"currency {country} ({series}): {len(rows)} rows < floor "
                  f"{min_rows}; skipping incomplete response", flush=True)
            continue

        # Extend past the monthly series' last observation with the weekly average.
        try:
            weekly = _fetch_fred(WEEKLY_SERIES, start_date)
        except Exception as e:
            print(f"fred {WEEKLY_SERIES}: {e}", flush=True)
            errors.append(f"FRED:{WEEKLY_SERIES}:{e}")
            weekly = []
        if weekly:
            official = {d: Decimal(v) for d, v in rows_raw}
            derived = _monthly_from_weekly(weekly)
            mismatches = _validate_overlap(official, derived)
            if mismatches:
                errors.append(f"{WEEKLY_SERIES}: monthly-average identity broken: "
                              f"{mismatches[:3]}")
                print(f"currency {country}: {WEEKLY_SERIES} no longer reproduces "
                      f"{series} — NOT extending: {mismatches[:3]}", flush=True)
            else:
                extra = sorted(m for m in derived if m > max(official))
                rows += [{"country": country, "stat_date": m, "value": str(derived[m])}
                         for m in extra]
                if extra:
                    print(f"currency {country}: {series} ends {max(official)}; extended "
                          f"with {len(extra)} month(s) of {WEEKLY_SERIES} average "
                          f"({extra[0]}..{extra[-1]})", flush=True)

        # Check freshness independently of the full-history row count.
        newest = max(r["stat_date"] for r in rows)
        stale = (date.today() - date.fromisoformat(newest)).days
        if stale > _MAX_STALE_DAYS:
            errors.append(f"{series}:newest observation {newest} is {stale}d old "
                          f"(> {_MAX_STALE_DAYS}d) freshness limit")
            print(f"currency {country} ({series}): newest {newest} is {stale}d old "
                  f"— posting available data with a failing exit status", flush=True)

        n = _post("currency_circulation", rows, token)  # Raises on ingestion failure.
        total += n
        print(f"currency {country} ({series}): {len(rows)} rows → posted {n}", flush=True)
    if errors:
        raise CrawlIncomplete(
            f"currency: {len(errors)}/{len(_CURRENCY)} series failed/partial: {errors} "
            f"— currency collection is incomplete")
    return total


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("cmd", choices=["currency"])
    args = ap.parse_args()
    n = crawl_currency(_token())
    print(f"{args.cmd}: {n} rows", flush=True)
    return 0


if __name__ == "__main__":
    sys.exit(main())
