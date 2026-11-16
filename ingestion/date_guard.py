"""Validate that a TWSE daily response belongs to the requested date.

Some endpoints return another trading day's data when the requested date is
unavailable. Per-stock responses usually carry a YYYYMMDD ``date`` field.
Aggregate responses such as BFI82U use a ROC-format date in ``title`` instead.
Callers check response_date_matches before emitting rows.
"""
from __future__ import annotations

import re
from datetime import date
from typing import Any

# Matches an ROC date inside a title string, e.g. "115年06月02日" or "115年6月2日".
_ROC_TITLE_RE = re.compile(r"(\d{2,3})\s*年\s*(\d{1,2})\s*月\s*(\d{1,2})\s*日")


def _to_iso(target: date | str) -> str:
    return target if isinstance(target, str) else target.isoformat()


def _ymd_to_iso(s: str) -> str | None:
    """'20260526' -> '2026-05-26'. Returns None if not 8 digits."""
    s = (s or "").strip()
    if len(s) == 8 and s.isdigit():
        return f"{s[0:4]}-{s[4:6]}-{s[6:8]}"
    return None


def _roc_from_title(title: str) -> str | None:
    """Extract an ISO date from a TWSE ROC-format title, or None."""
    if not title:
        return None
    m = _ROC_TITLE_RE.search(title)
    if not m:
        return None
    roc_y, mm, dd = int(m.group(1)), int(m.group(2)), int(m.group(3))
    try:
        return date(roc_y + 1911, mm, dd).isoformat()
    except ValueError:
        return None


def response_date(payload: dict[str, Any], *, prefer_title: bool = False) -> str | None:
    """Best-effort extract of the date the TWSE response is *actually* for.

    Returns an ISO string ('YYYY-MM-DD') or None if no date can be located.

    * ``prefer_title=True`` -> read the ROC date from ``title`` first (use for
      aggregate endpoints like BFI82U where the top-level ``date`` is the
      report-generation date, not the data date).
    * otherwise -> read the top-level ``date`` (YYYYMMDD) first, falling back
      to the title.
    """
    title_iso = _roc_from_title(payload.get("title") or "")
    # Some endpoints carry per-table titles instead of a top-level one.
    if title_iso is None:
        for t in payload.get("tables") or []:
            title_iso = _roc_from_title(t.get("title") or "")
            if title_iso:
                break
    top_iso = _ymd_to_iso(str(payload.get("date") or ""))

    if prefer_title:
        return title_iso or top_iso
    return top_iso or title_iso


def response_date_matches(
    payload: dict[str, Any],
    target: date | str,
    *,
    prefer_title: bool = False,
    require_date: bool = True,
) -> bool:
    """True only if the response can be confirmed to be for ``target``.

    Gate row emission on a matching source date:

        if not response_date_matches(raw, target):
            return []

    ``require_date=True`` (default): if NO date can be extracted from the
    payload we treat it as un-trustworthy and return False (fail closed).
    Set ``require_date=False`` only for legacy endpoints that genuinely carry
    no date marker AND already echo the request reliably.
    """
    got = response_date(payload, prefer_title=prefer_title)
    if got is None:
        return not require_date
    return got == _to_iso(target)
