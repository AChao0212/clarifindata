"""Check row-count floors after a source serves date-matched daily data.

Empty responses, holidays and mismatched dates are handled by the crawler before
these checks. A count below the configured floor raises CompletenessError.
"""
from __future__ import annotations

import logging
from datetime import date
from typing import Any

log = logging.getLogger(__name__)


class CompletenessError(RuntimeError):
    """The source served data but the captured row count is below its floor."""


def assert_captured_floor(
    *,
    dataset: str,
    captured: int,
    floor: int,
    target: date | str | None = None,
    detail: str = "",
) -> None:
    """Raise CompletenessError if a confirmed, date-matched capture is below its floor."""
    when = "" if target is None else f" {target}"
    if captured < floor:
        msg = (
            f"Partial/empty capture: {dataset}{when} captured {captured} rows "
            f"but expected >= {floor} for a date the source served data."
        )
        if detail:
            msg += f" [{detail}]"
        log.error(msg)
        raise CompletenessError(msg)
    log.info(
        "%s%s completeness OK: %d rows (floor %d)%s",
        dataset, when, captured, floor, f" [{detail}]" if detail else "",
    )


def era_floor(table: str, date_col: str, target: date, cap: int, frac: float = 0.7) -> int:
    """Derive a historical floor from nearby stored dates, capped at ``cap``.

    Use ``frac`` times the smaller nonempty count before and after the target.
    If no neighboring count is available, retain the absolute floor.
    """
    try:
        import psycopg

        from api.config import settings
        with psycopg.connect(settings().database_url) as conn:
            row = conn.execute(
                f"""SELECT
                      (SELECT count(*) FROM {table} WHERE {date_col} =
                         (SELECT max({date_col}) FROM {table} WHERE {date_col} < %(d)s)),
                      (SELECT count(*) FROM {table} WHERE {date_col} =
                         (SELECT min({date_col}) FROM {table} WHERE {date_col} > %(d)s))""",  # noqa: S608
                {"d": target}).fetchone()
    except Exception as e:  # noqa: BLE001 - retain the floor if its lookup fails
        log.warning("era_floor lookup failed (%s) — using the absolute floor %d", e, cap)
        return cap
    near = [n for n in row if n]
    if not near:
        return cap
    return max(1, min(cap, int(frac * min(near))))


def served_real_data(raw: dict[str, Any] | None) -> bool:
    """True iff the TWSE JSON payload positively carries real rows for a date
    (``stat == "OK"``). A holiday / no-data response has stat != OK (or is empty),
    in which case the caller legitimately returns 0 rows and must NOT floor-check."""
    if not raw:
        return False
    return raw.get("stat") in ("OK", None) and bool(
        raw.get("data") or raw.get("tables")
    )
