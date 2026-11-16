"""Read official trading-day flags from Postgres.

Return a separate calendar-error state if the database is unavailable or its
calendar does not yet cover the requested date. Callers decide whether to stop.
"""
from __future__ import annotations

import datetime as _dt
import os

DB = os.environ.get("CFD_DB_URL", "postgresql://clarifindata:dev@localhost:5432/clarifindata")

# the three states (step 1 + 2 collapsed into one explicit answer)
TRADING = "trading"
NON_TRADING = "non_trading"
CALENDAR_ERROR = "calendar_error"


def trading_day_status(d: _dt.date) -> str:
    """3-state trading-day check for `d`. Returns TRADING / NON_TRADING / CALENDAR_ERROR.

    CALENDAR_ERROR means we genuinely could not determine it (DB down, or the calendar is
    not extended up to `d` yet) — the caller must handle it explicitly, NOT guess."""
    try:
        import psycopg
        with psycopg.connect(DB, connect_timeout=5) as c, c.cursor() as cur:
            cur.execute("SELECT 1 FROM taiwan_stock_trading_date "
                        "WHERE trade_date = %s AND is_trading_day", (d,))
            if cur.fetchone():
                return TRADING
            # `d` is not flagged a trading day. Trust that as NON_TRADING only if the
            # calendar actually COVERS d (extended to/past it); otherwise it's unknown.
            cur.execute("SELECT max(trade_date) FROM taiwan_stock_trading_date")
            mx = cur.fetchone()[0]
            if mx is not None and mx >= d:
                return NON_TRADING
            return CALENDAR_ERROR   # calendar behind d -> cannot decide
    except Exception:               # noqa: BLE001 — DB/transport failure -> cannot decide
        return CALENDAR_ERROR


def is_trading_day(d: _dt.date) -> bool | None:
    """True / False, or None on CALENDAR_ERROR (caller decides). Convenience over
    trading_day_status() for the common 'skip on non-trading' gate."""
    s = trading_day_status(d)
    return None if s == CALENDAR_ERROR else (s == TRADING)


def recent_trading_days(n: int, today: _dt.date) -> list[str] | None:
    """Last `n` official trading days <= today (ISO, newest first). None on calendar
    error (so the caller can distinguish 'no trading days' from 'calendar unavailable')."""
    try:
        import psycopg
        with psycopg.connect(DB, connect_timeout=5) as c, c.cursor() as cur:
            cur.execute("SELECT trade_date FROM taiwan_stock_trading_date "
                        "WHERE is_trading_day AND trade_date <= %s "
                        "ORDER BY trade_date DESC LIMIT %s", (today, n))
            rows = [r[0].isoformat() for r in cur.fetchall()]
            return rows or None
    except Exception:               # noqa: BLE001
        return None
