"""Run a crawler with shared date selection, completeness checks and optional alerts.

Each crawler supplies a fetch function returning its row count and a Collector
instance. Run that crawler's module to select dates, check the trading calendar,
and return a nonzero exit code on collection errors or incomplete results.
"""
from __future__ import annotations

import argparse
import datetime as _dt
import logging
import os
import time
import urllib.parse
import urllib.request
from dataclasses import dataclass, field
from typing import Callable, Optional

from ingestion.waf_guard import WafRefusal
from ingestion.trading_calendar import (
    CALENDAR_ERROR, NON_TRADING, TRADING, recent_trading_days, trading_day_status,
)

CST = _dt.timezone(_dt.timedelta(hours=8))
_SECRET = os.path.expanduser("~/.secret/telegram.txt")


def telegram(text: str) -> None:
    """Send an optional alert using ~/.secret/telegram.txt; ignore delivery errors."""
    try:
        token = chat = ""
        with open(_SECRET) as f:
            for line in f:
                if line.startswith("BOT_TOKEN"):
                    token = line.split("=", 1)[1].strip().strip("'\"")
                elif line.startswith("BOT_CHAT_ID"):
                    chat = line.split("=", 1)[1].strip().strip("'\"")
        if not token or not chat:
            return
        data = urllib.parse.urlencode({"chat_id": chat, "text": text}).encode()
        urllib.request.urlopen(
            urllib.request.Request(f"https://api.telegram.org/bot{token}/sendMessage", data=data),
            timeout=15,
        )
    except Exception:  # noqa: BLE001
        pass


@dataclass
class Collector:
    """A uniform, launchable data collector. See module docstring."""

    name: str
    fetch: Callable[[Optional[_dt.date]], int]
    market: str = "tw"            # "tw" → gate on the TW trading calendar; "none" → no gate
    per_date: bool = True         # loop fetch(day) over dates; False → fetch(None) once
    min_rows: int = 0             # minimum accepted row count for each date
    default_recent: int = 3       # default number of recent trading days
    # Exclude today from the recent window until the source's publication hour,
    # measured in Taiwan time.
    publish_hour: int = 14
    completeness: Optional[Callable[[Optional[_dt.date], int], bool]] = None  # custom floor
    label: str = ""
    log: logging.Logger = field(default=None, repr=False)  # type: ignore[assignment]

    def __post_init__(self) -> None:
        if self.log is None:
            logging.basicConfig(
                level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
            self.log = logging.getLogger(self.name)
        if not self.label:
            self.label = self.name

    # Command-line arguments.
    def _parse(self, argv) -> argparse.Namespace:
        p = argparse.ArgumentParser(
            prog=f"python -m ingestion.crawlers.{self.name}", description=self.label)
        g = p.add_mutually_exclusive_group()
        g.add_argument("--date", help="single date YYYY-MM-DD")
        g.add_argument("--dates", help="comma-separated dates YYYY-MM-DD,YYYY-MM-DD")
        g.add_argument("--recent", type=int, metavar="N",
                       help=f"last N trading days (default {self.default_recent})")
        p.add_argument("--backfill", nargs=2, metavar=("START", "END"),
                       help="inclusive date range START END (trading days only)")
        p.add_argument("--force", action="store_true",
                       help="bypass the trading-day gate (run the given dates regardless)")
        p.add_argument("--quiet", action="store_true", help="suppress the success summary")
        p.add_argument("--no-alert", action="store_true",
                       help="suppress Telegram alerts; errors still produce a nonzero exit")
        return p.parse_args(argv)

    # Resolve requested dates against the trading calendar.
    def _resolve_dates(self, a: argparse.Namespace) -> Optional[list[Optional[_dt.date]]]:
        """Returns the list of dates to run, or None to signal 'abort' (CALENDAR_ERROR).
        For market='none' non-dated sources, returns [None]."""
        if not self.per_date or self.market == "none":
            return [None]
        today = _dt.datetime.now(CST).date()

        if a.date:
            want = [_dt.date.fromisoformat(a.date)]
        elif a.dates:
            want = [_dt.date.fromisoformat(d.strip()) for d in a.dates.split(",") if d.strip()]
        elif a.backfill:
            s, e = (_dt.date.fromisoformat(x) for x in a.backfill)
            want = [s + _dt.timedelta(days=i) for i in range((e - s).days + 1)]
        else:
            n = a.recent or self.default_recent
            rt = recent_trading_days(n, today)
            if rt is None:                      # CALENDAR_ERROR on the default path
                return None
            days = [_dt.date.fromisoformat(d) for d in rt]
            now = _dt.datetime.now(CST)
            if now.hour < self.publish_hour and today in days:
                self.log.info("%s: dropping %s from the window — it is %02d:%02d CST and this "
                              "source publishes from %02d:00; an unfinished session is not a "
                              "漏抓", self.name, today, now.hour, now.minute, self.publish_hour)
                days = [d for d in days if d != today]
            return days

        if a.force or self.market != "tw":
            return want
        # gate the explicitly-requested dates to real trading days
        kept: list[Optional[_dt.date]] = []
        for d in want:
            st = trading_day_status(d)
            if st == TRADING:
                kept.append(d)
            elif st == NON_TRADING:
                self.log.info("%s: %s is a non-trading day — skip", self.name, d)
            else:  # CALENDAR_ERROR
                return None
        return kept

    # Execute collection and aggregate failures.
    def run(self, argv=None) -> int:
        a = self._parse(argv)
        # The scheduler can suppress per-run alerts when it sends its own summary.
        suppress = a.no_alert or bool(os.environ.get("CFD_NO_ALERT"))
        t0 = time.time()
        dates = self._resolve_dates(a)
        if dates is None:
            msg = (f"🔴 {self.name}: 交易日曆讀取失敗（CALENDAR_ERROR），"
                   f"無法判斷交易日，不盲跑。查 taiwan_stock_trading_date / DB。")
            self.log.error(msg)
            if not suppress:
                telegram(msg)
            return 1
        if not dates:
            self.log.info("%s: nothing to do (no trading days in range)", self.name)
            return 0

        failures: list[str] = []
        total = 0
        for d in dates:
            tag = d.isoformat() if d else "once"
            try:
                n = int(self.fetch(d) or 0)
                total += n
                ok = self.completeness(d, n) if self.completeness else (n >= self.min_rows)
                if ok:
                    self.log.info("[OK] %s %s rows=%d", self.name, tag, n)
                else:
                    # Results below the configured floor make the run fail.
                    floor = self.min_rows
                    failures.append(f"{tag}: 漏抓 rows={n} < floor {floor}")
                    self.log.error("[漏抓] %s %s rows=%d < floor %d", self.name, tag, n, floor)
            except WafRefusal as e:
                # Stop the remaining dates when the source refuses requests.
                left = len(dates) - dates.index(d) - 1
                failures.append(f"{tag}: WAF refusal — run stopped, {left} date(s) not attempted")
                self.log.error("[WAF] %s %s: %s — stopping, %d date(s) not attempted",
                               self.name, tag, e, left)
                break
            except Exception as e:  # noqa: BLE001 — never swallow; record + fail loud
                failures.append(f"{tag}: {type(e).__name__}: {e}")
                self.log.exception("[FAIL] %s %s", self.name, tag)

        secs = round(time.time() - t0, 1)
        if failures:
            head = f"🔴 clarifindata {self.name} 失敗/漏抓（{len(failures)}/{len(dates)}）{secs}s"
            if not suppress:
                telegram(head + "\n" + "\n".join(failures[:8]))
            self.log.error("%s DONE with %d failures in %ss", self.name, len(failures), secs)
            return 1
        if not a.quiet:
            self.log.info("%s DONE ok days=%d rows=%d %ss", self.name, len(dates), total, secs)
        return 0
