"""Collect Bank of Taiwan daily cash and spot exchange-rate quotes.

The dated CSV endpoint supplies one row per currency. Publish all available
buy and sell quotes with a spot midpoint, or a cash midpoint when spot quotes
are absent. Empty non-business days are skipped; incomplete captures and
HTML challenge responses raise errors without alternate access methods.

Run: python -m ingestion.crawlers.bot_exchange_rate --date 2026-05-22
"""
from __future__ import annotations

import asyncio
import csv
import io
import logging
from datetime import date, timedelta
from decimal import Decimal, InvalidOperation
from typing import Any

import httpx
from tenacity import retry, stop_after_attempt, wait_exponential

from ingestion.collector import Collector
from ingestion.publish import publish_crawl

log = logging.getLogger(__name__)

BASE_URL = "https://rate.bot.com.tw/xrt/flcsv/0"
USER_AGENT = "Mozilla/5.0 (clarifindata data crawler; +https://github.com/clarifindata)"

# Require at least 18 of the 19 published currencies before storing a day.
MIN_CURRENCIES = 18


class CrawlIncomplete(RuntimeError):
    """The capture is empty or incomplete for a finalized business-day CSV."""


class SourceWAFBlocked(RuntimeError):
    """The source returned an HTML challenge instead of exchange-rate data."""


def _is_waf_challenge(text: str) -> bool:
    """Recognize HTML responses where the endpoint should return plain CSV."""
    head = text.lstrip()[:300].lower()
    return head.startswith("<!doctype html") or head.startswith("<html") or "challenge validation" in head


def _dec(s: str) -> Decimal | None:
    s = s.strip().replace(",", "")
    if s in {"", "-", "0", "0.00000"}:
        return None
    try:
        return Decimal(s)
    except (InvalidOperation, ValueError):
        return None


@retry(stop=stop_after_attempt(3), wait=wait_exponential(min=1, max=10))
async def fetch_csv(target: date | None) -> str:
    url = BASE_URL + ("/day" if target is None else f"/{target.isoformat()}")
    from curl_cffi import AsyncSession
    async with AsyncSession() as s:
        r = await s.get(url, impersonate="chrome", timeout=20)
        r.raise_for_status()
        return r.text


def parse_rows(text: str, trade_date: date) -> list[dict[str, Any]]:
    """Parse cash and spot buy/sell quotes from each currency's CSV row.

    Both quote groups appear inline. The reference rate uses the midpoint of
    available spot quotes, or cash quotes when spot quotes are absent.
    """
    reader = csv.reader(io.StringIO(text))
    rows = list(reader)
    if not rows:
        return []
    rows = rows[1:]  # drop header

    iso = trade_date.isoformat()
    out: list[dict[str, Any]] = []
    for r in rows:
        if len(r) < 4:
            continue
        currency = r[0].strip()
        # Accept Chinese and English labels for the buying quote group.
        if not currency or r[1].strip() not in ("本行買入", "Buying"):
            continue
        cash_buy  = _dec(r[2])  if len(r) > 2  else None   # 現金買入
        spot_buy  = _dec(r[3])  if len(r) > 3  else None   # 即期買入
        cash_sell = _dec(r[12]) if len(r) > 12 else None   # 現金賣出
        spot_sell = _dec(r[13]) if len(r) > 13 else None   # 即期賣出
        spot_vals = [v for v in (spot_buy, spot_sell) if v is not None]
        cash_vals = [v for v in (cash_buy, cash_sell) if v is not None]
        if spot_vals:                                      # SPOT mid (majority ccy)
            mid = sum(spot_vals) / Decimal(len(spot_vals))
        elif cash_vals:                                    # CASH-only ccy fallback
            mid = sum(cash_vals) / Decimal(len(cash_vals))
        else:
            continue                                       # no usable quote → skip
        out.append({
            "trade_date": iso, "currency": currency, "rate": mid,
            "cash_buy": cash_buy, "cash_sell": cash_sell,
            "spot_buy": spot_buy, "spot_sell": spot_sell,
        })
    return out


async def _publish_one(target: date) -> int:
    """Publish one dated CSV, skipping empty non-business-day responses."""
    text = await fetch_csv(target)
    if _is_waf_challenge(text):
        raise SourceWAFBlocked(
            f"exchange-rate {target}: source returned an HTML challenge "
            f"({len(text)} bytes), not CSV; stopping collection for this date")
    rows = parse_rows(text, target)
    if not rows:
        # Short empty responses represent non-business days; substantive CSV
        # content without parsed rows indicates an unsupported response layout.
        if len(text.strip()) > 40:
            raise CrawlIncomplete(
                f"exchange-rate {target}: BOT returned {len(text)} bytes of CSV but "
                f"parse yielded 0 rows; refusing to treat it as a non-business day")
        return 0
    # Do not publish a partial currency set from a finalized daily response.
    if len(rows) < MIN_CURRENCIES:
        raise CrawlIncomplete(
            f"exchange-rate {target}: only {len(rows)} currencies parsed "
            f"(floor {MIN_CURRENCIES}); refusing to store a partial capture")
    src = f"{BASE_URL}/{target.isoformat()}"
    await publish_crawl(
        dataset="ExchangeRate",
        raw={"source": src, "csv_length": len(text)},
        rows=rows,
        key_parts=["BOT", target.isoformat()],
        source_url=src,
        trade_date=target.isoformat(),
    )
    return len(rows)


async def crawl_and_publish(target: date | None = None, window: int = 5) -> int:
    """Publish one date or a trailing calendar-day window ending today.

    Dated responses update existing rows by date and currency. Non-business
    days are skipped. Collect remaining dates after a date-specific error;
    a window with no captured business-day data raises CrawlIncomplete.
    """
    if target is not None:
        # A zero result represents an empty response for this date.
        n = await _publish_one(target)
        if not n:
            log.warning("no exchange-rate rows for %s (non-business day or empty CSV)", target)
        return n
    today = date.today()
    total = 0
    days_with_rows = 0
    errors: list[str] = []
    for back in range(window, -1, -1):
        d = today - timedelta(days=back)
        try:
            n = await _publish_one(d)
            total += n
            if n:
                days_with_rows += 1
        except Exception as e:  # Continue with other dates without alternate access.
            log.warning("exchange-rate %s: %s", d, e)
            errors.append(f"{d}:{e}")
    # A window with no captured data cannot be reported as a successful update.
    if days_with_rows == 0:
        raise CrawlIncomplete(
            f"exchange-rate: 0 currencies captured across the trailing {window}-day "
            f"window ending {today}; no data captured "
            f"(errors={errors or 'none'})")
    if errors:
        log.warning("exchange-rate: %d window day(s) errored (others succeeded): %s",
                    len(errors), errors)
    return total


def _fetch(day: date) -> int:
    """Collect one trading date and propagate incomplete or blocked responses."""
    return asyncio.run(_publish_one(day))


# The daily capture validates currency counts and propagates source errors.
# Empty non-business days are allowed. The default recent window republishes
# the last five trading dates, including revised quotes.
COLLECTOR = Collector(
    "bot_exchange_rate", _fetch, market="tw", min_rows=0, default_recent=5,
    label="ExchangeRate",
)

if __name__ == "__main__":
    raise SystemExit(COLLECTOR.run())
