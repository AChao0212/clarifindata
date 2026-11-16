"""Recognise source refusal pages and statuses so a crawler backs off.

TWSE can return a refusal page with HTTP 307 and no Location header, or with
another refusal status. Raise WafRefusal so callers stop retrying the source.
"""
from __future__ import annotations

import urllib.error

# Recognised refusal statuses; inspect the response body where available.
WAF_STATUSES = frozenset({307, 403, 428})

_MARKERS = (
    "FOR SECURITY REASONS",
    "因為安全性考量",
    "THIS PAGE CAN NOT BE DISPLAYED",
)


class WafRefusal(RuntimeError):
    """The source refused the request. Do not retry inside this run."""


def is_waf_refusal(body: str | bytes | None) -> bool:
    if not body:
        return False
    if isinstance(body, bytes):
        body = body.decode("utf-8", errors="replace")
    return any(m in body for m in _MARKERS)


def raise_if_waf_http(exc: BaseException, source: str) -> None:
    """Translate a recognised urllib HTTP refusal into WafRefusal."""
    if not isinstance(exc, urllib.error.HTTPError):
        return
    if exc.code in WAF_STATUSES:
        try:
            body = exc.read()
        except Exception:  # noqa: BLE001 — body already consumed / not readable
            body = b""
        # A Location-less 307 IS the refusal; the body confirms it when readable.
        if exc.code == 307 or is_waf_refusal(body):
            raise WafRefusal(
                f"{source}: WAF refusal (HTTP {exc.code}, 「因為安全性考量」/"
                f"FOR SECURITY REASONS). Stopping requests to this source for this run."
            ) from exc


def raise_if_waf(body: str | bytes | None, source: str) -> None:
    """Raise WafRefusal for a refusal page so callers stop requesting the source."""
    if is_waf_refusal(body):
        raise WafRefusal(
            f"{source}: WAF refusal (「因為安全性考量」/FOR SECURITY REASONS). "
            f"Stopping requests to this source for this run."
        )
