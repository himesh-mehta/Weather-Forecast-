"""
Shared HTTP client: polite, fair-use behaviour for every upstream API.

  * Descriptive User-Agent with a contact URL on every request (MET Norway's terms require
    one; Open-Meteo and NOAA ask for it). Set CONTACT_URL in .env / the workflow.
  * Retries transient failures (connection errors, timeouts, 500/502/503/504) with exponential
    backoff + jitter: 1 s, 2 s, ... capped, at most MAX_RETRIES extra attempts.
  * HTTP 429 (rate limited): honours a short Retry-After once; otherwise returns the 429 so the
    caller stops calling that provider for the rest of the run (see ingest._run_provider).
    We never rotate keys or accounts to get around a provider's limit.
"""
import asyncio
import os
import random
from datetime import datetime, timezone
from email.utils import parsedate_to_datetime

import httpx

CONTACT = os.getenv("CONTACT_URL", "").strip()
USER_AGENT = f"WeatherConsensus/1.0 (+{CONTACT})" if CONTACT else "WeatherConsensus/1.0 (non-commercial research)"
RETRY_STATUSES = {500, 502, 503, 504}
MAX_RETRIES = 2
BACKOFF_BASE_S = 1.0
BACKOFF_CAP_S = 20.0
MAX_RETRY_AFTER_S = 30.0  # a 429 asking us to wait longer than this ends the provider's run instead


def retry_after_seconds(resp: httpx.Response) -> float | None:
    value = resp.headers.get("retry-after")
    if not value:
        return None
    try:
        return max(0.0, float(value))
    except ValueError:
        try:
            return max(0.0, (parsedate_to_datetime(value) - datetime.now(timezone.utc)).total_seconds())
        except (TypeError, ValueError):
            return None


class PoliteTransport(httpx.AsyncBaseTransport):
    def __init__(self, inner: httpx.AsyncBaseTransport | None = None, max_retries: int = MAX_RETRIES,
                 sleep=asyncio.sleep):
        self.inner = inner or httpx.AsyncHTTPTransport()
        self.max_retries = max_retries
        self.sleep = sleep
        self.retries_done = 0  # for tests / diagnostics

    def _backoff(self, attempt: int) -> float:
        return min(BACKOFF_CAP_S, BACKOFF_BASE_S * 2 ** attempt) * random.uniform(0.75, 1.25)

    async def handle_async_request(self, request: httpx.Request) -> httpx.Response:
        attempt = 0
        while True:
            try:
                resp = await self.inner.handle_async_request(request)
            except (httpx.ConnectError, httpx.ConnectTimeout, httpx.ReadTimeout, httpx.RemoteProtocolError):
                if attempt >= self.max_retries:
                    raise
                await self.sleep(self._backoff(attempt))
                attempt += 1
                self.retries_done += 1
                continue

            if attempt < self.max_retries and resp.status_code in RETRY_STATUSES:
                wait = retry_after_seconds(resp) or self._backoff(attempt)
            elif attempt == 0 and resp.status_code == 429:
                wait = retry_after_seconds(resp)
                if wait is None or wait > MAX_RETRY_AFTER_S:
                    return resp  # don't hammer: the caller marks this provider rate-limited
            else:
                return resp
            await resp.aclose()
            await self.sleep(min(wait, BACKOFF_CAP_S if resp.status_code != 429 else MAX_RETRY_AFTER_S))
            attempt += 1
            self.retries_done += 1

    async def aclose(self) -> None:
        await self.inner.aclose()


def make_client(timeout: float = 25.0, **kw) -> httpx.AsyncClient:
    headers = {"User-Agent": USER_AGENT, **kw.pop("headers", {})}
    return httpx.AsyncClient(timeout=timeout, follow_redirects=True, headers=headers,
                             transport=PoliteTransport(), **kw)


async def pace() -> None:
    """Randomised spacing between consecutive calls to the same provider (250-450 ms)."""
    await asyncio.sleep(random.uniform(0.25, 0.45))
