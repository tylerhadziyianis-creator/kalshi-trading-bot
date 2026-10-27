"""
rate_limiter.py — Token-Bucket Rate Limiter + Request Cache

Token Bucket
────────────
Models Kalshi's 2026 per-operation token cost system.
Callers `await gate.consume("write_order")` — blocks until tokens are available,
so write operations are never silently dropped.

Request Cache
─────────────
Keyed by SHA-256(path + sorted params). Serves stale-safe reads for static
market metadata, freeing the expensive write_order token budget for orders.

RateLimitedGate
───────────────
Composed façade used by every HTTP call in the system.
"""
from __future__ import annotations

import asyncio
import hashlib
import json
import logging
import time
from typing import Any, Optional

from config import BotConfig
from session_manager import SessionManager

log = logging.getLogger(__name__)


# ═══════════════════════════════════════════════════════════════════════════════
#  Token Bucket
# ═══════════════════════════════════════════════════════════════════════════════

class TokenBucket:
    def __init__(self, capacity: int, refill_rate: float, costs: dict[str, int]) -> None:
        self._capacity    = float(capacity)
        self._tokens      = float(capacity)
        self._refill_rate = refill_rate         # tokens / second
        self._costs       = costs
        self._last        = time.monotonic()
        self._lock        = asyncio.Lock()

    async def consume(self, operation: str) -> None:
        cost = self._costs.get(operation)
        if cost is None:
            raise ValueError(f"Unknown operation: {operation!r}")
        if cost == 0:
            return

        async with self._lock:
            while True:
                self._refill()
                if self._tokens >= cost:
                    self._tokens -= cost
                    log.debug("Bucket: -%d for %-16s | remaining=%.1f",
                              cost, operation, self._tokens)
                    return
                # How long until we have enough?
                deficit   = cost - self._tokens
                wait      = deficit / self._refill_rate
                log.debug("Bucket throttle: need %.1f more, wait %.3f s", deficit, wait)
                # Release lock while sleeping so other coroutines can run
                self._lock.release()
                try:
                    await asyncio.sleep(wait)
                finally:
                    await self._lock.acquire()

    def _refill(self) -> None:
        now           = time.monotonic()
        self._tokens  = min(self._capacity, self._tokens + (now - self._last) * self._refill_rate)
        self._last    = now

    @property
    def available(self) -> float:
        self._refill()
        return self._tokens


# ═══════════════════════════════════════════════════════════════════════════════
#  TTL Cache
# ═══════════════════════════════════════════════════════════════════════════════

class _Entry:
    __slots__ = ("value", "expires_at")
    def __init__(self, value: Any, ttl: float) -> None:
        self.value      = value
        self.expires_at = time.monotonic() + ttl


class RequestCache:
    def __init__(self) -> None:
        self._store: dict[str, _Entry] = {}
        self._lock  = asyncio.Lock()

    def _key(self, path: str, params: Optional[dict]) -> str:
        raw = json.dumps({"p": path, "q": params or {}}, sort_keys=True)
        return hashlib.sha256(raw.encode()).hexdigest()[:24]

    async def get(self, path: str, params: Optional[dict] = None) -> Optional[Any]:
        k = self._key(path, params)
        async with self._lock:
            e = self._store.get(k)
            if e is None:
                return None
            if time.monotonic() > e.expires_at:
                del self._store[k]
                return None
            return e.value

    async def set(self, path: str, value: Any, ttl: float,
                  params: Optional[dict] = None) -> None:
        k = self._key(path, params)
        async with self._lock:
            self._store[k] = _Entry(value, ttl)

    async def purge(self) -> int:
        now = time.monotonic()
        async with self._lock:
            stale = [k for k, e in self._store.items() if now > e.expires_at]
            for k in stale:
                del self._store[k]
        return len(stale)


# ═══════════════════════════════════════════════════════════════════════════════
#  Composed Gate
# ═══════════════════════════════════════════════════════════════════════════════

class RateLimitedGate:
    """
    Single entry-point for all HTTP calls.
    • Reads go through cache → token bucket → network.
    • Writes (orders, cancels) skip cache → token bucket → network.
    """

    def __init__(self, cfg: BotConfig) -> None:
        self.bucket = TokenBucket(
            capacity    = cfg.token_bucket_capacity,
            refill_rate = cfg.token_refill_rate,
            costs       = cfg.token_costs,
        )
        self.cache  = RequestCache()
        self._cfg   = cfg

    # ── reads (cacheable) ─────────────────────────────────────────────────────

    async def cached_get(
        self,
        sess:      SessionManager,
        path:      str,
        *,
        operation: str            = "read_market",
        params:    Optional[dict] = None,
        ticker:    str            = "",
        ttl:       Optional[float] = None,
    ) -> dict:
        effective_ttl = ttl if ttl is not None else float(self._cfg.market_cache_ttl)
        cached = await self.cache.get(path, params)
        if cached is not None:
            log.debug("Cache HIT: %s", path)
            return cached

        await self.bucket.consume(operation)
        data = await sess.get(path, ticker=ticker, params=params)
        await self.cache.set(path, data, ttl=effective_ttl, params=params)
        return data

    # ── writes (never cached) ─────────────────────────────────────────────────

    async def place_order(self, sess: SessionManager, payload: dict, ticker: str) -> dict:
        await self.bucket.consume("write_order")
        return await sess.post("/portfolio/orders", ticker=ticker, payload=payload)

    async def cancel_order(self, sess: SessionManager, order_id: str, ticker: str) -> dict:
        await self.bucket.consume("cancel_order")
        return await sess.delete(f"/portfolio/orders/{order_id}", ticker=ticker)

    async def get_balance(self, sess: SessionManager) -> dict:
        # Balance changes after fills — short TTL
        return await self.cached_get(
            sess, "/portfolio/balance",
            operation="get_balance",
            ttl=float(self._cfg.balance_cache_ttl),
        )

    async def get_settlements(self, sess: SessionManager) -> dict:
        # Settlements are historical and don't change often, so a longer TTL is fine.
        # We will fetch all settlements and then filter/process them in the bot_engine.
        return await self.cached_get(
            sess, "/portfolio/settlements",
            operation="get_settlements",
            ttl=float(self._cfg.settlement_cache_ttl),
            params={"limit": 1000} # Fetch as many as possible
        )
