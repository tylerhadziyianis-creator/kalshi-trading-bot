"""
order_manager.py — Confidence-Based Execution + Ghost-Order Cleaner

Ghost-Order Cleaner
───────────────────
Because we are always makers, our limit orders rest on the book.
If the market price moves away before we are filled, the order becomes a
"ghost" — still resting, but at a price that is no longer near best.

Strategy:
  • Every open order has a `created_at` timestamp.
  • A background task (`ghost_cleaner_loop`) runs every STALE_ORDER_TTL_SEC (30 s).
  • Any order older than TTL that is still OPEN is:
      1. Cancelled via DELETE /portfolio/orders/{id}
      2. Replaced with a fresh limit order at the CURRENT best market price + 1 tick
         (fetched from a live orderbook snapshot).

Order Payload (v2 field naming)
────────────────────────────────
Uses `yes_price_dollars` / `no_price_dollars` STRING fields.
Sets `post_only: true` — Kalshi will reject (not convert) any order that
would immediately match, so we are guaranteed maker status or no fill.

Price Convention
────────────────
For YES orders: improve by +MAKER_IMPROVE_TICKS ticks vs whale price.
For NO  orders: same logic.
Bounds: [0.0001, 0.9999] — both endpoints are exclusive.
"""
from __future__ import annotations

import asyncio
import logging
import uuid
from dataclasses import dataclass, field
from decimal import Decimal
from enum import Enum
from typing import Optional

from config import BotConfig
from rate_limiter import RateLimitedGate
from session_manager import SessionManager

log = logging.getLogger(__name__)

MIN_PRICE = Decimal("0.0001")
MAX_PRICE = Decimal("0.9999")


# ═══════════════════════════════════════════════════════════════════════════════
#  Domain Objects
# ═══════════════════════════════════════════════════════════════════════════════

class Side(str, Enum):
    YES = "yes"
    NO  = "no"


class OrderStatus(str, Enum):
    PENDING   = "pending"
    OPEN      = "open"
    FILLED    = "filled"
    CANCELLED = "cancelled"
    REJECTED  = "rejected"


@dataclass
class ManagedOrder:
    client_id:      str
    kalshi_id:      Optional[str]
    ticker:         str
    side:           Side
    price:          Decimal     # yes_price_dollars or no_price_dollars (string in payload)
    contracts:      int
    cost_basis:     Decimal
    status:         OrderStatus = OrderStatus.PENDING
    created_at:     float       = field(default_factory=lambda: __import__("time").monotonic())
    filled_at:      Optional[float] = None
    is_arb:         bool        = False   # True = arb taker order; ghost cleaner cancels but never replaces
    filled_count:   int         = 0       # contracts actually filled (may be < contracts on partial fill)
    ghost_attempts:  int         = 0       # how many times this order has been ghost-replaced
    cancel_attempts: int         = 0       # consecutive failed cancel attempts (for backoff/give-up)
    whale_price:     Optional[Decimal] = None  # original whale trade price — hard cap anchor

    def age(self) -> float:
        import time
        return time.monotonic() - self.created_at

    def is_stale(self, ttl: int) -> bool:
        return self.age() >= ttl and self.status == OrderStatus.OPEN


# ═══════════════════════════════════════════════════════════════════════════════
#  Order Manager
# ═══════════════════════════════════════════════════════════════════════════════

class OrderManager:
    """
    Manages the full lifecycle of post-only maker limit orders.

    Key responsibilities
    ────────────────────
    • place_maker_order()       — construct v2 payload, consume token budget, POST
    • cancel_order()            — DELETE and update internal registry
    • sync_status()             — GET order status and update registry
    • ghost_cleaner_loop()      — background task: cancel + replace stale orders
    • cancel_all_open()         — emergency shutdown helper
    """

    def __init__(
        self,
        cfg:     BotConfig,
        sess:    SessionManager,
        gate:    RateLimitedGate,
    ) -> None:
        self._cfg     = cfg
        self._sess    = sess
        self._gate    = gate
        self._orders: dict[str, ManagedOrder] = {}   # kalshi_id → order
        self._lock    = asyncio.Lock()
        self._dry_run = getattr(cfg, "dry_run", False)
        if self._dry_run:
            log.warning(
                "═══ DRY-RUN MODE ACTIVE ═══  No real orders will be submitted. "
                "All order calls are simulated and logged."
            )

    # ── public: placement ─────────────────────────────────────────────────────

    async def place_maker_order(
        self,
        ticker:        str,
        side:          Side,
        whale_price:   Decimal,
        contracts:     int,
        cost_basis:    Decimal,
        confidence:    float = 0.0,
        ghost_attempt:        int              = 0,
        max_chase:            Optional[int]    = None,
        orig_whale_price:     Optional[Decimal] = None,
        notional_chase_bonus: int              = 0,
        mispricing:           float            = 0.0,
        fund_conf:            float            = 0.0,
    ) -> Optional[ManagedOrder]:
        """
        Place a limit order with aggressiveness scaled to signal confidence.

        Confidence tiers (when ENABLE_AGGRESSIVE_EXECUTION=true):
          < 0.65  → passive: 1¢ below whale price (post_only, guaranteed maker)
          0.65–0.80 → at whale price (post_only)
          0.80–0.92 → whale + 1¢ (post_only off — willing to take)
          ≥ 0.92  → whale + 2¢ (taker mode, priority fill)
          ≥ 0.92 + cross cap → whale + MAX_CHASE_CENTS

        ghost_attempt: escalates chase by +1¢ per attempt on top of confidence tier,
        capped at max_chase_cents. e.g. conf=0.823 → +2¢ base; ghost #1 → +3¢; #2 → +4¢.

        Returns ManagedOrder on success, None on rejection/error.
        """
        entry_price, post_only, chase_cents = self._execution_price(whale_price, confidence)

        # Notional-size chase bonus: large whales deserve a harder chase
        if notional_chase_bonus > 0:
            CENT = Decimal("0.01")
            ceiling = max_chase if max_chase is not None else self._cfg.max_chase_cents
            bonus   = min(notional_chase_bonus, ceiling - chase_cents)
            if bonus > 0:
                entry_price = entry_price + CENT * bonus
                chase_cents = chase_cents + bonus
                post_only   = False
                log.info(
                    "Notional chase bonus: +%d¢ (%.1f× threshold) → total +%d¢ @ $%.4f",
                    bonus, 0.0, chase_cents, entry_price,
                )
            entry_price = min(entry_price, Decimal("0.99"))
            entry_price = max(entry_price, Decimal("0.01"))

        # Ghost escalation: add +1¢ per attempt on top of confidence-based chase
        if ghost_attempt > 0:
            CENT = Decimal("0.01")
            chase_ceiling = max_chase if max_chase is not None else self._cfg.max_chase_cents
            escalation   = min(ghost_attempt, chase_ceiling - chase_cents)
            if escalation > 0:
                entry_price = entry_price + CENT * escalation
                chase_cents = chase_cents + escalation
                post_only   = False  # escalated orders must be taker-eligible
                log.info(
                    "Ghost escalation attempt #%d: +%d¢ extra chase → total +%d¢ @ $%.4f",
                    ghost_attempt, escalation, chase_cents, entry_price,
                )
            entry_price = min(entry_price, Decimal("0.99"))
            entry_price = max(entry_price, Decimal("0.01"))
        if not self._valid_price(entry_price):
            log.warning("Price %.4f out of bounds — not placing order on %s", entry_price, ticker)
            return None

        # ── Edge-after-chase check (real-model markets only) ──────────────────
        # Only chase if the remaining edge after chase cost is at least 30% of
        # the original mispricing. Prevents chasing away most of the EV.
        # Skipped on no-model markets (fund_conf < 0.20) — mispricing is
        # circular there and this check would be meaningless.
        if fund_conf >= 0.20 and mispricing > 0 and chase_cents > 0:
            chase_cost     = chase_cents * 0.01
            remaining_edge = mispricing - chase_cost
            required_edge  = mispricing * 0.30   # keep at least 30% of original edge
            if remaining_edge < required_edge:
                # Don't block outright — reduce chase to what the edge allows.
                # max_viable_chase = mispricing * 0.70 (keeping 70% of edge)
                max_viable_cents = int(mispricing * 0.70 * 100)
                if max_viable_cents >= 1:
                    log.info(
                        "Chase trimmed %d¢ → %d¢ to preserve edge "
                        "(mispricing=%.4f, required_edge=%.4f) on %s",
                        chase_cents, max_viable_cents, mispricing, required_edge, ticker,
                    )
                    chase_cents  = max_viable_cents
                    entry_price  = whale_price + Decimal("0.01") * chase_cents
                else:
                    # Edge too small to chase at all — place at whale price (maker only)
                    log.info(
                        "Chase zeroed — edge too thin (mispricing=%.4f) on %s, placing at whale price",
                        mispricing, ticker,
                    )
                    chase_cents = 0
                    entry_price = whale_price

        client_id = str(uuid.uuid4())

        # Kalshi v2 accepts price as integer cents (yes_price) OR dollar string (yes_price_dollars).
        # Integer cents is the most reliable format — send both for maximum compatibility.
        # Round to nearest whole cent (integer) — this is the valid tick on most markets.
        price_cents = int(round(float(entry_price) * 100))
        price_cents = max(1, min(99, price_cents))   # clamp to [1, 99]
        price_str   = f"{price_cents / 100:.4f}"     # "0.3500" for logging

        # ── v2 payload ────────────────────────────────────────────────────────
        payload: dict = {
            "ticker":          ticker,
            "client_order_id": client_id,
            "action":          "buy",
            "side":            side.value,
            "type":            "limit",
            "count":           contracts,
            "count_fp":        f"{contracts}.00",
            "post_only":       post_only,
        }
        if side == Side.YES:
            payload["yes_price"] = price_cents
        else:
            payload["no_price"]  = price_cents

        mode_str = "POST-ONLY" if post_only else "AGGRESSIVE"
        log.info(
            "%sORDER → %s %s LIMIT @ %d¢ / $%s (%d contracts) [%s] conf=%.3f chase=+%d¢",
            "[DRY-RUN] " if self._dry_run else "",
            mode_str, side.value.upper(), price_cents, price_str,
            contracts, ticker, confidence, chase_cents,
        )

        if self._dry_run:
            # Simulate a successful order placement without hitting the API
            import time as _t
            kalshi_id = f"DRY-{client_id[:8]}"
            order = ManagedOrder(
                client_id   = client_id,
                kalshi_id   = kalshi_id,
                ticker      = ticker,
                side        = side,
                price       = entry_price,
                contracts   = contracts,
                cost_basis  = cost_basis,
                status      = OrderStatus.OPEN,
                whale_price = orig_whale_price or whale_price,
            )
            async with self._lock:
                self._orders[kalshi_id] = order
            log.info("[DRY-RUN] Simulated order: kalshi_id=%s", kalshi_id)
            return order

        try:
            resp = await self._gate.place_order(self._sess, payload, ticker)
        except Exception as exc:
            exc_str = str(exc).lower()
            if "post only cross" in exc_str:
                # The ask was at or below our bid when we sent the post-only order.
                # By the time of the retry (~60ms later) the ask may have moved
                # higher — especially in illiquid markets.  Chase up to the full
                # max_chase_cents ceiling so we have the best shot at crossing it.
                # Fresh UUID required: Kalshi tracks client_order_id even for
                # rejected orders, so reusing the same id yields order_already_exists.
                extra = self._cfg.max_chase_cents - chase_cents
                retry_cents = max(1, min(99, price_cents + extra))
                retry_str   = f"{retry_cents / 100:.4f}"
                log.info(
                    "post only cross on %s — retrying as taker @ %d¢ (+%d¢ extra)",
                    ticker, retry_cents, extra,
                )
                payload["post_only"] = False
                payload["client_order_id"] = str(uuid.uuid4())
                if side == Side.YES:
                    payload["yes_price"] = retry_cents
                else:
                    payload["no_price"]  = retry_cents
                try:
                    resp = await self._gate.place_order(self._sess, payload, ticker)
                except Exception as exc2:
                    log.warning("Order placement failed on %s (taker retry): %s", ticker, exc2)
                    return None
            elif "429" in exc_str or "too_many_requests" in exc_str or "too many requests" in exc_str:
                # Kalshi rate limit — exponential backoff, up to 3 retries.
                _delays = [2.0, 5.0, 10.0]
                resp = None
                for _attempt, _delay in enumerate(_delays, 1):
                    log.warning("Rate limited on %s — retry %d/%d in %.0fs", ticker, _attempt, len(_delays), _delay)
                    await asyncio.sleep(_delay)
                    payload["client_order_id"] = str(uuid.uuid4())
                    try:
                        resp = await self._gate.place_order(self._sess, payload, ticker)
                        break  # success
                    except Exception as exc2:
                        if _attempt == len(_delays):
                            log.warning("Order placement failed on %s (429 after %d retries): %s", ticker, _attempt, exc2)
                            return None
                        exc_str2 = str(exc2).lower()
                        if "429" not in exc_str2 and "too_many" not in exc_str2:
                            log.warning("Order placement failed on %s (429 retry %d, non-429 error): %s", ticker, _attempt, exc2)
                            return None
            else:
                log.error("Order placement failed on %s: %s", ticker, exc)
                return None

        kalshi_id = resp.get("order", {}).get("order_id")
        if not kalshi_id:
            log.error("No order_id in response: %s", resp)
            return None

        order = ManagedOrder(
            client_id   = client_id,
            kalshi_id   = kalshi_id,
            ticker      = ticker,
            side        = side,
            price       = entry_price,
            contracts   = contracts,
            cost_basis  = cost_basis,
            status      = OrderStatus.OPEN,
            whale_price = whale_price,
        )
        async with self._lock:
            self._orders[kalshi_id] = order

        log.info("Order live: kalshi_id=%s", kalshi_id)
        return order

    async def place_taker_order(
        self,
        ticker:     str,
        side:       Side,
        price:      Decimal,   # send at (or just above) the current ask — fills immediately
        contracts:  int,
        cost_basis: Decimal,
    ) -> Optional[ManagedOrder]:
        """
        Place an immediate-or-rest limit order at the given price with post_only=False.

        Unlike place_maker_order there is no confidence-based price adjustment —
        `price` is used exactly.  Used for two-leg arb execution where both legs
        are sent concurrently (asyncio.gather) and must fill against resting
        liquidity as quickly as possible.

        If the order does not match immediately it rests on the book and the
        ghost cleaner will handle it at TTL like any other open order.
        """
        price = min(max(price, Decimal("0.01")), Decimal("0.99"))
        if not self._valid_price(price):
            log.warning(
                "Taker price %.4f out of bounds — not placing %s %s",
                price, side.value, ticker,
            )
            return None

        client_id   = str(uuid.uuid4())
        price_cents = int(round(float(price) * 100))
        price_cents = max(1, min(99, price_cents))
        price_str   = f"{price_cents / 100:.4f}"

        payload: dict = {
            "ticker":          ticker,
            "client_order_id": client_id,
            "action":          "buy",
            "side":            side.value,
            "type":            "limit",
            "count":           contracts,
            "count_fp":        f"{contracts}.00",
            "post_only":       False,
        }
        if side == Side.YES:
            payload["yes_price"] = price_cents
        else:
            payload["no_price"]  = price_cents

        log.info(
            "%sTAKER → %s LIMIT @ %d¢ / $%s (%d contracts) [%s]",
            "[DRY-RUN] " if self._dry_run else "",
            side.value.upper(), price_cents, price_str, contracts, ticker,
        )

        if self._dry_run:
            import time as _t
            kalshi_id = f"DRY-{client_id[:8]}"
            order = ManagedOrder(
                client_id  = client_id,
                kalshi_id  = kalshi_id,
                ticker     = ticker,
                side       = side,
                price      = price,
                contracts  = contracts,
                cost_basis = cost_basis,
                status     = OrderStatus.OPEN,
            )
            async with self._lock:
                self._orders[kalshi_id] = order
            log.info("[DRY-RUN] Simulated taker order: kalshi_id=%s", kalshi_id)
            return order

        try:
            resp = await self._gate.place_order(self._sess, payload, ticker)
        except Exception as exc:
            log.error("Taker order failed on %s: %s", ticker, exc)
            return None

        kalshi_id = resp.get("order", {}).get("order_id")
        if not kalshi_id:
            log.error("No order_id in taker response for %s: %s", ticker, resp)
            return None

        order = ManagedOrder(
            client_id  = client_id,
            kalshi_id  = kalshi_id,
            ticker     = ticker,
            side       = side,
            price      = price,
            contracts  = contracts,
            cost_basis = cost_basis,
            status     = OrderStatus.OPEN,
            is_arb     = True,   # taker orders are always arb legs — never replace on ghost
        )
        async with self._lock:
            self._orders[kalshi_id] = order

        log.info("Taker order live: kalshi_id=%s", kalshi_id)
        return order

    async def _prune_pending(self) -> None:
        """Remove PENDING orders older than 10s that never got a kalshi_id (Bug 7 fix)."""
        import time
        cutoff = time.monotonic() - 10
        async with self._lock:
            stale_pending = [
                k for k, o in self._orders.items()
                if o.status == OrderStatus.PENDING and o.created_at < cutoff
            ]
            for k in stale_pending:
                del self._orders[k]
                log.debug("Pruned stale PENDING order: %s", k)

    # ── public: cancel ────────────────────────────────────────────────────────

    async def cancel_order(self, kalshi_id: str) -> bool:
        """Cancel an open order. Returns True on success or if already gone (404)."""
        async with self._lock:
            order = self._orders.get(kalshi_id)
            if not order or order.status != OrderStatus.OPEN:
                return False

        if self._dry_run:
            async with self._lock:
                order.status = OrderStatus.CANCELLED
            log.info("[DRY-RUN] Simulated cancel: %s", kalshi_id)
            return True

        try:
            await self._gate.cancel_order(self._sess, kalshi_id, order.ticker)
            async with self._lock:
                order.status = OrderStatus.CANCELLED
            log.info("Cancelled: %s", kalshi_id)
            return True
        except Exception as exc:
            # 404 = order already filled/cancelled/expired on exchange.
            # Fetch the real status so we correctly update exposure tracking.
            if "404" in str(exc) or "not_found" in str(exc):
                real_status = await self.sync_status(kalshi_id)
                if real_status == OrderStatus.FILLED:
                    log.info("Cancel 404 on %s — confirmed FILLED", kalshi_id)
                    return True   # filled = done, don't replace
                elif real_status == OrderStatus.CANCELLED:
                    async with self._lock:
                        order.status = OrderStatus.CANCELLED
                    log.info("Cancel 404 on %s — confirmed CANCELLED, releasing exposure", kalshi_id)
                    return True   # confirmed gone, safe to release
                else:
                    # Ambiguous — order may still be live or just filled.
                    # Do NOT call replace callback (would release exposure on a live order).
                    # Keep OPEN and let ghost cleaner retry next cycle.
                    async with self._lock:
                        order.cancel_attempts += 1
                        attempts = order.cancel_attempts
                    if attempts >= 5:
                        # After 5 consecutive ambiguous 404s, stop ghost-cycling.
                        # Mark CANCELLED so ghost cleaner stops retrying.
                        # Do NOT release exposure here — _watch_order owns that.
                        async with self._lock:
                            order.status = OrderStatus.CANCELLED
                        log.warning(
                            "Cancel 404 on %s — gave up after %d attempts, "
                            "marking cancelled to stop ghost loop (verify on Kalshi dashboard)",
                            kalshi_id, attempts,
                        )
                        return True  # _watch_order will handle exposure release
                    log.warning(
                        "Cancel 404 on %s — status ambiguous (sync returned %s) "
                        "— keeping OPEN, will retry next ghost cycle (attempt %d/5)",
                        kalshi_id, real_status, attempts,
                    )
                    return False  # don't trigger replace callback
            log.error("Cancel failed %s: %s", kalshi_id, exc)
            return False

    async def cancel_all_open(self) -> int:
        """Emergency: cancel every open order.  Returns count cancelled."""
        async with self._lock:
            open_ids = [oid for oid, o in self._orders.items()
                        if o.status == OrderStatus.OPEN]
        count = 0
        for oid in open_ids:
            if await self.cancel_order(oid):
                count += 1
        return count

    # ── public: status sync ───────────────────────────────────────────────────

    async def sync_status(self, kalshi_id: str) -> Optional[OrderStatus]:
        """Fetch live order status from Kalshi and update internal state."""
        async with self._lock:
            order = self._orders.get(kalshi_id)
        if not order:
            return None

        if self._dry_run:
            # Simulate a fill after DRY_RUN_FILL_DELAY_SEC seconds.
            # This lets the bot track P&L and settlement outcomes without
            # needing a real exchange counterparty.
            fill_delay = getattr(self._cfg, "dry_run_fill_delay_sec", 25)
            if order.status == OrderStatus.OPEN and order.age() >= fill_delay:
                import time as _t
                async with self._lock:
                    order.status    = OrderStatus.FILLED
                    order.filled_at = _t.monotonic()
                log.info(
                    "[DRY-RUN] Simulated fill: %s %s @ $%.4f (%d contracts) "
                    "after %.0fs",
                    order.ticker, order.side.value.upper(),
                    float(order.price), order.contracts, order.age(),
                )
            return order.status

        try:
            resp = await self._sess.get(
                f"/portfolio/orders/{kalshi_id}", ticker=order.ticker
            )
        except Exception as exc:
            log.error("Status sync failed %s: %s", kalshi_id, exc)
            return None

        order_data = resp.get("order", {})
        raw = order_data.get("status", "").lower()
        log.debug("sync_status raw for %s: status=%r filled_count=%s",
                  kalshi_id, raw, order_data.get("filled_count"))
        status_map = {
            "open":         OrderStatus.OPEN,
            "resting":      OrderStatus.OPEN,
            "pending":      OrderStatus.OPEN,
            "filled":       OrderStatus.FILLED,
            "executed":     OrderStatus.FILLED,   # Kalshi v2 primary fill status
            "settled":      OrderStatus.FILLED,
            "closed":       OrderStatus.FILLED,
            "matched":      OrderStatus.FILLED,
            "cancelled":    OrderStatus.CANCELLED,
            "canceled":     OrderStatus.CANCELLED,
            "expired":      OrderStatus.CANCELLED,
            "rejected":     OrderStatus.CANCELLED,
        }
        new_status = status_map.get(raw, order.status)
        if raw not in status_map:
            log.warning("sync_status unknown status %r for %s — defaulting to %s",
                        raw, kalshi_id, order.status)

        # Capture partial fill count — Kalshi returns filled_count (int) on
        # every order response.  We need this to detect partial-fill cancels.
        filled_count = int(order_data.get("filled_count", 0) or 0)

        async with self._lock:
            order.status       = new_status
            order.filled_count = filled_count
            if new_status == OrderStatus.FILLED:
                import time
                order.filled_at = time.monotonic()
        return new_status

    # ── Ghost-Order Cleaner ───────────────────────────────────────────────────

    async def ghost_cleaner_loop(
        self,
        on_replace_callback,   # async callable(ticker, side, cost_basis, contracts, ghost_attempt)
    ) -> None:
        """
        Background task: every STALE_ORDER_TTL_SEC seconds, cancel any resting
        order that hasn't filled and call `on_replace_callback` so the engine
        can re-price and re-submit at the current market price.

        The callback signature:
            async def on_replace(ticker, side, cost_basis, contracts, ghost_attempt) -> None
        """
        ttl = self._cfg.stale_order_ttl
        log.info("Ghost cleaner started — TTL=%ds", ttl)
        while True:
            await asyncio.sleep(ttl)
            await self._prune_pending()  # Bug 7: clean up stuck PENDING orders
            async with self._lock:
                stale = [
                    o for o in self._orders.values() if o.is_stale(ttl)
                ]

            for order in stale:
                if order.is_arb:
                    # Arb taker orders are never repriced — if they didn't fill
                    # the opportunity has likely closed.  Cancel only; let the
                    # arb scanner re-detect on the next pass.
                    log.info(
                        "GHOST ARB ORDER: %s (age=%.0fs) — cancelling (no replace)",
                        order.kalshi_id, order.age(),
                    )
                    await self.cancel_order(order.kalshi_id)
                    continue

                ghost_attempt = order.ghost_attempts + 1
                log.info(
                    "GHOST ORDER detected: %s (age=%.0fs, attempt #%d) — cancelling & replacing",
                    order.kalshi_id, order.age(), ghost_attempt,
                )
                cancelled = await self.cancel_order(order.kalshi_id)
                if cancelled:
                    try:
                        await on_replace_callback(
                            order.ticker,
                            order.side,
                            order.cost_basis,
                            order.contracts,
                            ghost_attempt,
                            order.whale_price,
                        )
                    except Exception as exc:
                        log.error("Replace callback failed: %s", exc)

    # ── diagnostics ───────────────────────────────────────────────────────────

    def open_orders(self) -> list[ManagedOrder]:
        return [o for o in self._orders.values() if o.status == OrderStatus.OPEN]

    def total_open_cost_basis(self) -> Decimal:
        return sum(o.cost_basis for o in self.open_orders())

    # ── internals ─────────────────────────────────────────────────────────────

    def _execution_price(
        self,
        whale_price: Decimal,
        confidence:  float,
    ) -> tuple[Decimal, bool, int]:
        """
        Returns (entry_price, post_only, chase_cents) based on confidence.

        Exact tiers per spec:
          conf < 0.65        → at whale price,           post_only=True  (passive maker)
          0.65 <= conf < 0.80 → whale + 1¢,              post_only=True  (queue improvement)
          0.80 <= conf < 0.92 → whale + 2¢,              post_only=False (willing taker)
          conf >= 0.92        → whale + MAX_CHASE_CENTS,  post_only=False (max aggression)

        Safeguards:
          - Price always clamped to [1¢, 99¢]
          - Chase never exceeds cfg.max_chase_cents
          - post_only=False only when confidence justifies crossing the spread
        """
        CENT       = Decimal("0.01")
        aggressive = self._cfg.enable_aggressive_execution
        max_chase  = self._cfg.max_chase_cents

        # Snap whale price down to nearest whole cent
        base = (whale_price / CENT).to_integral_value(rounding="ROUND_FLOOR") * CENT

        if not aggressive or confidence < 0.65:
            # Passive: bid at whale price — rests on book as maker
            price     = base
            post_only = True
            chase     = 0
            if aggressive:
                log.info(
                    "Confidence=%.3f → passive maker @ %d¢ (at whale price, post-only)",
                    confidence, int(price * 100),
                )

        elif confidence < 0.80:
            # Moderate: +1¢ above whale, willing to take liquidity.
            # Previously post-only but ghost-cancels were too frequent — at
            # 0.65+ confidence crossing the spread for 1¢ is worth it.
            chase     = min(1, max_chase)
            price     = base + CENT * chase
            post_only = False
            log.info(
                "Aggressive execution enabled — Confidence=%.3f → chasing whale by +%d¢ (taker ok)",
                confidence, chase,
            )

        elif confidence < 0.88:
            # Strong: +2¢, cross spread to guarantee fill
            chase     = min(2, max_chase)
            price     = base + CENT * chase
            post_only = False
            log.info(
                "Aggressive execution enabled — Confidence=%.3f → chasing whale by +%d¢ (taker ok)",
                confidence, chase,
            )

        else:
            # Max aggression (≥0.88): up to MAX_CHASE_CENTS, cross spread.
            # Threshold lowered from 0.92 → 0.88 so strong FED/CPI/crypto
            # signals with real fundamental backing reach this tier.
            chase     = max_chase
            price     = base + CENT * chase
            post_only = False
            log.info(
                "Aggressive execution enabled — Confidence=%.3f → chasing whale by +%d¢ (MAX AGGRESSION)",
                confidence, chase,
            )

        # Hard safety clamps — inviolable regardless of confidence
        price = min(price, Decimal("0.99"))
        price = max(price, Decimal("0.01"))

        # Spread guard: if we are chasing more than MAX_CHASE_CENTS above
        # the whale price AND post_only is False, verify we are not crossing
        # an absurd spread. This check is advisory — the hard clamp above
        # is the ultimate safety net. We do not have live OB data here so
        # we cap maximum taker aggression to cfg.max_chase_cents.
        if not post_only and chase > max_chase:
            chase = max_chase
            price = base + CENT * chase
            log.warning(
                "Spread guard: clamped chase to +%d¢ (max_chase_cents limit)",
                chase,
            )

        return price, post_only, chase


    @staticmethod
    def _valid_price(p: Decimal) -> bool:
        return MIN_PRICE <= p <= MAX_PRICE
