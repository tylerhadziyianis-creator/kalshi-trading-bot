"""
ws_feed.py — Kalshi v2 WebSocket Feed with Strict Whale Filter

THE CRITICAL FILTER
───────────────────
The user_orders channel is one of the highest-volume streams on Kalshi.
Thousands of small trades per minute are published.  We MUST NOT react
to noise.  The `min_notional_usd` filter (default $5,000) discards any
order whose dollar notional (price × count) falls below the threshold.

This is enforced at parse time — below-threshold events are dropped before
they ever reach the event queue.

v2 Field Semantics
──────────────────
• `yes_price_dollars`  — string, e.g. "0.6200"  ← USE THIS
• `no_price_dollars`   — string, e.g. "0.3800"  ← USE THIS
• `count`              — integer contracts
• `count_fp`           — string display count (informational)
• Deprecated integer cent fields (`yes_price`, `no_price`) are IGNORED.

Channels subscribed
───────────────────
• trades           — public trade flow (whale detection) — THIS is the correct channel
• orderbook_delta  — top-of-book price updates (used for ghost-order repricing)

NOTE: 'user_orders' is a private channel that only shows YOUR OWN orders.
      'trades' is the public channel that shows all market trades, which is
      what we need to detect large whale activity.

Reconnection
────────────
Exponential back-off: 2 s → 4 s → 8 s → … → 30 s ceiling.
On reconnect, re-sends subscription message for all watched tickers.
"""
from __future__ import annotations

import asyncio
import json
import logging
import time
from dataclasses import dataclass
from decimal import Decimal, InvalidOperation
from typing import Optional

import aiohttp

from config import BotConfig
from session_manager import SessionManager

log = logging.getLogger(__name__)


# ═══════════════════════════════════════════════════════════════════════════════
#  Event Types
# ═══════════════════════════════════════════════════════════════════════════════

@dataclass(slots=True)
class WhaleEvent:
    whale_id:      str
    ticker:        str
    side:          str          # "yes" | "no"
    price_dollars: Decimal      # v2 *_price_dollars field
    notional_usd:  Decimal      # price × contracts
    contracts:     int
    ts:            float        # time.time()
    raw:           dict
    track_only:    bool = False  # True = track win/loss only, do not trade


@dataclass(slots=True)
class OrderbookSnapshot:
    ticker:       str
    best_yes_bid: Optional[Decimal]   # yes_price_dollars
    best_no_bid:  Optional[Decimal]   # no_price_dollars
    ts:           float


# ═══════════════════════════════════════════════════════════════════════════════
#  WebSocket Feed
# ═══════════════════════════════════════════════════════════════════════════════

class WebSocketFeed:
    """
    Persistent, auto-reconnecting WebSocket connection to Kalshi v2.

    Usage:
        feed = WebSocketFeed(cfg, session)
        await feed.subscribe(["KXBTCD-25DEC-T100000", "KXINX-25DEC-T5000"])
        async for event in feed.events():
            if isinstance(event, WhaleEvent):
                ...
    """

    def __init__(self, cfg: BotConfig, session: SessionManager, market_selector=None) -> None:
        self._cfg      = cfg
        self._session  = session
        self._selector = market_selector   # optional: enables dynamic thresholds
        self._queue:  asyncio.Queue = asyncio.Queue(maxsize=2000)
        self._tickers: list[str]   = []
        # Tickers added by ensure_ticker_subscribed() for arb-pair coverage.
        # Kept separate from _tickers so market-selector refreshes don't evict them.
        self._extra_tickers: set[str] = set()
        self._running  = False
        self._task:    Optional[asyncio.Task] = None
        self._seq      = 0              # monotonic message sequence ID

    # ── lifecycle ─────────────────────────────────────────────────────────────

    async def subscribe(self, tickers: list[str]) -> None:
        """Start (or add to) the WebSocket subscription."""
        self._tickers = list(tickers)
        if not self._running:
            self._running = True
            self._task    = asyncio.create_task(self._run_forever(), name="ws-feed")

    async def stop(self) -> None:
        self._running = False
        if self._task:
            self._task.cancel()

    async def resubscribe(self, tickers: list[str]) -> None:
        """
        Update the ticker list and trigger a WS reconnect so the new
        subscription set takes effect immediately.
        Called by MarketSelector after a market refresh.
        """
        added   = [t for t in tickers if t not in self._tickers]
        removed = [t for t in self._tickers if t not in tickers]
        self._tickers = list(tickers)
        log.info(
            "WS resubscribe: +%d added, -%d removed (total=%d)",
            len(added), len(removed), len(tickers),
        )
        if not self._running:
            return
        # Cancel and restart the WS task — _run_forever will reconnect
        # with the updated self._tickers on its next iteration
        if self._task:
            self._task.cancel()
        self._task = asyncio.create_task(self._run_forever(), name="ws-feed")

    async def ensure_ticker_subscribed(self, ticker: str) -> None:
        """
        Add `ticker` to the persistent extra-ticker set and reconnect the WS
        so it starts receiving orderbook_delta events for that market.

        Called by bot_engine when the arb liquidity gate fails because a hedge
        leg isn't in the current subscription.  Once subscribed, the WS-
        triggered arb detection will fire the moment a price update arrives.

        Safe to call repeatedly — no-op if already subscribed.
        """
        if ticker in self._tickers or ticker in self._extra_tickers:
            return   # already subscribed
        self._extra_tickers.add(ticker)
        log.info("WS: adding arb-pair ticker %s to subscription — reconnecting", ticker)
        if not self._running:
            return
        if self._task:
            self._task.cancel()
        self._task = asyncio.create_task(self._run_forever(), name="ws-feed")

    # ── event generator ───────────────────────────────────────────────────────

    async def events(self):
        """Async generator — yields WhaleEvent and OrderbookSnapshot objects."""
        while self._running:
            try:
                event = await asyncio.wait_for(self._queue.get(), timeout=1.0)
                yield event
            except asyncio.TimeoutError:
                continue

    # ── connection management ─────────────────────────────────────────────────

    async def _run_forever(self) -> None:
        delay = self._cfg.ws_reconnect_base
        while self._running:
            try:
                await self._connect()
                delay = self._cfg.ws_reconnect_base     # reset on clean exit
            except asyncio.CancelledError:
                return
            except Exception as exc:
                log.error("WS error: %s — reconnecting in %.1f s", exc, delay)
                await asyncio.sleep(delay)
                delay = min(self._cfg.ws_reconnect_max, delay * 2.0)

    async def _connect(self) -> None:
        headers = await self._session.get_headers("GET", "/trade-api/ws/v2")
        log.info("Connecting WS → %s", self._cfg.ws_base)

        connector = aiohttp.TCPConnector(ssl=True)
        async with aiohttp.ClientSession(connector=connector) as tmp:
            async with tmp.ws_connect(
                self._cfg.ws_base,
                headers      = headers,
                heartbeat    = self._cfg.ws_ping_interval,
                max_msg_size = 8 * 1024 * 1024,
            ) as ws:
                log.info("WS connected. Subscribing to %d tickers.", len(self._tickers))
                await self._send_subscribe(ws)

                try:
                    async for msg in ws:
                        if not self._running:
                            break
                        if msg.type == aiohttp.WSMsgType.TEXT:
                            await self._handle(msg.data)
                        elif msg.type in (aiohttp.WSMsgType.ERROR,
                                          aiohttp.WSMsgType.CLOSED):
                            log.warning("WS closed/error: %s", msg.type)
                            break
                finally:
                    pass

    async def _send_subscribe(self, ws) -> None:
        # The Kalshi WS API requires one subscribe command per channel.
        # Correct channel names (from official AsyncAPI spec):
        #   "trade"           — public trade flow (whale detection)
        #   "orderbook_delta" — top-of-book price updates (ghost repricing)
        #
        # Merge market-selector tickers with any extra arb-pair tickers added
        # via ensure_ticker_subscribed().
        all_tickers = list(dict.fromkeys(self._tickers + list(self._extra_tickers)))

        # Subscribe to trade channel
        self._seq += 1
        await ws.send_str(json.dumps({
            "id":  self._seq,
            "cmd": "subscribe",
            "params": {
                "channels":       ["trade"],
                "market_tickers": all_tickers,
            },
        }))

        # Subscribe to orderbook_delta channel
        self._seq += 1
        await ws.send_str(json.dumps({
            "id":  self._seq,
            "cmd": "subscribe",
            "params": {
                "channels":       ["orderbook_delta"],
                "market_tickers": all_tickers,
            },
        }))

    # ── message parsing ───────────────────────────────────────────────────────

    async def _handle(self, raw_text: str) -> None:
        try:
            msg = json.loads(raw_text)
        except json.JSONDecodeError:
            return

        msg_type = msg.get("type", "")
        if msg_type == "trade":
            await self._parse_orders(msg)
        elif msg_type == "orderbook_delta":
            await self._parse_orderbook(msg)
        elif msg_type in ("subscribed", "ok"):
            log.info("WS subscription confirmed: %s", msg)
        elif msg_type == "error":
            log.error("WS server error: %s", msg)

    async def _parse_orders(self, msg: dict) -> None:
        """
        Parse a 'trade' channel event from Kalshi WS API.

        The 'trade' channel message structure:
          { "type": "trade", "sid": N, "seq": N, "msg": { <trade fields> } }

        !! CRITICAL FILTER !!
        Only whale trades (notional ≥ WHALE_MIN_NOTIONAL_USD) are enqueued.
        All other trades are silently discarded here.
        """
        # The actual trade data is nested under msg.msg (Kalshi WS envelope)
        trade_data = msg.get("msg", {})
        if not trade_data:
            return

        # Kalshi 'trade' channel emits one trade per message
        orders = [trade_data] if isinstance(trade_data, dict) else trade_data

        for order in orders:
            try:
                # Official Kalshi 'trade' WS message fields (from docs):
                # { "trade_id": str, "market_ticker": str,
                #   "yes_price_dollars": str, "no_price_dollars": str,
                #   "count_fp": str, "taker_side": "yes"|"no",
                #   "ts": int, "ts_ms": int }

                ticker = order.get("market_ticker", "")
                side   = order.get("taker_side", "yes")
                # count_fp is a fixed-point string like "136.00"
                count = int(float(order.get("count_fp", "0")))

                # Both prices are always present; pick by taker_side for notional calc
                if side == "yes":
                    price_raw = order.get("yes_price_dollars") or "0"
                else:
                    price_raw = order.get("no_price_dollars") or "0"

                price    = Decimal(str(price_raw))
                notional = price * count

                # ── THE WHALE FILTER ─────────────────────────────────────────
                # Two-tier threshold system:
                #   trade_threshold — dynamic (5% of 24h vol, $200–$5000)
                #   track_threshold — flat $200 minimum (cfg.whale_track_notional)
                #
                # notional >= trade_threshold → full signal + potential order
                # notional >= track_threshold → shadow tracking only (no order)
                # notional <  track_threshold → silently dropped
                if self._selector is not None:
                    trade_threshold = self._selector.get_threshold(ticker)
                else:
                    trade_threshold = self._cfg.whale_min_notional

                track_threshold = self._cfg.whale_track_notional

                if notional < track_threshold:
                    log.debug(
                        "Skipped: notional=$%.2f < track_threshold=$%.2f (%s)",
                        notional, track_threshold, ticker,
                    )
                    continue

                # Only emit events that clear the full trade threshold.
                # Sub-threshold trades have no member_id to track and add no signal.
                if notional < trade_threshold:
                    log.debug(
                        "Sub-threshold: notional=$%.2f < trade_threshold=$%.2f (%s)",
                        notional, trade_threshold, ticker,
                    )
                    continue
                # ─────────────────────────────────────────────────────────────

                whale_id = (
                    order.get("trade_id")
                    or order.get("member_id")
                    or "unknown"
                )

                event = WhaleEvent(
                    whale_id      = str(whale_id),
                    ticker        = ticker,
                    side          = side,
                    price_dollars = price,
                    notional_usd  = notional,
                    contracts     = count,
                    ts            = time.time(),
                    raw           = order,
                    track_only    = False,
                )
                await self._queue.put(event)
                vol24 = self._selector.get_vol24(ticker) if self._selector else None
                vol24_str = f"${vol24:,.0f}" if vol24 is not None else "unknown"
                log.info(
                    "WHALE: %s | %s @ $%s | notional=$%.2f | threshold=$%.2f | mkt_vol_24h=%s | %d contracts",
                    ticker, side.upper(), price_raw,
                    float(notional), float(trade_threshold), vol24_str, count,
                )

            except (InvalidOperation, ValueError, TypeError, KeyError) as exc:
                log.debug("Malformed order skipped: %s | %s", exc, order)

    async def _parse_orderbook(self, msg: dict) -> None:
        """Parse orderbook_delta for current best prices (ghost order repricing)."""
        try:
            # Kalshi WS envelope: { "type": "orderbook_delta", "sid": N, "msg": {...} }
            data   = msg.get("msg") or msg.get("data") or msg
            ticker = data.get("market_ticker", "")

            def _d(val) -> Optional[Decimal]:
                return Decimal(str(val)) if val is not None else None

            # orderbook_delta fields: yes_price_dollars, no_price_dollars (best bid)
            snap = OrderbookSnapshot(
                ticker       = ticker,
                best_yes_bid = _d(data.get("yes_price_dollars")),
                best_no_bid  = _d(data.get("no_price_dollars")),
                ts           = time.time(),
            )
            await self._queue.put(snap)
        except Exception as exc:
            log.debug("Orderbook parse error: %s", exc)
