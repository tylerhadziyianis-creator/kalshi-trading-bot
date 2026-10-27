"""
bot_engine.py — Main Orchestrator: Liquidity-Capture HFT Engine

Event Loop
──────────
1.  WebSocketFeed yields WhaleEvent objects (already filtered to ≥ $5k notional).
2.  BotEngine passes each to WhaleRegistry.observe_trade() → MarketSignal.
3.  If signal.final_confidence ≥ MIN_KELLY_EDGE and Kelly gives a positive stake:
    a.  PositionSizer.compute_stake(signal.win_probability, signal.base_price)
    b.  OrderManager.place_maker_order(...)
4.  OrderbookSnapshot events update a live price cache used by the Ghost Cleaner.

Background Tasks
────────────────
• ghost_cleaner_loop   — every 30 s: cancel + re-price stale resting orders
• order_watcher_loop   — every 5 s: sync fill status for open orders
• decay_loop           — every 60 min: apply whale performance decay
• balance_sync_loop    — every 5 min: fetch live balance from Kalshi
• cache_purge_loop     — every 10 min: evict stale cache entries
"""
from __future__ import annotations

import asyncio
import logging
from decimal import Decimal
from typing import Optional

from config import BotConfig
from order_manager import ManagedOrder, OrderManager, OrderStatus, Side
from position_sizer import PositionSizer
from rate_limiter import RateLimitedGate
from session_manager import SessionManager
from whale_registry import TradeRecord, WhaleRegistry
from ws_feed import OrderbookSnapshot, WebSocketFeed, WhaleEvent
from market_selector import MarketSelector
from trade_logger import TradeLogger
from signal_engine import SignalEngine, CompositeSignal
from external_data import ExternalDataCache, set_cache, detect_category
from state_store import StateStore
from flow_tracker import FlowTracker
from ob_tracker import OBTracker
from strike_arb import StrikeArbScanner, ArbOpportunity, _MIN_EDGE
from sum_arb import SumArbScanner, SumArbOpportunity

import datetime
import json
import os
import re
import time
from collections import defaultdict

log = logging.getLogger(__name__)

# ── Category P&L persistence ──────────────────────────────────────────────────
_CATEGORY_PNL_FILE = "category_pnl.json"   # saved in bot's working directory

# ── Telegram notifications ────────────────────────────────────────────────────
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "")
TELEGRAM_CHAT_ID   = os.getenv("TELEGRAM_CHAT_ID", "")

async def _send_telegram(message: str) -> None:
    """Fire-and-forget Telegram push notification."""
    import aiohttp
    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    try:
        async with aiohttp.ClientSession() as s:
            await s.post(url, json={"chat_id": TELEGRAM_CHAT_ID, "text": message}, timeout=aiohttp.ClientTimeout(total=5))
    except Exception as exc:
        log.debug("Telegram notify failed: %s", exc)


class BotEngine:
    """
    Wires all subsystems and drives the main event loop.
    """

    def __init__(self, cfg: BotConfig) -> None:
        self._cfg       = cfg
        self._registry  = WhaleRegistry(cfg)
        self._sizer     = PositionSizer(cfg)
        # Current best prices from orderbook (for ghost-order repricing)
        self._ob_cache: dict[str, OrderbookSnapshot] = {}
        # Subsystems initialised inside start() after session is live
        self._sess:    Optional[SessionManager]  = None
        self._gate:    Optional[RateLimitedGate] = None
        self._orders:  Optional[OrderManager]   = None
        self._feed:    Optional[WebSocketFeed]   = None
        self._running  = False
        self._open_positions: dict[str, dict] = {}  # open positions for CLV tracking
        self._selector: Optional[MarketSelector] = None
        self._logger   = TradeLogger()
        # ticker → last signal (so ghost replace can reuse confidence)
        self._signal_cache: dict[str, object] = {}
        self._signal_engine: Optional[SignalEngine] = None
        self._ext_data: Optional[ExternalDataCache] = None
        # Supplementary signal sources
        self._flow_tracker: Optional[FlowTracker] = None
        self._ob_tracker:   Optional[OBTracker]   = None
        self._strike_arb:   Optional[StrikeArbScanner] = None
        self._sum_arb:      Optional[SumArbScanner]   = None
        # State persistence — survives bot restarts
        self._state = StateStore(cfg.state_file)
        # Strike-arb cooldown: ticker → datetime of last placement attempt.
        # Prevents the ghost-order loop (place → 60s ghost → cancel → re-detect →
        # place again, forever) by blocking re-attempts for ARB_COOLDOWN_SECONDS.
        self._arb_cooldown: dict[str, datetime] = {}
        self._ARB_COOLDOWN_SECONDS = 300   # 5 minutes between successful placement attempts
        # Separate shorter cooldown for OB-gate failures (no liquidity yet).
        # 60s lets the bot retry quickly once quotes appear, without flooding logs.
        self._arb_ob_cooldown: dict[str, datetime] = {}
        self._ARB_OB_COOLDOWN_SECONDS = 300
        # Fill-speed tracking: measure seconds from order placement to fill.
        # When recent fills average < fast_fill_threshold_sec the market is
        # liquid — raise the minimum edge floor to only trade the best arbs.
        self._fill_times:       list[float] = []          # rolling fill durations
        self._order_placed_at:  dict[str, float] = {}     # kalshi_id → time.time()

        # ── Quick flip detection ──────────────────────────────────────────────
        # If a large opposing whale arrives within this window after our entry,
        # cancel pending orders and flag the position for exit.
        self._position_entry_time: dict[str, tuple[float, str]] = {}  # ticker → (ts, side)
        self._QUICK_FLIP_WINDOW_SECONDS: float = 300.0   # 5-minute window

        # ── Category P&L tracking ─────────────────────────────────────────────
        # Aggregate win/loss by market category. Persisted to disk across restarts.
        self._category_pnl:    dict[str, float] = defaultdict(float)
        self._category_trades: dict[str, int]   = defaultdict(int)
        self._category_wins:   dict[str, int]   = defaultdict(int)

        # ── Event-level dedup window ─────────────────────────────────────────
        # Tracks the timestamp of the last entry per event key.
        # A second signal on the same event is blocked for 60 minutes,
        # then allowed — enough time that it's likely a new independent whale.
        self._event_last_entry: dict[str, float] = {}   # event_key → epoch seconds
        self._EVENT_DEDUP_WINDOW: float = 3600.0        # 60 minutes

        # ── Telegram two-way control ──────────────────────────────────────────
        self._trading_paused: bool = False   # /pause halts new signal processing

    # ── lifecycle ──────────────────────────────────────────────────────────────

    async def start(self, tickers: list[str]) -> None:
        """
        Main entry point.  Runs until stop() or unrecoverable error.
        """
        dry = " [DRY-RUN — no real orders]" if self._cfg.dry_run else ""
        log.info("═══ Kalshi HFT Liquidity-Capture Bot%s ═══", dry)
        log.info("Tickers: %s", tickers)
        log.info("Config:  balance=$%.2f | kelly=%.0f%% | max_trade=$%.2f | max_exp=%.0f%%",
                 self._cfg.starting_balance,
                 self._cfg.kelly_fraction * 100,
                 self._cfg.max_single_trade_usd,
                 self._cfg.max_exposure_pct * 100)

        async with SessionManager(self._cfg) as sess:
            self._sess     = sess
            self._gate     = RateLimitedGate(self._cfg)
            self._orders   = OrderManager(self._cfg, sess, self._gate)
            self._selector      = MarketSelector(self._cfg, sess)
            self._feed          = WebSocketFeed(self._cfg, sess, market_selector=self._selector)
            self._signal_engine = SignalEngine(self._cfg)
            self._flow_tracker  = FlowTracker()
            self._ob_tracker    = OBTracker()
            self._strike_arb    = StrikeArbScanner(sess, self._cfg)
            self._sum_arb       = SumArbScanner(sess, self._cfg)
            # Subscribe both arb legs as soon as a violation is found — don't
            # wait for the consumer to hit the OB gate before subscribing.
            self._strike_arb.set_subscribe_callback(
                lambda t: self._feed.ensure_ticker_subscribed(t)
            )

            # Pre-flight checks — abort if basic connectivity fails
            await self._preflight(tickers)

            # Load persisted state from previous run
            self._load_category_pnl()
            self._state.load()
            if self._state.open_positions:
                log.warning(
                    "Recovered %d open positions from previous session — "
                    "settlement watcher will handle them.",
                    len(self._state.open_positions),
                )
                self._open_positions.update(self._state.open_positions)

                # Seed event-level dedup window for all recovered positions.
                # Without this, _event_last_entry is empty on restart and the
                # 1-hour window appears "expired" for every existing position —
                # allowing duplicate entries on the same event immediately after boot.
                import time as _t
                for _pos in self._state.open_positions.values():
                    _tkr = _pos.get("ticker", "")
                    if _tkr:
                        _parts = _tkr.rsplit("-", 1)
                        _ekey  = _parts[0] if len(_parts) == 2 else _tkr
                        self._event_last_entry[_ekey] = _t.time()
                log.info(
                    "Dedup: seeded %d event keys from recovered positions",
                    len(self._event_last_entry),
                )

            # Sync portfolio: register exposure from recovered positions and
            # reconcile with live Kalshi orders to catch anything missing from state
            await self._portfolio_sync()

            # Start external data cache (fear/greed, funding rates, calendar)
            self._ext_data = ExternalDataCache()
            await self._ext_data.start()
            set_cache(self._ext_data)  # make available to fundamental_model

            # Resolve tickers: dynamic selection overrides static list
            active_tickers = await self._selector.get_tickers()
            if not active_tickers:
                active_tickers = tickers  # hard fallback to CLI/env tickers
            log.info("Active tickers (%d): %s", len(active_tickers), active_tickers)

            # Wire the selector → feed so refreshes trigger WS resubscription
            self._selector.set_refresh_callback(self._feed.resubscribe)

            await self._sync_balance()
            await self._feed.subscribe(active_tickers)
            self._running = True

            bg_tasks = [
                asyncio.create_task(
                    self._orders.ghost_cleaner_loop(self._on_ghost_replace),
                    name="ghost-cleaner"
                ),
                asyncio.create_task(self._order_watcher_loop(),              name="order-watcher"),
                asyncio.create_task(self._balance_sync_loop(),               name="balance-sync"),
                asyncio.create_task(self._cache_purge_loop(),                name="cache-purge"),
                asyncio.create_task(self._settlement_watcher_loop(),         name="settlement-watcher"),
                asyncio.create_task(self._position_monitor_loop(),           name="position-monitor"),
                asyncio.create_task(self._telegram_poll_loop(),              name="telegram-poll"),
                asyncio.create_task(self._selector.refresh_loop(),           name="market-selector"),
                asyncio.create_task(self._strike_arb.scan_loop(30),          name="strike-arb-scanner"),
                asyncio.create_task(self._strike_arb_consumer_loop(),        name="strike-arb-consumer"),
                *([
                    asyncio.create_task(self._sum_arb.scan_loop(30),         name="sum-arb-scanner"),
                    asyncio.create_task(self._sum_arb_consumer_loop(),       name="sum-arb-consumer"),
                ] if self._cfg.enable_sum_arb else []),
            ]

            try:
                await self._event_loop()
            finally:
                self._running = False
                log.info("Shutting down — cancelling all open orders …")
                n = await self._orders.cancel_all_open()
                log.info("Cancelled %d open orders.", n)
                if self._ext_data:
                    await self._ext_data.stop()
                await self._feed.stop()
                for t in bg_tasks:
                    t.cancel()
                await asyncio.gather(*bg_tasks, return_exceptions=True)
                log.info("Bot stopped cleanly.")

    async def _preflight(self, tickers: list[str]) -> None:
        """
        Validate connectivity, credentials, and market state before trading.
        Raises RuntimeError on hard failures (bad auth, no markets found).
        Logs warnings for soft issues (balance mismatch, ticker not found).
        """
        log.info("Running pre-flight checks …")

        # 1. Verify API connectivity and credentials via balance fetch
        try:
            data = await self._gate.get_balance(self._sess)
            raw  = data.get("balance")
            bal_str = data.get("balance_dollars")
            if bal_str is not None:
                live_balance = float(bal_str)
            elif raw is not None:
                live_balance = float(raw) / 100
            else:
                raise ValueError(f"Unexpected balance response: {data}")
            log.info("Pre-flight OK: API credentials valid | live balance=$%.2f", live_balance)

            expected = float(self._cfg.starting_balance)
            if abs(live_balance - expected) > expected * 0.5:
                log.warning(
                    "Pre-flight WARN: live balance $%.2f differs significantly "
                    "from configured starting_balance $%.2f. "
                    "Update STARTING_BALANCE_USD in config or .env to match your "
                    "actual account balance before trading.",
                    live_balance, expected,
                )
        except Exception as exc:
            raise RuntimeError(
                f"Pre-flight FAILED: could not authenticate with Kalshi API — {exc}\n"
                "Check KALSHI_API_KEY and KALSHI_PRIVATE_KEY_PATH."
            ) from exc

        # 2. Verify at least one target ticker exists and is open
        if tickers and not self._cfg.auto_select_markets:
            found, missing = [], []
            for ticker in tickers[:5]:   # sample first 5 to avoid rate-limit hammering
                try:
                    resp = await self._sess.get(f"/markets/{ticker}")
                    status = resp.get("market", {}).get("status", "unknown")
                    if status in ("active", "open"):
                        found.append(ticker)
                    else:
                        log.warning("Pre-flight WARN: %s is %s (not active)", ticker, status)
                        missing.append(ticker)
                except Exception:
                    log.warning("Pre-flight WARN: ticker %s not found on exchange", ticker)
                    missing.append(ticker)

            if not found:
                log.warning(
                    "Pre-flight WARN: none of the target tickers appear active. "
                    "Bot will still start — auto-selection may find active markets."
                )
            else:
                log.info("Pre-flight OK: %d/%d tickers verified active", len(found), len(tickers[:5]))

        # 3. Watchlist summary
        wl_counts = self._registry.watchlist.count()
        log.info(
            "Pre-flight: watchlist loaded — watching=%d proven=%d demoted=%d banned=%d",
            wl_counts.get("watching", 0), wl_counts.get("proven", 0),
            wl_counts.get("demoted", 0),  wl_counts.get("banned", 0),
        )

        if self._cfg.dry_run:
            log.warning(
                "Pre-flight: DRY-RUN mode — no real orders will be placed. "
                "Run with DRY_RUN=false to enable live trading."
            )

        log.info("Pre-flight checks complete.")

    async def stop(self) -> None:
        self._running = False

    # ── main event loop ───────────────────────────────────────────────────────

    async def _event_loop(self) -> None:
        async for event in self._feed.events():
            if not self._running:
                break
            if isinstance(event, WhaleEvent):
                await self._on_whale(event)
            elif isinstance(event, OrderbookSnapshot):
                self._ob_cache[event.ticker] = event
                # Feed OB tracker so spread/velocity signals stay current
                if self._ob_tracker is not None:
                    self._ob_tracker.update(
                        event.ticker, event.best_yes_bid, event.best_no_bid
                    )
                # ── Real-time arb detection ───────────────────────────────────
                # When a price update arrives for a threshold market we already
                # subscribe to, run an instant in-memory monotonicity check.
                # This fires within milliseconds of a price move — far faster
                # than waiting for the next 30s REST scan pass.
                if (
                    self._strike_arb is not None
                    and event.best_yes_bid is not None
                    and self._strike_arb.is_indexed_ticker(event.ticker)
                ):
                    ws_opps = self._strike_arb.update_ws_price(
                        event.ticker, float(event.best_yes_bid)
                    )
                    for opp in ws_opps:
                        try:
                            self._strike_arb.opportunities.put_nowait(opp)
                        except asyncio.QueueFull:
                            pass

    # ── whale handler ─────────────────────────────────────────────────────────

    async def _on_whale(self, event: WhaleEvent) -> None:
        """
        Core strategy: observe whale → build signal → size → place maker order.
        """
        if self._trading_paused:
            log.debug("Trading paused — ignoring whale signal on %s", event.ticker)
            return

        log.info(
            "WHALE: %s | %s @ $%.4f | $%.2f notional",
            event.ticker, event.side.upper(),
            event.price_dollars, event.notional_usd,
        )
        self._logger.log_whale(
            ticker    = event.ticker,
            side      = event.side,
            notional  = float(event.notional_usd),
            contracts = event.contracts,
            price     = float(event.price_dollars),
        )

        # ── Quick flip detection ──────────────────────────────────────────────
        # If this whale opposes a position we entered within the flip window,
        # cancel pending orders on this ticker — the original signal is invalidated.
        if await self._check_quick_flip(event):
            return  # don't follow the opposing whale on the same ticker

        trade = TradeRecord(
            market_ticker = event.ticker,
            side          = event.side,
            price_dollars = event.price_dollars,
            notional_usd  = event.notional_usd,
            contracts     = event.contracts,
            timestamp     = datetime.datetime.now(datetime.timezone.utc),
        )

        whale_signal = await self._registry.observe_trade(event.whale_id, trade)
        if whale_signal is None:
            self._logger.log_skip(event.ticker, event.side, "no_signal")
            return

        # ── Flow imbalance recording ──────────────────────────────────────────
        if self._flow_tracker is not None:
            self._flow_tracker.record(event.ticker, event.side, float(event.notional_usd))

        # ── Hybrid signal evaluation ──────────────────────────────────────────
        # Fetch live market price for fundamental model and spread check
        # market_price must always be the YES price (0.0–1.0).
        # If the whale traded NO at 78¢, the YES proxy is 1 - 0.78 = 0.22.
        _raw_price = float(event.price_dollars)
        market_price = _raw_price if event.side == "yes" else (1.0 - _raw_price)
        yes_bid, yes_ask = None, None
        try:
            ob = self._ob_cache.get(event.ticker)
            if ob:
                if ob.best_yes_bid:
                    yes_bid = float(ob.best_yes_bid)
                    market_price = yes_bid   # live YES bid overrides proxy
                if ob.best_no_bid:
                    yes_ask = float(Decimal('1') - ob.best_no_bid) if ob.best_no_bid else None
        except Exception:
            pass

        # Fetch dynamic whale threshold for the notional boost calculation
        whale_threshold = (
            float(self._selector.get_threshold(event.ticker))
            if self._selector is not None else 0.0
        )

        close_time_iso = (
            self._selector._market_close_times.get(event.ticker)
            if self._selector is not None else None
        )

        signal = self._signal_engine.evaluate(
            whale_signal     = whale_signal,
            market_price     = market_price,
            close_time_iso   = close_time_iso,
            yes_ask          = yes_ask,
            yes_bid          = yes_bid,
            whale_registry   = self._registry,
            flow_tracker     = self._flow_tracker,
            ob_tracker       = self._ob_tracker,
            notional_usd     = float(event.notional_usd),
            whale_threshold  = whale_threshold,
        )

        # Cache for ghost replace
        self._signal_cache[signal.market_ticker] = signal

        if signal.skip_reason:
            self._logger.log_skip(event.ticker, event.side, signal.skip_reason,
                                  confidence=signal.whale_confidence)
            return

        log.info(
            "COMPOSITE SIGNAL: %s | side=%s | price=$%.2f | conf=%.3f | p_win=%.4f | "
            "whale_conf=%.3f fund_conf=%.3f agree=%.3f mispricing=%+.3f%s | whales=%s",
            signal.market_ticker, signal.side,
            float(signal.base_price),
            signal.final_confidence, signal.win_probability,
            signal.whale_confidence, signal.fundamental_confidence,
            signal.agreement_score, signal.mispricing,
            " [CONVERGENCE]" if signal.convergence_applied else "",
            signal.contributing_whales,
        )

        # ── Signal metadata for logging ───────────────────────────────────────
        _days_to_expiry = self._selector.get_days_to_close(signal.market_ticker) if self._selector else None
        _sig_meta = dict(
            notional_ratio = getattr(signal, "notional_ratio", None),
            days_to_expiry = _days_to_expiry,
            category       = ticker_category if "ticker_category" in dir() else detect_category(signal.market_ticker),
            fund_conf      = signal.fundamental_confidence,
        )

        # ── Max expiry gate ───────────────────────────────────────────────────
        # Never trade markets more than 30 days out. Long-duration markets have
        # high uncertainty, wide spreads, and whale signals carry less information
        # (too much time for conditions to change before settlement).
        _MAX_TRADE_DAYS = 30
        if _days_to_expiry is not None and _days_to_expiry > _MAX_TRADE_DAYS:
            log.info(
                "Expiry gate: %s is %d days out (max %d) — skip",
                signal.market_ticker, _days_to_expiry, _MAX_TRADE_DAYS,
            )
            return

        # ── Event-level deduplication ─────────────────────────────────────────
        # Block any second position on the same underlying event, not just the
        # exact same strike.  KXWTI-26JUN0814-T93.99 and KXWTI-26JUN0814-T91.99
        # are different tickers but 100% correlated — same week, same commodity.
        # Entering both doubles risk without adding diversification.
        #
        # Event key = ticker with the final hyphen-segment (strike/range) stripped.
        #   KXWTI-26JUN0814-T93.99  →  KXWTI-26JUN0814
        #   KXINX-26JUN08H1600-B7412 → KXINX-26JUN08H1600
        #   KXCPI-26MAY-T0.5        →  KXCPI-26MAY
        #   KXRT-MAS-68             →  KXRT-MAS   (same movie, different threshold)
        def _event_key(ticker: str) -> str:
            parts = ticker.rsplit("-", 1)
            return parts[0] if len(parts) == 2 else ticker

        _signal_event   = _event_key(signal.market_ticker)
        _open_tickers   = {pos.get("ticker") for pos in self._open_positions.values()}
        _open_events    = {_event_key(t) for t in _open_tickers}

        if signal.market_ticker in _open_tickers:
            log.info(
                "Dedup skip (exact): already holding %s — ignoring %s signal",
                signal.market_ticker, signal.side,
            )
            return

        if _signal_event in _open_events:
            # Already holding a position on this event — block unconditionally
            # until the position is closed. Time-based windows don't work here:
            # WTI weekly markets can run for days and the window would expire
            # while the position is still open, allowing correlated stacking.
            _blocking = next(
                (t for t in _open_tickers if _event_key(t) == _signal_event), "?"
            )
            log.info(
                "Dedup skip (event): %s blocked by open position %s — "
                "same underlying event, ignoring %s signal",
                signal.market_ticker, _blocking, signal.side,
            )
            return

        # ── Kelly sizing ──────────────────────────────────────────────────────
        # Large whales (≥ premium_whale_notional) get access to the extended
        # 30% exposure zone — they represent higher-conviction informed flow.
        is_premium_whale = float(event.notional_usd) >= self._cfg.premium_whale_notional
        stake = self._sizer.compute_stake(
            win_probability = signal.win_probability,
            price_dollars   = signal.base_price,
            label           = f"{signal.market_ticker}/{signal.side}",
            use_premium_cap = is_premium_whale,
        )

        # ── Category-based Kelly scaling ──────────────────────────────────────
        # Reduce or boost stake based on the bot's own win rate in this category.
        # Requires ≥5 settled trades before adjusting — below that, stay neutral.
        # Only applies when Kelly gives a positive stake — conviction bypass is
        # intentionally unaffected (it already has its own scoring system).
        _cat_for_kelly = detect_category(signal.market_ticker)
        if stake > Decimal("0"):
            _kel_mult = self._category_kelly_multiplier(_cat_for_kelly)
            if _kel_mult != 1.0:
                _adjusted = stake * Decimal(str(_kel_mult))
                # Hard cap: never exceed MAX_SINGLE_TRADE_USD after scaling up
                _adjusted = min(_adjusted, Decimal(str(self._cfg.max_single_trade_usd)))
                log.debug(
                    "Category Kelly multiplier: %s %.2f× → $%.2f → $%.2f",
                    _cat_for_kelly, _kel_mult, float(stake), float(_adjusted),
                )
                stake = _adjusted

        if stake <= Decimal("0"):
            # ── Conviction-score bypass ───────────────────────────────────────
            # When Kelly finds no edge, use a conviction score to decide whether
            # to trade anyway. Score 0-10 — need ≥5 to trade.
            # Hard limit: never trade above 90¢ regardless of conviction.
            #
            # Scoring (plain English):
            #   Notional size:   1× threshold=0, 3×=+2, 5×=+3, 10×=+4
            #   Days to expiry:  >30 days=0, ≤30=+1, ≤14=+2, ≤7=+3, ≤3=+4
            #   Category wins:   <5 trades=0, ≥60% wins=+2, ≥70% wins=+3
            #   Contract price:  >90¢=BLOCK, >85¢=-3, >75¢=-2, >60¢=-1, ≤60¢=0
            #   No real model:   fund_conf≥0.20 → not eligible for bypass

            # Always use YES price for price penalties — base_price is the whale's
            # execution price which is the NO price for NO trades (wrong direction).
            price_f        = market_price   # already corrected to YES price above
            notional_ratio = getattr(signal, "notional_ratio", 1.0)

            # Hard limit — never bypass above 90¢ YES price
            if price_f >= 0.90:
                self._logger.log_skip(event.ticker, event.side, "no_kelly_edge",
                                      confidence=signal.final_confidence, **_sig_meta)
                return

            # Only bypass on no-model markets — real models should use Kelly
            if signal.fundamental_confidence >= 0.20:
                self._logger.log_skip(event.ticker, event.side, "no_kelly_edge",
                                      confidence=signal.final_confidence, **_sig_meta)
                return

            # Price cap: no conviction bypass above 75¢ YES price on no-model markets.
            # Above 75¢ the book is thin, fills are rare, and we have no model to
            # justify the trade. Commodity markets with real spot data (fund_conf≥0.20)
            # are already excluded from bypass entirely (handled above).
            if price_f > 0.75:
                log.info(
                    "Conviction bypass: %s — price %.2f > 0.75 cap — skip",
                    signal.market_ticker, price_f,
                )
                self._logger.log_skip(event.ticker, event.side, "no_kelly_edge",
                                      confidence=signal.final_confidence, **_sig_meta)
                return

            conviction = 0

            # Notional size score
            if notional_ratio >= 10.0:
                conviction += 4
            elif notional_ratio >= 5.0:
                conviction += 3
            elif notional_ratio >= 3.0:
                conviction += 2
            elif notional_ratio >= 2.0:
                conviction += 1

            # Days to expiry score
            days_out_cv = (
                self._selector.get_days_to_close(signal.market_ticker)
                if self._selector else None
            )
            if days_out_cv is not None:
                if days_out_cv <= 3:
                    conviction += 4
                elif days_out_cv <= 7:
                    conviction += 3
                elif days_out_cv <= 14:
                    conviction += 2
                elif days_out_cv <= 30:
                    conviction += 1

            # Category history score
            ticker_category = detect_category(signal.market_ticker)
            cat_trades_cv = self._category_trades.get(ticker_category, 0)
            cat_wins_cv   = self._category_wins.get(ticker_category, 0)
            if cat_trades_cv >= 5:
                win_rate = cat_wins_cv / cat_trades_cv
                if win_rate >= 0.70:
                    conviction += 3
                elif win_rate >= 0.60:
                    conviction += 2

            # Price penalty
            if price_f > 0.85:
                conviction -= 3
            elif price_f > 0.75:
                conviction -= 2
            elif price_f > 0.60:
                conviction -= 1

            if conviction >= 6:
                # Scale stake with conviction: score 7→$7, 8→$8, 9→$9, 10+→$10
                bypass_stake = Decimal(str(min(10.0, max(7.0, conviction))))
                stake = bypass_stake
                log.info(
                    "Conviction bypass: %s — score=%d/10 → $%.2f stake "
                    "(notional=%.1f× | days=%s | wins=%d/%d | price=%.2f)",
                    signal.market_ticker, conviction, float(stake),
                    notional_ratio,
                    days_out_cv if days_out_cv is not None else "?",
                    cat_wins_cv, cat_trades_cv, price_f,
                )
            else:
                log.info(
                    "Conviction bypass: %s — score=%d/10 < 7 — skip "
                    "(notional=%.1f× | days=%s | wins=%d/%d | price=%.2f)",
                    signal.market_ticker, conviction,
                    notional_ratio,
                    days_out_cv if days_out_cv is not None else "?",
                    cat_wins_cv, cat_trades_cv, price_f,
                )
                self._logger.log_skip(event.ticker, event.side, "no_kelly_edge",
                                      confidence=signal.final_confidence,
                                      conviction_score=conviction, **_sig_meta)
                return

        contracts  = self._sizer.contracts_for_stake(stake, signal.base_price)
        cost_basis = signal.base_price * contracts

        if cost_basis > self._sizer._exposure_headroom(is_premium_whale):
            log.warning(
                "Exposure headroom $%.2f < cost_basis $%.2f — skip",
                self._sizer._exposure_headroom(), cost_basis,
            )
            self._logger.log_skip(
                event.ticker, event.side, "exposure_cap",
                confidence=signal.final_confidence, **_sig_meta,
            )
            return

        if not self._sizer.can_trade():
            log.warning("Bot cannot trade due to safety limits. Skipping order placement.")
            self._logger.log_skip(
                event.ticker, event.side, "safety_limits_reached",
                confidence=signal.final_confidence, **_sig_meta,
            )
            return

        # ── Correlation / concentration check ─────────────────────────────────
        # Allow up to 2 open positions per macro category.
        # e.g. KXFEDDECISION-H25 and KXFEDDECISION-H0 are different contracts
        # on the same event — a whale on the second is independent information.
        _MAX_PER_CATEGORY = 2
        ticker_category = detect_category(signal.market_ticker)
        if ticker_category not in ("GENERIC",):
            open_cat_count = sum(
                1 for pos in self._open_positions.values()
                if detect_category(pos.get("ticker", "")) == ticker_category
            )
            if open_cat_count >= _MAX_PER_CATEGORY:
                log.info(
                    "Correlation skip: %d open %s positions (max=%d) — skipping %s",
                    open_cat_count, ticker_category, _MAX_PER_CATEGORY,
                    signal.market_ticker,
                )
                self._logger.log_skip(
                    event.ticker, event.side,
                    f"correlated_position_limit ({ticker_category} {open_cat_count}/{_MAX_PER_CATEGORY})",
                    confidence=signal.final_confidence,
                )
                return

        side_enum = Side.YES if signal.side == "yes" else Side.NO

        # ── Chase score ───────────────────────────────────────────────────────
        # Additive bonuses on top of confidence-based base chase, hard cap = 5¢.
        # Plain English: chase harder when the whale is more convincing,
        # the market closes soon, and we have a good track record here.
        notional_chase_bonus = 0

        # 1. Notional size — how far above threshold the whale traded
        #    3–7×  → +1¢  |  ≥7× → +2¢
        notional_ratio = getattr(signal, "notional_ratio", 1.0)
        if notional_ratio >= 7.0:
            notional_chase_bonus += 2
        elif notional_ratio >= 3.0:
            notional_chase_bonus += 1

        # 2. Days to expiry — sooner = chase harder (informed traders act late)
        #    ≤3 days → +2¢  |  ≤7 days → +1¢
        days_out = (
            self._selector.get_days_to_close(signal.market_ticker)
            if self._selector else None
        )
        if days_out is not None:
            if days_out <= 3:
                notional_chase_bonus += 2
            elif days_out <= 7:
                notional_chase_bonus += 1

        # 3. Category win rate — if we've been profitable here, chase harder
        #    ≥60% win rate with ≥5 settled trades → +1¢
        cat_trades = self._category_trades.get(ticker_category, 0)
        cat_wins   = self._category_wins.get(ticker_category, 0)
        if cat_trades >= 5 and (cat_wins / cat_trades) >= 0.60:
            notional_chase_bonus += 1

        log.info(
            "Chase score: %s | notional=+%d¢ (%.1f×) | days=+%d¢ (%s days) | "
            "history=+%d¢ (%d/%d wins) | total_bonus=+%d¢",
            signal.market_ticker,
            2 if notional_ratio >= 7.0 else (1 if notional_ratio >= 3.0 else 0),
            notional_ratio,
            2 if days_out is not None and days_out <= 3 else (1 if days_out is not None and days_out <= 7 else 0),
            days_out if days_out is not None else "?",
            1 if cat_trades >= 5 and cat_wins / max(cat_trades, 1) >= 0.60 else 0,
            cat_wins, cat_trades,
            notional_chase_bonus,
        )

        order = await self._orders.place_maker_order(
            ticker               = signal.market_ticker,
            side                 = side_enum,
            whale_price          = signal.base_price,
            contracts            = contracts,
            cost_basis           = cost_basis,
            confidence           = signal.final_confidence,
            notional_chase_bonus = notional_chase_bonus,
            mispricing           = signal.mispricing,
            fund_conf            = signal.fundamental_confidence,
        )
        if order:
            self._order_placed_at[order.kalshi_id] = time.time()
            # Record entry direction for quick-flip detection
            self._position_entry_time[signal.market_ticker] = (time.time(), signal.side)
            # Record event-level entry time for 60-min dedup window
            self._event_last_entry[_signal_event] = time.time()
            pos = {
                "ticker":               signal.market_ticker,
                "side":                 side_enum,
                "entry_price":          signal.base_price,
                "contracts":            contracts,
                "cost_basis":           cost_basis,
                "confidence":           signal.final_confidence,
                "win_prob":             signal.win_probability,
                "whales_contributing":  signal.contributing_whales,
            }
            self._open_positions[order.kalshi_id] = pos
            self._state.upsert_position(order.kalshi_id, {
                **pos,
                "side": side_enum.value,
                "entry_price": str(signal.base_price),
                "cost_basis": str(cost_basis),
            })
            self._sizer.register_open(cost_basis)
            self._logger.log_order(
                ticker           = signal.market_ticker,
                side             = signal.side,
                order_id         = order.kalshi_id,
                entry_price      = float(signal.base_price),
                contracts        = contracts,
                cost             = float(cost_basis),
                confidence       = signal.final_confidence,
                win_prob         = signal.win_probability,
                chase_cents      = getattr(order, "_chase_cents", 0),
                post_only        = getattr(order, "_post_only", True),
                conviction_score = conviction if "conviction" in dir() else None,
                **_sig_meta,
            )
            asyncio.create_task(
                self._watch_order(order.kalshi_id, cost_basis),
                name=f"watch-{order.kalshi_id}",
            )

    # ── ghost order replace callback ──────────────────────────────────────────

    async def _on_ghost_replace(
        self,
        ticker:        str,
        side:          Side,
        cost_basis:    Decimal,
        contracts:     int,
        ghost_attempt: int = 1,
        whale_price:   Optional[Decimal] = None,
    ) -> None:
        """
        Called by ghost_cleaner_loop after cancelling a stale order.
        Re-prices at the current best orderbook price and re-submits,
        escalating the chase by +1¢ per attempt on top of the confidence-based chase.
        """
        _MAX_GHOST_ATTEMPTS = 3
        if ghost_attempt > _MAX_GHOST_ATTEMPTS:
            log.info(
                "GHOST REPLACE: %s — max attempts (%d) reached, abandoning",
                ticker, _MAX_GHOST_ATTEMPTS,
            )
            self._sizer.register_close(cost_basis, Decimal("0"), was_filled=False)
            # Clean up any stale position entry so exposure tracking stays accurate
            stale_ids = [k for k, v in self._open_positions.items() if v.get("ticker") == ticker]
            for kid in stale_ids:
                self._open_positions.pop(kid, None)
                self._state.remove_position(kid)
            return
        # Release exposure from the cancelled order (was_filled=False — not a loss)
        self._sizer.register_close(cost_basis, Decimal("0"), was_filled=False)

        # Get current best price — OB cache first, REST API fallback
        current_price: Optional[Decimal] = None
        ob = self._ob_cache.get(ticker)
        if ob is not None:
            current_price = ob.best_yes_bid if side == Side.YES else ob.best_no_bid

        if current_price is None:
            # OB cache empty (thin market with no recent book updates) — fetch
            # current price from REST API so the ghost can still be repriced.
            try:
                resp = await self._sess.get(f"/markets/{ticker}")
                mkt  = resp.get("market", {})
                # Kalshi REST returns yes_bid / no_bid as cent strings or dollars.
                # Fallback: NO bid ≈ 1 - yes_ask when no_bid is absent (thin markets).
                if side == Side.YES:
                    price_raw = mkt.get("yes_bid")
                else:
                    price_raw = mkt.get("no_bid")
                    if price_raw is None:
                        yes_ask_raw = mkt.get("yes_ask")
                        if yes_ask_raw is not None:
                            yes_ask = Decimal(str(yes_ask_raw))
                            # Normalise: Kalshi may return cents (0-100) or dollars (0-1)
                            if yes_ask > 1:
                                yes_ask = yes_ask / Decimal("100")
                            price_raw = str((Decimal("1") - yes_ask).quantize(Decimal("0.01")))
                            log.info(
                                "Ghost reprice: no_bid absent — using 1-yes_ask proxy: %s NO → $%s",
                                ticker, price_raw,
                            )
                if price_raw is not None:
                    current_price = Decimal(str(price_raw))
                    if current_price > 1:
                        current_price = current_price / Decimal("100")
                    log.info(
                        "Ghost reprice REST fallback: %s %s → $%.4f",
                        ticker, side.value.upper(), current_price,
                    )
            except Exception as exc:
                log.warning("Ghost reprice REST fallback failed for %s: %s", ticker, exc)

        if current_price is None:
            log.warning("No best price for %s %s — cannot reprice ghost order", ticker, side.value)
            return

        # Price-based chase ceiling: contracts > 75¢ are near-certain — cap at 3¢
        effective_max_chase = 3 if current_price > Decimal("0.75") else self._cfg.max_chase_cents

        # Hard cap: never pay more than whale_price + 5¢ regardless of market movement
        if whale_price is not None:
            absolute_ceiling = whale_price + Decimal("0.05")
            if current_price > absolute_ceiling:
                log.info(
                    "GHOST REPLACE: %s — current_price=$%.4f exceeds whale_price+5¢ ($%.4f) — abandoning",
                    ticker, current_price, absolute_ceiling,
                )
                stale_ids = [k for k, v in self._open_positions.items() if v.get("ticker") == ticker]
                for kid in stale_ids:
                    self._open_positions.pop(kid, None)
                    self._state.remove_position(kid)
                return  # exposure already released above

        log.info(
            "GHOST REPLACE: %s %s | new_price=$%.4f | contracts=%d | max_chase=%d¢",
            ticker, side.value.upper(), current_price, contracts, effective_max_chase,
        )

        # Re-size at the new price (always re-run Kelly to stay disciplined)
        # Optimization: Use the latest signal from the registry instead of a hardcoded 0.55
        signal = await self._registry.get_signal(ticker)
        win_prob = signal.win_probability if signal else 0.55
        
        stake = self._sizer.compute_stake(
            win_probability = win_prob,
            price_dollars   = current_price,
            label           = f"replace-{ticker}",
        )
        if stake <= Decimal("0"):
            return

        new_contracts  = self._sizer.contracts_for_stake(stake, current_price)
        new_cost_basis = current_price * new_contracts

        # Reuse cached confidence so ghost replace uses same execution tier
        cached_signal = self._signal_cache.get(ticker)
        if cached_signal is None:
            log.warning("Ghost replace: no cached signal for %s — skipping reprice", ticker)
            return
        cached_conf = cached_signal.final_confidence

        order = await self._orders.place_maker_order(
            ticker        = ticker,
            side          = side,
            whale_price   = current_price,
            contracts     = new_contracts,
            cost_basis    = new_cost_basis,
            confidence    = cached_conf,
            ghost_attempt = ghost_attempt,
            max_chase     = effective_max_chase,
            orig_whale_price = whale_price,
        )
        if order:
            order.ghost_attempts = ghost_attempt
            pos = {
                "ticker":              ticker,
                "side":               side,
                "entry_price":        current_price,
                "contracts":          new_contracts,
                "cost_basis":         new_cost_basis,
                "confidence":         cached_conf,
                "win_prob":           cached_signal.win_probability if cached_signal else 0.5,
                "whales_contributing": cached_signal.contributing_whales if cached_signal else [],
            }
            self._open_positions[order.kalshi_id] = pos
            self._state.upsert_position(order.kalshi_id, {
                **pos,
                "side": side.value,
                "entry_price": str(current_price),
                "cost_basis": str(new_cost_basis),
            })
            self._sizer.register_open(new_cost_basis)
            asyncio.create_task(
                self._watch_order(order.kalshi_id, new_cost_basis),
                name=f"watch-replace-{order.kalshi_id}",
            )

    # ── order watcher ─────────────────────────────────────────────────────────

    async def _watch_order(self, kalshi_id: str, cost_basis: Decimal) -> None:
        """
        Poll a single order until it reaches a terminal state.
        On fill, updates PositionSizer.  Ghost cleaner handles stale ones.
        """
        for _ in range(60):   # max 5 minutes of watching (5s × 60)
            await asyncio.sleep(5)
            status = await self._orders.sync_status(kalshi_id)
            if status == OrderStatus.FILLED:
                log.info("FILLED: %s", kalshi_id)
                pos_preview = self._open_positions.get(kalshi_id, {})
                asyncio.create_task(_send_telegram(
                    f"✅ FILLED: {pos_preview.get('ticker', kalshi_id)}\n"
                    f"{str(pos_preview.get('side', '')).upper()} "
                    f"{pos_preview.get('contracts', '?')} contracts @ "
                    f"${float(pos_preview.get('entry_price', 0)):.2f} | "
                    f"cost ${float(pos_preview.get('cost_basis', 0)):.2f}"
                ))
                # Record fill speed for dynamic edge threshold
                placed_at = self._order_placed_at.pop(kalshi_id, None)
                if placed_at is not None:
                    fill_dur = time.time() - placed_at
                    self._fill_times.append(fill_dur)
                    if len(self._fill_times) > self._cfg.fill_speed_window:
                        self._fill_times = self._fill_times[-self._cfg.fill_speed_window:]
                pos = self._open_positions.get(kalshi_id, {})
                self._logger.log_fill(
                    ticker      = pos.get("ticker", ""),
                    side        = pos.get("side", Side.YES).value if hasattr(pos.get("side"), "value") else str(pos.get("side", "")),
                    order_id    = kalshi_id,
                    entry_price = float(pos.get("entry_price", 0)),
                    fill_price  = float(pos.get("entry_price", 0)),
                    contracts   = pos.get("contracts", 0),
                    cost        = float(pos.get("cost_basis", cost_basis)),
                    confidence  = pos.get("confidence", 0.0),
                )
                # Filled orders remain in _open_positions until settlement
                # At settlement we compute realized PnL and call register_close
                return
            elif status in (OrderStatus.CANCELLED, OrderStatus.REJECTED):
                # Check for partial fill before treating as a total cancel.
                # Kalshi sets filled_count > 0 when some contracts traded before
                # the order was cancelled (e.g. taker order with insufficient depth,
                # or ghost cleaner fired while resting portion was still open).
                managed = self._orders._orders.get(kalshi_id)
                partial_fill = managed.filled_count if managed else 0

                if partial_fill > 0:
                    pos = self._open_positions.get(kalshi_id, {})
                    orig_contracts  = pos.get("contracts", partial_fill)
                    orig_cost_basis = pos.get("cost_basis", cost_basis)
                    filled_cost   = orig_cost_basis * Decimal(partial_fill) / Decimal(max(orig_contracts, 1))
                    unfilled_cost = orig_cost_basis - filled_cost

                    fill_pct = partial_fill / max(orig_contracts, 1)

                    # ── Partial fill below 50%: not worth holding, exit immediately ──
                    if fill_pct < 0.50:
                        log.info(
                            "PARTIAL FILL BELOW THRESHOLD: %s — %d/%d contracts (%.0f%%) "
                            "< 50%% minimum — releasing full exposure, not holding position",
                            kalshi_id, partial_fill, orig_contracts, fill_pct * 100,
                        )
                        self._sizer.register_close(orig_cost_basis, Decimal("0"), was_filled=False)
                        self._open_positions.pop(kalshi_id, None)
                        self._state.remove_position(kalshi_id)
                        return

                    # ── Partial fill ≥ 50%: keep the filled portion as a live position ──
                    log.info(
                        "PARTIAL FILL: %s — %d/%d contracts filled (%.0f%%) | "
                        "filled_cost=$%.2f | releasing unfilled=$%.2f",
                        kalshi_id, partial_fill, orig_contracts, fill_pct * 100,
                        float(filled_cost), float(unfilled_cost),
                    )

                    # Release only the unfilled portion of exposure
                    if unfilled_cost > Decimal("0"):
                        self._sizer.register_close(unfilled_cost, Decimal("0"), was_filled=False)

                    # Update position to reflect the actual fill count / cost
                    if kalshi_id in self._open_positions:
                        self._open_positions[kalshi_id]["contracts"]  = partial_fill
                        self._open_positions[kalshi_id]["cost_basis"] = filled_cost
                        self._state.upsert_position(kalshi_id, {
                            **self._open_positions[kalshi_id],
                            "side":        self._open_positions[kalshi_id]["side"].value
                                           if hasattr(self._open_positions[kalshi_id]["side"], "value")
                                           else str(self._open_positions[kalshi_id]["side"]),
                            "entry_price": str(self._open_positions[kalshi_id].get("entry_price", "")),
                            "cost_basis":  str(filled_cost),
                            "contracts":   partial_fill,
                        })
                    self._logger.log_fill(
                        ticker      = pos.get("ticker", ""),
                        side        = pos.get("side", Side.YES).value if hasattr(pos.get("side"), "value") else str(pos.get("side", "")),
                        order_id    = kalshi_id,
                        entry_price = float(pos.get("entry_price", 0)),
                        fill_price  = float(pos.get("entry_price", 0)),
                        contracts   = partial_fill,
                        cost        = float(filled_cost),
                        confidence  = pos.get("confidence", 0.0),
                    )
                    # Position stays in _open_positions — settlement watcher will close it
                else:
                    # ── Total cancel: zero fill, release all exposure ──────────────
                    self._sizer.register_close(cost_basis, Decimal("0"), was_filled=False)
                    log.info("Order %s: %s — releasing exposure, not counting as loss", status.value, kalshi_id)
                    pos = self._open_positions.pop(kalshi_id, {})
                    self._state.remove_position(kalshi_id)
                    self._logger.log_cancel(
                        ticker   = pos.get("ticker", ""),
                        side     = pos.get("side", Side.YES).value if hasattr(pos.get("side"), "value") else "",
                        order_id = kalshi_id,
                        reason   = status.value,
                    )
                return
        # Timeout — release exposure and mark cancelled so ghost cleaner stops retrying
        log.warning("_watch_order timeout for %s — releasing exposure", kalshi_id)
        self._sizer.register_close(cost_basis, Decimal("0"), was_filled=False)
        self._open_positions.pop(kalshi_id, None)
        self._state.remove_position(kalshi_id)
        async with self._orders._lock:
            timed_out_order = self._orders._orders.get(kalshi_id)
            if timed_out_order and timed_out_order.status == OrderStatus.OPEN:
                timed_out_order.status = OrderStatus.CANCELLED

    # ── background tasks ──────────────────────────────────────────────────────

    async def _order_watcher_loop(self) -> None:
        """Periodic batch status sync for all open orders."""
        while True:
            await asyncio.sleep(5)
            for order in self._orders.open_orders():
                try:
                    await self._orders.sync_status(order.kalshi_id)
                except Exception as exc:
                    log.debug("Watcher error %s: %s", order.kalshi_id, exc)

    async def _decay_loop(self) -> None:
        while True:
            await asyncio.sleep(self._cfg.decay_interval_seconds)
            try:
                await self._registry.decay_pass()
            except Exception as exc:
                log.error("Decay loop error: %s", exc)

    async def _balance_sync_loop(self) -> None:
        while True:
            await asyncio.sleep(self._cfg.balance_sync_interval_seconds)
            await self._sync_balance()

    async def _cache_purge_loop(self) -> None:
        while True:
            await asyncio.sleep(600)
            try:
                n = await self._gate.cache.purge()
                log.debug("Cache purge: removed %d stale entries", n)
            except Exception as exc:
                log.debug("Cache purge error: %s", exc)

    async def _settlement_watcher_loop(self) -> None:
        """
        Poll Kalshi for settled markets we have open positions in.
        When a market settles: compute realized PnL, update sizer, log to trade_logger.
        """
        while True:
            await asyncio.sleep(self._cfg.settlement_check_interval_seconds)
            try:
                await self._check_settlements()
            except Exception as exc:
                log.error("Settlement watcher error: %s", exc)

    async def _check_settlements(self) -> None:
        """Check if any of our open positions have settled."""
        if not self._open_positions:
            return
        tickers_to_check = {
            pos["ticker"] for pos in self._open_positions.values()
            if isinstance(pos.get("ticker"), str)
        }
        for ticker in tickers_to_check:
            try:
                resp = await self._sess.get(f"/markets/{ticker}")
                market = resp.get("market", {})
                status = market.get("status", "")
                result = market.get("result", "")

                log.debug("Settlement check %s: status=%r result=%r", ticker, status, result)

                if status not in ("settled", "closed", "finalized", "resolved") or not result:
                    continue

                # Market has settled — determine settlement value
                if result.lower() == "yes":
                    settlement_value = Decimal("1.00")
                elif result.lower() == "no":
                    settlement_value = Decimal("0.00")
                else:
                    log.warning("Unknown settlement result '%s' for %s", result, ticker)
                    continue

                log.info("SETTLEMENT: %s | result=%s | value=$%.2f", ticker, result, settlement_value)

                # Find all filled positions for this ticker and settle them
                settled_ids = [
                    oid for oid, pos in self._open_positions.items()
                    if pos.get("ticker") == ticker
                ]
                for oid in settled_ids:
                    pos = self._open_positions.pop(oid, None)
                    self._state.remove_position(oid)
                    if not pos:
                        continue
                    side        = pos.get("side", Side.YES)
                    entry_price = Decimal(str(pos.get("entry_price", 0)))
                    contracts   = pos.get("contracts", 0)
                    cost_basis  = Decimal(str(pos.get("cost_basis", 0)))

                    # Realized PnL calculation:
                    # YES position: paid entry_price, wins (1 - entry_price) if YES settles,
                    #               loses entry_price if NO settles.
                    # NO  position: paid entry_price, wins (1 - entry_price) if NO settles,
                    #               loses entry_price if YES settles.
                    # In both cases the loss is -entry_price (your stake), not -(1-entry_price).
                    side_val = side.value if hasattr(side, "value") else str(side)
                    if side_val == "yes":
                        if settlement_value == Decimal("1.00"):
                            pnl = (Decimal("1.00") - entry_price) * contracts
                        else:
                            pnl = -entry_price * contracts
                    else:
                        no_win = Decimal("1.00") - entry_price   # profit per contract if NO wins
                        if settlement_value == Decimal("0.00"):   # NO settles as winner
                            pnl = no_win * contracts
                        else:                                      # YES settles, NO loses stake
                            pnl = -entry_price * contracts

                    self._sizer.register_close(cost_basis, pnl, was_filled=True)

                    # ── Category P&L tracking ─────────────────────────────────
                    category = detect_category(ticker)
                    self._category_pnl[category]    += float(pnl)
                    self._category_trades[category] += 1
                    if float(pnl) > 0:
                        self._category_wins[category] += 1

                    self._logger.log_settlement(
                        ticker           = ticker,
                        settlement_value = float(settlement_value),
                        our_side         = side_val,
                        entry_price      = float(entry_price),
                        contracts        = contracts,
                        realized_pnl     = float(pnl),
                    )
                    log.info(
                        "SETTLED: %s | side=%s | entry=$%.4f | contracts=%d | pnl=$%+.4f",
                        ticker, side_val, entry_price, contracts, pnl,
                    )
                    self._log_category_pnl()
                    self._save_category_pnl()

            except Exception as exc:
                log.debug("Settlement check failed for %s: %s", ticker, exc)

    # ── sum-to-1 arbitrage ────────────────────────────────────────────────────

    async def _sum_arb_consumer_loop(self) -> None:
        """Drain the sum-arb opportunity queue."""
        while True:
            try:
                opp = await asyncio.wait_for(
                    self._sum_arb.opportunities.get(), timeout=5.0
                )
                await self._on_sum_arb(opp)
            except asyncio.TimeoutError:
                pass
            except asyncio.CancelledError:
                return
            except Exception as exc:
                log.debug("Sum arb consumer error: %s", exc)

    async def _on_sum_arb(self, opp: SumArbOpportunity) -> None:
        """
        Evaluate and trade a sum-to-1 opportunity.

        Buys YES on every leg concurrently.  All legs must have OB liquidity
        before any order is placed.  If any leg fails after placement we log
        it; filled legs are kept as directional positions (still +EV if the
        mutual-exclusivity assumption holds).
        """
        log.info("SUM ARB candidate: %s", repr(opp))

        if not self._sizer.can_trade():
            log.debug("Sum arb: safety limits — skip")
            return

        # ── Cooldown (keyed on collection ticker) ────────────────────────────
        now = datetime.datetime.now(datetime.timezone.utc)
        last = self._arb_cooldown.get(f"sum:{opp.collection_ticker}")
        if last is not None and (now - last).total_seconds() < self._ARB_COOLDOWN_SECONDS:
            log.debug("Sum arb: %s on cooldown — skip", opp.collection_ticker)
            return

        # ── Existing-position guard ───────────────────────────────────────────
        open_tickers = {pos.get("ticker") for pos in self._open_positions.values()}
        if any(leg.ticker in open_tickers for leg in opp.legs):
            log.debug("Sum arb: already hold a leg of %s — skip", opp.collection_ticker)
            return

        # ── OB liquidity gate: all legs must have resting YES sellers ─────────
        # YES ask = 1 − best_no_bid on that market.
        leg_asks: dict[str, float] = {}
        for leg in opp.legs:
            ob = self._ob_cache.get(leg.ticker)
            if ob is None or ob.best_no_bid is None:
                log.info(
                    "Sum arb: no OB data for %s — subscribing and skipping",
                    leg.ticker,
                )
                if self._feed:
                    await self._feed.ensure_ticker_subscribed(leg.ticker)
                return
            leg_asks[leg.ticker] = float(Decimal("1.00") - ob.best_no_bid)

        combined_taker = sum(leg_asks.values())
        if combined_taker >= 1.0 - 0.01:   # require ≥ 1¢ net profit after spreads
            log.info(
                "Sum arb: spread too wide — combined taker=%.3f ≥ 0.99 | %s",
                combined_taker, opp.collection_ticker,
            )
            return

        # ── Sizing: 1 contract per leg (conservative for first live test) ─────
        # Each contract costs `combined_taker` total; payout is $1.00 per set.
        cost_per_set = Decimal(str(round(combined_taker, 4)))
        headroom     = self._sizer._exposure_headroom(False)
        max_sets     = int(headroom / cost_per_set) if cost_per_set > 0 else 0
        contracts    = int(min(max_sets, self._cfg.max_single_trade_usd // cost_per_set))

        # Cap at 5 contracts per sum-arb trade while strategy is unproven
        contracts = min(contracts, 5)

        if contracts < 1:
            log.debug(
                "Sum arb: headroom $%.2f < cost_per_set $%.2f — skip",
                float(headroom), float(cost_per_set),
            )
            return

        arb_conf = min(0.85, opp.profit_pct * 5)

        # Stamp cooldown before placement
        self._arb_cooldown[f"sum:{opp.collection_ticker}"] = now

        # ── Place all legs concurrently ───────────────────────────────────────
        tasks = [
            self._orders.place_taker_order(
                ticker     = leg.ticker,
                side       = Side.YES,
                price      = Decimal(str(round(min(leg_asks[leg.ticker], 0.99), 2))),
                contracts  = contracts,
                cost_basis = Decimal(str(round(leg_asks[leg.ticker], 4))) * contracts,
            )
            for leg in opp.legs
        ]
        results = await asyncio.gather(*tasks)

        filled_legs  = [(leg, order) for leg, order in zip(opp.legs, results) if order]
        missing_legs = [leg for leg, order in zip(opp.legs, results) if not order]

        if missing_legs:
            log.warning(
                "Sum arb: %d/%d legs accepted — missing: %s | "
                "filled legs kept as directional positions",
                len(filled_legs), opp.n_legs,
                [l.ticker for l in missing_legs],
            )

        # ── Register filled legs ──────────────────────────────────────────────
        now_ts = time.time()
        for leg, order in filled_legs:
            ep = Decimal(str(round(leg_asks[leg.ticker], 4)))
            cb = ep * contracts
            self._order_placed_at[order.kalshi_id] = now_ts
            pos = {
                "ticker":              leg.ticker,
                "side":                Side.YES,
                "entry_price":         ep,
                "contracts":           contracts,
                "cost_basis":          cb,
                "confidence":          arb_conf,
                "win_prob":            1.0,
                "whales_contributing": [],
            }
            self._open_positions[order.kalshi_id] = pos
            self._state.upsert_position(order.kalshi_id, {
                **pos,
                "side":        Side.YES.value,
                "entry_price": str(ep),
                "cost_basis":  str(cb),
            })
            self._sizer.register_open(cb)
            asyncio.create_task(
                self._watch_order(order.kalshi_id, cb),
                name=f"watch-sum-arb-{order.kalshi_id}",
            )

        if len(filled_legs) == opp.n_legs:
            net_profit = 1.0 - combined_taker
            log.info(
                "SUM ARB LOCKED: %s | %d legs | combined=$%.3f | "
                "guaranteed_profit=$%.3f (%.1f%%) × %d contracts",
                opp.collection_ticker, opp.n_legs, combined_taker,
                net_profit, net_profit * 100, contracts,
            )

    # ── strike arbitrage ──────────────────────────────────────────────────────

    async def _strike_arb_consumer_loop(self) -> None:
        """
        Drain the strike-arb opportunity queue and evaluate each one.
        Runs as a background task alongside the main event loop.
        """
        while True:
            try:
                opp = await asyncio.wait_for(
                    self._strike_arb.opportunities.get(), timeout=5.0
                )
                await self._on_strike_arb(opp)
            except asyncio.TimeoutError:
                pass   # nothing in queue — loop again
            except asyncio.CancelledError:
                return
            except Exception as exc:
                log.warning("Strike arb consumer error: %s", exc, exc_info=True)

    async def _on_strike_arb(self, opp: ArbOpportunity) -> None:
        """
        Evaluate and optionally trade an inter-strike arbitrage opportunity.

        Logic: P(rate ≤ higher_strike) must be ≥ P(rate ≤ lower_strike).
        When the market violates this, the higher-strike YES is underpriced.
        We buy it at the current price and hold until convergence or settlement.

        Edge sizing: treat fair_value as the win probability (it's the price
        the market *should* be at) and let Kelly do the rest.
        """
        # ── Cooldown guard — prevents log/order spam on persistent violations ───
        now = datetime.datetime.now(datetime.timezone.utc)
        last_attempt = self._arb_cooldown.get(opp.ticker)
        if last_attempt is not None:
            if (now - last_attempt).total_seconds() < self._ARB_COOLDOWN_SECONDS:
                return
        last_ob_fail = self._arb_ob_cooldown.get(opp.ticker)
        if last_ob_fail is not None:
            if (now - last_ob_fail).total_seconds() < self._ARB_OB_COOLDOWN_SECONDS:
                return
        # Prune stale cooldown entries to keep the dicts small
        self._arb_cooldown = {
            t: ts for t, ts in self._arb_cooldown.items()
            if (now - ts).total_seconds() < self._ARB_COOLDOWN_SECONDS
        }
        self._arb_ob_cooldown = {
            t: ts for t, ts in self._arb_ob_cooldown.items()
            if (now - ts).total_seconds() < self._ARB_OB_COOLDOWN_SECONDS
        }

        log.info(
            "STRIKE ARB candidate: %s %s | mkt=%.3f fair≥%.3f edge=+%.3f | %s",
            opp.ticker, opp.side.upper(),
            opp.market_price, opp.fair_value, opp.edge, opp.notes,
        )

        if not self._sizer.can_trade():
            log.debug("Strike arb: safety limits — skip")
            return

        # Sanity-check: don't trade if price has moved since scan
        ob  = self._ob_cache.get(opp.ticker)
        if ob and ob.best_yes_bid:
            live_price = float(ob.best_yes_bid)
            # Skip if live price already converged within 1¢ of fair value
            if live_price >= opp.fair_value - 0.01:
                log.debug("Strike arb: %s already converged (live=%.3f) — skip", opp.ticker, live_price)
                return
            price_dec = Decimal(str(round(live_price, 2)))
        else:
            price_dec = Decimal(str(round(opp.market_price, 2)))

        # Same-ticker guard: never stack a second position in the exact same market.
        # The cooldown prevents re-entry for 5 min, but if the position from a
        # previous fill is still open (waiting for settlement), we must not open
        # another one — otherwise exposure stacks up indefinitely.
        # Also check the hedge leg ticker — we don't want to hold the same lo-strike
        # from a previous arb pair and accidentally double up the NO side.
        open_tickers = {pos.get("ticker") for pos in self._open_positions.values()}
        if opp.ticker in open_tickers:
            log.debug("Strike arb: already hold %s — skip", opp.ticker)
            return
        if opp.hedge_ticker and opp.hedge_ticker in open_tickers:
            log.debug("Strike arb: already hold hedge leg %s — skip", opp.hedge_ticker)
            return

        # Correlation guard: skip if we already hold a position in this series
        ticker_category = detect_category(opp.ticker)
        if ticker_category not in ("GENERIC",):
            open_cats = {
                detect_category(pos.get("ticker", ""))
                for pos in self._open_positions.values()
            }
            if ticker_category in open_cats:
                log.debug("Strike arb: already in %s — skip", ticker_category)
                return

        # Dynamic minimum edge: when fills are coming fast the market is liquid
        # and we can afford to be selective — raise the bar to 5¢.
        effective_min_edge = _MIN_EDGE
        if len(self._fill_times) >= 3:
            avg_fill = sum(self._fill_times[-self._cfg.fill_speed_window:]) / min(len(self._fill_times), self._cfg.fill_speed_window)
            if avg_fill < self._cfg.fast_fill_threshold_sec:
                effective_min_edge = self._cfg.fast_fill_min_edge
                log.debug(
                    "Strike arb: hot market (avg fill=%.1fs) — min edge raised to %.2f",
                    avg_fill, effective_min_edge,
                )
        if opp.edge < effective_min_edge:
            log.debug(
                "Strike arb: %s edge=%.3f below dynamic min=%.3f — skip",
                opp.ticker, opp.edge, effective_min_edge,
            )
            return

        # Premium arb: qualifies for the extended 30% exposure zone if the edge
        # is large (≥6¢) or the opportunity is fast-settling (high edge/day).
        is_premium_arb = (
            opp.edge >= self._cfg.premium_arb_min_edge or
            opp.edge_per_day >= self._cfg.premium_arb_min_epd
        )

        # Kelly sizing — fair_value is our best estimate of where this should trade
        arb_win_prob = min(0.97, max(0.52, opp.fair_value))
        stake = self._sizer.compute_stake(
            win_probability = arb_win_prob,
            price_dollars   = price_dec,
            label           = f"arb-{opp.ticker}",
            use_premium_cap = is_premium_arb,
        )
        if stake <= Decimal("0"):
            log.debug("Strike arb: Kelly no edge — skip")
            return

        contracts  = self._sizer.contracts_for_stake(stake, price_dec)
        cost_basis = price_dec * contracts

        if cost_basis > self._sizer._exposure_headroom(is_premium_arb):
            log.debug("Strike arb: exposure cap — skip")
            return

        arb_conf = min(0.90, opp.edge * 12)   # 5¢ edge → 60% conf

        # ── Compute live taker ask prices for both legs ───────────────────────
        # hi_yes_ask = cost to buy hi_strike YES immediately
        #            = 1 − best_no_bid on hi_strike (NO sellers define the YES ask)
        # lo_no_ask  = cost to buy lo_strike NO immediately
        #            = 1 − best_yes_bid on lo_strike (YES buyers define the NO ask)
        hi_ob = self._ob_cache.get(opp.ticker)
        lo_ob = self._ob_cache.get(opp.hedge_ticker) if opp.hedge_ticker else None

        # ── Liquidity gate: require confirmed resting counterparties on both sides
        # before placing either order.  Without this, one leg fills and the other
        # rests indefinitely — breaking the locked-arb structure.
        #
        # hi_ob.best_no_bid: someone is actively bidding NO on hi_strike,
        #   meaning a YES seller already exists for us to cross against.
        # lo_ob.best_yes_bid: someone is actively bidding YES on lo_strike,
        #   meaning a NO seller already exists for us to cross against.
        #
        # If either is absent the arb is not executable right now — skip.
        # (lo_strike may simply not be subscribed in the WS feed yet, in which
        #  case lo_ob is None and we skip cleanly rather than guessing.)
        hi_has_liq = hi_ob is not None and hi_ob.best_no_bid is not None
        lo_has_liq = lo_ob is not None and lo_ob.best_yes_bid is not None

        if not hi_has_liq or not lo_has_liq:
            log.debug(
                "Strike arb: no OB liquidity — cannot execute "
                "(hi_no_bid=%s lo_yes_bid=%s) — subscribing missing legs",
                hi_ob.best_no_bid if hi_ob else "no_snapshot",
                lo_ob.best_yes_bid if lo_ob else "no_snapshot",
            )
            # Subscribe any missing leg so WS-triggered detection can fire
            # the moment a price update arrives — next OB event will re-check.
            if not hi_has_liq and self._feed:
                await self._feed.ensure_ticker_subscribed(opp.ticker)
            if not lo_has_liq and self._feed and opp.hedge_ticker:
                await self._feed.ensure_ticker_subscribed(opp.hedge_ticker)
            # Short cooldown — retries in 60s so we react quickly when quotes arrive.
            self._arb_ob_cooldown[opp.ticker] = datetime.datetime.now(datetime.timezone.utc)
            return

        hi_yes_ask = float(Decimal("1.00") - hi_ob.best_no_bid)
        lo_no_ask  = float(Decimal("1.00") - lo_ob.best_yes_bid)
        combined_taker = hi_yes_ask + lo_no_ask

        # ── Spread check: taker is only worthwhile if the arb survives the spread.
        # Require < 0.99 (≥ 1¢ net locked profit after crossing both spreads).
        if combined_taker >= 0.99:
            log.info(
                "Strike arb: spread too wide — combined taker cost=%.3f ≥ 0.99, "
                "edge=%.3f does not survive spread — skipping %s",
                combined_taker, opp.edge, opp.ticker,
            )
            return

        # ── Re-size contracts against the actual taker cost per pair ──────────
        hi_taker_dec  = Decimal(str(round(min(hi_yes_ask, 0.99), 2)))
        lo_taker_dec  = Decimal(str(round(min(lo_no_ask,  0.99), 2)))
        cost_per_pair = hi_taker_dec + lo_taker_dec   # guaranteed < $1.00

        headroom = self._sizer._exposure_headroom(is_premium_arb)
        max_by_headroom = int(headroom / cost_per_pair) if cost_per_pair > 0 else 0
        contracts = min(contracts, max_by_headroom)
        if contracts < 1:
            log.debug(
                "Strike arb: headroom $%.2f < cost_per_pair $%.2f — skip",
                float(headroom), float(cost_per_pair),
            )
            return

        # Stamp cooldown here — all gates passed, we are about to place orders.
        # Moved past the liquidity gate deliberately: if OB data was simply
        # absent (WS not yet received a snapshot for that market), we no longer
        # lock ourselves out for 5 minutes.  The ghost-order loop is still
        # prevented because the cooldown covers the actual placement attempt.
        self._arb_cooldown[opp.ticker] = datetime.datetime.now(datetime.timezone.utc)

        hi_cost    = hi_taker_dec * contracts
        lo_cost    = lo_taker_dec * contracts

        # ── Concurrent taker placement — both legs hit the exchange together ──
        primary_order, hedge_order = await asyncio.gather(
            self._orders.place_taker_order(
                ticker    = opp.ticker,
                side      = Side.YES,
                price     = hi_taker_dec,
                contracts = contracts,
                cost_basis = hi_cost,
            ),
            self._orders.place_taker_order(
                ticker    = opp.hedge_ticker,
                side      = Side.NO,
                price     = lo_taker_dec,
                contracts = contracts,
                cost_basis = lo_cost,
            ),
        )

        # ── Orphaned-leg guard ────────────────────────────────────────────────
        # If only one side was accepted, the arb is broken — we'd hold a naked
        # directional position with no hedge.  Cancel the accepted leg immediately
        # before registering any position.  (The cancel may arrive after a fill
        # if the taker matched instantly; in that case cancel_order returns True
        # but the fill watcher will still see FILLED — we accept that residual.)
        if bool(primary_order) != bool(hedge_order):
            orphan = primary_order or hedge_order
            log.warning(
                "Two-leg: only one leg accepted (%s %s) — cancelling immediately "
                "to avoid naked exposure on %s",
                orphan.side.value.upper(), orphan.ticker, opp.ticker,
            )
            await self._orders.cancel_order(orphan.kalshi_id)
            return

        if not primary_order and not hedge_order:
            log.debug("Two-leg: both orders rejected — no position taken")
            return

        # ── Both legs accepted — register and watch ───────────────────────────
        now_ts = time.time()
        for order, tkr, side_e, ep, cb, side_str in [
            (primary_order, opp.ticker,       Side.YES, hi_taker_dec, hi_cost, "yes"),
            (hedge_order,   opp.hedge_ticker, Side.NO,  lo_taker_dec, lo_cost, "no"),
        ]:
            self._order_placed_at[order.kalshi_id] = now_ts
            pos = {
                "ticker":              tkr,
                "side":                side_e,
                "entry_price":         ep,
                "contracts":           contracts,
                "cost_basis":          cb,
                "confidence":          arb_conf,
                "win_prob":            1.0,   # guaranteed at settlement if both fill
                "whales_contributing": [],
            }
            self._open_positions[order.kalshi_id] = pos
            self._state.upsert_position(order.kalshi_id, {
                **pos,
                "side":        side_e.value,
                "entry_price": str(ep),
                "cost_basis":  str(cb),
            })
            self._sizer.register_open(cb)
            self._logger.log_order(
                ticker      = tkr,
                side        = side_str,
                order_id    = order.kalshi_id,
                entry_price = float(ep),
                contracts   = contracts,
                cost        = float(cb),
                confidence  = arb_conf,
                win_prob    = 1.0,
                chase_cents = 0,
                post_only   = False,
            )
            asyncio.create_task(
                self._watch_order(order.kalshi_id, cb),
                name=f"watch-arb-taker-{order.kalshi_id}",
            )

        locked_profit_pct = (1.0 - float(cost_per_pair)) * 100
        log.info(
            "TWO-LEG TAKER ARB LOCKED: %s YES@%.2f + %s NO@%.2f | "
            "combined=$%.3f/pair × %d = $%.2f | guaranteed_profit=%.1f%%",
            opp.ticker,       float(hi_taker_dec),
            opp.hedge_ticker, float(lo_taker_dec),
            float(cost_per_pair), contracts, float(hi_cost + lo_cost),
            locked_profit_pct,
        )

    # ── portfolio sync ────────────────────────────────────────────────────────

    async def _portfolio_sync(self) -> None:
        """
        On startup: register exposure for state-recovered positions, then
        fetch live resting orders from Kalshi to catch anything not in state.

        This fixes the bug where the bot restarted with $0 tracked exposure
        even though it had open positions, causing over-exposure on new trades.
        """
        # Step 1: register exposure for positions already recovered from state file
        recovered_exposure = Decimal("0")
        for oid, pos in self._open_positions.items():
            try:
                cb = Decimal(str(pos.get("cost_basis", 0)))
                if cb > 0:
                    self._sizer.register_open(cb)
                    recovered_exposure += cb
            except Exception:
                pass
        if recovered_exposure > 0:
            log.info(
                "Portfolio sync: registered $%.2f exposure from %d recovered positions",
                float(recovered_exposure), len(self._open_positions),
            )

        # Step 2: fetch live resting orders from Kalshi — catches orders placed
        # before a crash/restart that didn't make it into the state file
        try:
            resp = await self._sess.get(
                "/portfolio/orders", params={"status": "resting", "limit": "100"}
            )
            live_orders = resp.get("orders") or []
            new_from_api = 0
            for od in live_orders:
                kalshi_id = od.get("order_id") or od.get("id", "")
                ticker    = od.get("ticker", "")
                if not kalshi_id or not ticker or kalshi_id in self._open_positions:
                    continue
                side_str    = od.get("side", "yes")
                qty         = int(od.get("remaining_count") or od.get("quantity") or 0)
                price_cents = od.get("yes_price", 0) if side_str == "yes" else od.get("no_price", 0)
                price_dec   = Decimal(str(price_cents)) / 100
                cb          = price_dec * qty
                if cb <= 0:
                    continue
                self._sizer.register_open(cb)
                self._open_positions[kalshi_id] = {
                    "ticker":              ticker,
                    "side":                Side.YES if side_str == "yes" else Side.NO,
                    "entry_price":         price_dec,
                    "contracts":           qty,
                    "cost_basis":          cb,
                    "confidence":          0.5,
                    "win_prob":            0.5,
                    "whales_contributing": [],
                }
                log.info(
                    "Portfolio sync: found live resting order %s on %s "
                    "($%.2f, %d contracts) — tracking",
                    kalshi_id, ticker, float(cb), qty,
                )
                new_from_api += 1
            if new_from_api:
                log.info("Portfolio sync: added %d orders from live Kalshi API", new_from_api)
        except Exception as exc:
            log.warning("Portfolio sync: could not fetch live orders: %s", exc)

        # Step 3: fetch live FILLED positions from Kalshi portfolio.
        # This catches positions filled in a previous session that never made it
        # into bot_state.json — the root cause of the same-ticker double-entry bug.
        # Without this, the bot restarts with no knowledge of existing holdings
        # and will happily place duplicate orders on markets it already holds.
        try:
            resp = await self._sess.get(
                "/portfolio/positions", params={"limit": "100"}
            )
            live_positions = resp.get("market_positions") or resp.get("positions") or []
            new_from_positions = 0
            known_tickers = {
                pos.get("ticker") for pos in self._open_positions.values()
            }
            for lp in live_positions:
                ticker    = lp.get("ticker", "")
                qty       = int(lp.get("position", 0) or lp.get("quantity", 0) or 0)
                if not ticker or qty == 0 or ticker in known_tickers:
                    continue
                # Kalshi returns positive qty for YES, negative for NO
                side_str  = "yes" if qty > 0 else "no"
                qty       = abs(qty)
                # Use market_exposure or cost as price proxy — Kalshi may provide avg price
                avg_price_raw = (
                    lp.get("market_exposure") or
                    lp.get("average_price") or
                    lp.get("cost")
                )
                if avg_price_raw is not None:
                    try:
                        price_dec = Decimal(str(avg_price_raw))
                        if price_dec > 1:
                            price_dec = price_dec / 100
                    except Exception:
                        price_dec = Decimal("0.50")
                else:
                    price_dec = Decimal("0.50")   # conservative fallback
                cb = price_dec * qty
                # Use ticker as synthetic position ID (no order_id for external fills)
                synthetic_id = f"recovered-pos-{ticker}"
                self._sizer.register_open(cb)
                self._open_positions[synthetic_id] = {
                    "ticker":              ticker,
                    "side":                Side.YES if side_str == "yes" else Side.NO,
                    "entry_price":         price_dec,
                    "contracts":           qty,
                    "cost_basis":          cb,
                    "confidence":          0.5,
                    "win_prob":            0.5,
                    "whales_contributing": [],
                }
                known_tickers.add(ticker)
                log.warning(
                    "Portfolio sync: UNKNOWN filled position recovered — %s %s "
                    "%d contracts @ ~$%.2f (cost ~$%.2f) — tracking for settlement",
                    ticker, side_str.upper(), qty, float(price_dec), float(cb),
                )
                asyncio.create_task(_send_telegram(
                    f"⚠️ Unknown position recovered: {ticker} {side_str.upper()} "
                    f"{qty} contracts @ ~${float(price_dec):.2f}"
                ))
                new_from_positions += 1
            if new_from_positions:
                log.warning(
                    "Portfolio sync: recovered %d unknown filled positions from Kalshi — "
                    "these were filled in a previous session not captured in bot_state.json",
                    new_from_positions,
                )
        except Exception as exc:
            log.warning("Portfolio sync: could not fetch live positions: %s", exc)

        log.info(
            "Portfolio sync complete — %d positions tracked",
            len(self._open_positions),
        )

    # ── quick flip detection ──────────────────────────────────────────────────

    async def _check_quick_flip(self, event: WhaleEvent) -> bool:
        """
        Detect if this whale event opposes a position we recently entered.

        If a large opposing trade arrives within QUICK_FLIP_WINDOW_SECONDS of
        our entry, the original signal is likely invalid — cancel any pending
        orders on this ticker so we don't get filled into a losing position.

        Returns True if a flip was detected (caller should abort signal processing).
        """
        entry = self._position_entry_time.get(event.ticker)
        if entry is None:
            return False

        entry_time, entry_side = entry
        elapsed = time.time() - entry_time

        if elapsed > self._QUICK_FLIP_WINDOW_SECONDS:
            self._position_entry_time.pop(event.ticker, None)
            return False

        if event.side == entry_side:
            return False   # same direction — reinforces our signal, not a flip

        log.warning(
            "QUICK FLIP detected: %s — opposing %s whale ($%.0f) arrived %.0fs "
            "after our %s entry — cancelling pending orders",
            event.ticker, event.side.upper(), float(event.notional_usd),
            elapsed, entry_side.upper(),
        )

        # Cancel any pending (not yet filled) orders on this ticker
        cancelled = 0
        for order in list(self._orders.open_orders()):
            pos = self._open_positions.get(order.kalshi_id, {})
            if pos.get("ticker") == event.ticker and order.status != OrderStatus.FILLED:
                try:
                    await self._orders.cancel_order(order.kalshi_id)
                    cancelled += 1
                except Exception as exc:
                    log.debug("Quick flip: cancel failed for %s: %s", order.kalshi_id, exc)

        if cancelled:
            log.info("Quick flip: cancelled %d pending orders on %s", cancelled, event.ticker)

        # Clear so repeated opposing trades don't keep triggering
        self._position_entry_time.pop(event.ticker, None)
        return True

    # ── category P&L persistence ──────────────────────────────────────────────

    def _load_category_pnl(self) -> None:
        """Load category P&L from disk so performance memory survives restarts."""
        try:
            if os.path.exists(_CATEGORY_PNL_FILE):
                with open(_CATEGORY_PNL_FILE) as f:
                    data = json.load(f)
                for cat, stats in data.items():
                    self._category_pnl[cat]    = stats.get("pnl", 0.0)
                    self._category_trades[cat] = stats.get("trades", 0)
                    self._category_wins[cat]   = stats.get("wins", 0)
                log.info(
                    "Category P&L loaded: %d categories | lifetime P&L $%+.2f across %d trades",
                    len(data),
                    sum(v.get("pnl", 0) for v in data.values()),
                    sum(v.get("trades", 0) for v in data.values()),
                )
        except Exception as exc:
            log.warning("Could not load category P&L file: %s", exc)

    def _save_category_pnl(self) -> None:
        """Persist category P&L to disk after every settlement."""
        try:
            data = {
                cat: {
                    "pnl":    self._category_pnl[cat],
                    "trades": self._category_trades[cat],
                    "wins":   self._category_wins[cat],
                }
                for cat in self._category_trades
            }
            with open(_CATEGORY_PNL_FILE, "w") as f:
                json.dump(data, f, indent=2)
        except Exception as exc:
            log.warning("Could not save category P&L: %s", exc)

    # ── category Kelly multiplier ─────────────────────────────────────────────

    def _category_kelly_multiplier(self, category: str) -> float:
        """
        Scale Kelly stake based on the bot's own proven win rate in this category.
        Requires ≥5 settled trades — below that, returns 1.0 (no adjustment).

        Win rate ≥ 70%  → 1.20× (proven edge, deploy more)
        Win rate 60-70% → 1.00× (neutral)
        Win rate 50-60% → 0.80× (marginal, reduce)
        Win rate < 50%  → 0.60× (losing category, cut significantly)
        """
        n = self._category_trades.get(category, 0)
        if n < 5:
            return 1.0
        wins = self._category_wins.get(category, 0)
        wr   = wins / n
        if wr >= 0.70:
            return 1.20
        elif wr >= 0.60:
            return 1.00
        elif wr >= 0.50:
            return 0.80
        else:
            return 0.60

    # ── Telegram two-way control ──────────────────────────────────────────────

    async def _telegram_poll_loop(self) -> None:
        """
        Poll Telegram every 10 seconds for incoming commands.

        Supported commands:
          /pause     — stop processing new whale signals (orders already placed unaffected)
          /resume    — resume signal processing
          /positions — list current open positions
          /status    — balance, exposure, paused state
          /pnl       — category P&L summary
        """
        import aiohttp
        url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/getUpdates"
        last_update_id = 0

        while True:
            await asyncio.sleep(10)
            try:
                async with aiohttp.ClientSession() as s:
                    async with s.get(
                        url,
                        params={"offset": last_update_id + 1, "timeout": 5},
                        timeout=aiohttp.ClientTimeout(total=8),
                    ) as r:
                        if r.status != 200:
                            continue
                        data = await r.json()

                for update in data.get("result", []):
                    last_update_id = max(last_update_id, update["update_id"])
                    msg  = update.get("message", {})
                    text = msg.get("text", "").strip().lower()

                    if text == "/pause":
                        self._trading_paused = True
                        await _send_telegram("⏸ Trading PAUSED — new signals ignored until /resume")
                        log.info("Telegram: trading paused by user")

                    elif text == "/resume":
                        self._trading_paused = False
                        await _send_telegram("▶️ Trading RESUMED")
                        log.info("Telegram: trading resumed by user")

                    elif text == "/positions":
                        if not self._open_positions:
                            await _send_telegram("📭 No open positions")
                        else:
                            lines = ["📋 Open Positions:"]
                            for oid, pos in list(self._open_positions.items())[:10]:
                                ticker    = pos.get("ticker", oid[:8])
                                side      = pos.get("side", "")
                                side_str  = side.value.upper() if hasattr(side, "value") else str(side).upper()
                                entry     = float(pos.get("entry_price", 0))
                                contracts = pos.get("contracts", 0)
                                cost      = float(pos.get("cost_basis", 0))
                                lines.append(
                                    f"  {ticker} {side_str} {contracts}× "
                                    f"@ ${entry:.2f} (cost ${cost:.2f})"
                                )
                            await _send_telegram("\n".join(lines))

                    elif text == "/status":
                        try:
                            balance  = float(self._sizer.balance)
                            exposure = float(getattr(self._sizer, "open_exposure", 0))
                        except Exception:
                            balance, exposure = 0.0, 0.0
                        paused = "⏸ PAUSED" if self._trading_paused else "▶️ ACTIVE"
                        session_pnl = sum(self._category_pnl.values())
                        await _send_telegram(
                            f"🤖 Bot: {paused}\n"
                            f"💰 Balance: ${balance:.2f}\n"
                            f"📊 Exposure: ${exposure:.2f}\n"
                            f"📋 Positions: {len(self._open_positions)}\n"
                            f"📈 Session P&L: ${session_pnl:+.2f}"
                        )

                    elif text == "/pnl":
                        if not self._category_trades:
                            await _send_telegram("📊 No settled trades yet")
                        else:
                            total_pnl    = sum(self._category_pnl.values())
                            total_trades = sum(self._category_trades.values())
                            lines = [f"📊 P&L (total ${total_pnl:+.2f} / {total_trades} trades):"]
                            ranked = sorted(
                                self._category_pnl.items(), key=lambda x: x[1], reverse=True
                            )
                            for cat, pnl in ranked:
                                n   = self._category_trades[cat]
                                w   = self._category_wins[cat]
                                wr  = w / n if n > 0 else 0
                                lines.append(
                                    f"  {cat}: ${pnl:+.2f} ({w}/{n} = {wr:.0%})"
                                )
                            await _send_telegram("\n".join(lines))

            except asyncio.CancelledError:
                return
            except Exception as exc:
                log.debug("Telegram poll error: %s", exc)

    # ── early exit / position monitor ─────────────────────────────────────────

    async def _position_monitor_loop(self) -> None:
        """
        Re-evaluate open filled positions every 5 minutes against the fundamental model.

        Exit early when:
          1. Position is up ≥50% of entry price AND model has reversed against us
             (take profit while we can — model says we're likely to lose it back)
          2. Position is down ≥40% AND model has high confidence against us (≥0.55)
             (cut the loss — the signal has definitively failed)

        Only fires for categories with real fundamental models (conf ≥ 0.20).
        GENERIC markets have no model to reverse, so we never exit them early.
        """
        _CHECK_INTERVAL   = 300    # check every 5 minutes
        _PROFIT_THRESHOLD = 0.50   # exit if up 50%+ and model reversed
        _LOSS_THRESHOLD   = 0.40   # exit if down 40%+ and model strongly against

        await asyncio.sleep(90)    # brief warm-up — let fills settle first

        while True:
            await asyncio.sleep(_CHECK_INTERVAL)
            if not self._open_positions:
                continue

            for kalshi_id, pos in list(self._open_positions.items()):
                try:
                    ticker    = pos.get("ticker", "")
                    side      = pos.get("side", Side.YES)
                    side_val  = side.value if hasattr(side, "value") else str(side)
                    entry     = Decimal(str(pos.get("entry_price", 0)))
                    contracts = pos.get("contracts", 0)

                    if not ticker or contracts < 1 or entry <= 0:
                        continue

                    # Only exit positions that are actually filled and held
                    managed = self._orders._orders.get(kalshi_id)
                    if managed is not None and managed.status not in (
                        OrderStatus.FILLED,
                    ):
                        continue   # still pending — quick-flip detection handles those

                    # Get live price — OB cache first, REST fallback for quiet markets.
                    # Without the fallback, positions on thin/stale orderbooks are
                    # silently skipped and the stop-loss never fires — they bleed to zero.
                    ob = self._ob_cache.get(ticker)
                    current_price = None
                    if ob is not None:
                        current_price = ob.best_yes_bid if side_val == "yes" else ob.best_no_bid

                    if current_price is None:
                        # OB cache miss — hit REST for the current market price
                        try:
                            _mkt = await self._sess.get(f"/markets/{ticker}")
                            _yes_bid = _mkt.get("yes_bid_dollars") or _mkt.get("yes_bid")
                            _no_bid  = _mkt.get("no_bid_dollars")  or _mkt.get("no_bid")
                            if side_val == "yes" and _yes_bid is not None:
                                current_price = Decimal(str(_yes_bid))
                            elif side_val == "no" and _no_bid is not None:
                                current_price = Decimal(str(_no_bid))
                            if current_price is not None:
                                log.debug("Position monitor: REST fallback price for %s = $%.3f",
                                          ticker, float(current_price))
                        except Exception:
                            pass

                    if current_price is None:
                        continue   # genuinely no price available

                    entry_f   = float(entry)
                    current_f = float(current_price)
                    price_chg = (current_f - entry_f) / max(entry_f, 0.001)

                    # ── Hard floor stop-loss ──────────────────────────────────
                    # If price has collapsed to near-zero on a position entered
                    # above 20¢, it is statistically worthless — exit immediately
                    # without waiting for model confirmation.
                    # Saves ~80%+ of remaining value vs. holding to zero at settlement.
                    _FLOOR_EXIT_PRICE = 0.07   # below 7¢ = effectively dead
                    _FLOOR_ENTRY_MIN  = 0.18   # only applies if entry was meaningful
                    if current_f <= _FLOOR_EXIT_PRICE and entry_f >= _FLOOR_ENTRY_MIN:
                        exit_reason = (
                            f"floor_stop: price collapsed to ${current_f:.3f} "
                            f"(entered ${entry_f:.2f}) — exiting before worthless"
                        )
                        log.info(
                            "Position monitor FLOOR EXIT: %s %s | entry=%.2f current=%.3f "
                            "contracts=%d | %s",
                            ticker, side_val, entry_f, current_f, contracts, exit_reason,
                        )
                        await self._try_exit_position(kalshi_id, pos, current_price, exit_reason)
                        continue

                    # Current YES price for fundamental model
                    yes_price = current_f if side_val == "yes" else (1.0 - current_f)

                    close_time = (
                        self._selector._market_close_times.get(ticker)
                        if self._selector else None
                    )

                    from fundamental_model import get_fundamental_estimate
                    fundamental = get_fundamental_estimate(ticker, yes_price, close_time)

                    # Skip categories with no real model — p_fair just echoes market price
                    if fundamental.confidence < 0.20:
                        continue

                    # Model favors our side?
                    model_favors = (
                        (side_val == "yes" and fundamental.p_fair > 0.52) or
                        (side_val == "no"  and fundamental.p_fair < 0.48)
                    )

                    should_exit = False
                    exit_reason = ""

                    if (price_chg >= _PROFIT_THRESHOLD
                            and not model_favors
                            and fundamental.confidence >= 0.45):
                        should_exit = True
                        exit_reason = (
                            f"profit_take: up {price_chg:.0%}, model reversed "
                            f"(p_fair={fundamental.p_fair:.2f} conf={fundamental.confidence:.2f})"
                        )

                    elif (price_chg <= -_LOSS_THRESHOLD
                            and not model_favors
                            and fundamental.confidence >= 0.55):
                        should_exit = True
                        exit_reason = (
                            f"stop_loss: down {abs(price_chg):.0%}, model against us "
                            f"(p_fair={fundamental.p_fair:.2f} conf={fundamental.confidence:.2f})"
                        )

                    if should_exit:
                        log.info(
                            "Position monitor EXIT: %s %s | entry=%.2f current=%.2f "
                            "chg=%+.0f%% contracts=%d | %s",
                            ticker, side_val, entry_f, current_f,
                            price_chg * 100, contracts, exit_reason,
                        )
                        await self._try_exit_position(kalshi_id, pos, current_price, exit_reason)

                except Exception as exc:
                    log.debug("Position monitor error for %s: %s", kalshi_id, exc)

    async def _try_exit_position(
        self,
        kalshi_id:     str,
        pos:           dict,
        current_price: Decimal,
        reason:        str,
    ) -> None:
        """
        Exit an open position by placing an opposing taker order.
          YES position → buy NO (= sell YES)
          NO  position → buy YES (= sell NO)

        Uses the current best ask so the order fills immediately.
        """
        ticker    = pos.get("ticker", "")
        side      = pos.get("side", Side.YES)
        side_val  = side.value if hasattr(side, "value") else str(side)
        contracts = pos.get("contracts", 1)
        entry     = Decimal(str(pos.get("entry_price", 0)))

        ob = self._ob_cache.get(ticker)

        if side_val == "yes":
            exit_side = Side.NO
            # NO taker price = 1 - best_yes_bid (cross the spread to sell immediately)
            yes_bid = ob.best_yes_bid if ob else current_price
            exit_price = (Decimal("1.00") - yes_bid) if yes_bid else (Decimal("1.00") - current_price)
        else:
            exit_side = Side.YES
            no_bid  = ob.best_no_bid if ob else current_price
            exit_price = (Decimal("1.00") - no_bid) if no_bid else (Decimal("1.00") - current_price)

        exit_price = max(Decimal("0.01"), min(Decimal("0.99"), exit_price))
        exit_cost  = exit_price * contracts

        log.info(
            "Exit attempt: %s — %s × %d @ $%.2f (reason: %s)",
            ticker, exit_side.value.upper(), contracts, float(exit_price), reason,
        )

        try:
            order = await self._orders.place_taker_order(
                ticker     = ticker,
                side       = exit_side,
                price      = exit_price,
                contracts  = contracts,
                cost_basis = exit_cost,
            )
            if order:
                log.info("Exit order placed for %s: %s", ticker, order.kalshi_id)
                asyncio.create_task(_send_telegram(
                    f"🚪 EXIT: {ticker} {side_val.upper()}\n"
                    f"Reason: {reason}\n"
                    f"Entry: ${float(entry):.2f} | Exit @ ${float(exit_price):.2f} "
                    f"| {contracts} contracts"
                ))
            else:
                log.warning("Exit order rejected for %s", ticker)
        except Exception as exc:
            log.warning("Exit attempt failed for %s: %s", ticker, exc)

    # ── category P&L ─────────────────────────────────────────────────────────

    def _log_category_pnl(self) -> None:
        """Log a session P&L summary grouped by market category."""
        if not self._category_trades:
            return
        total_pnl = sum(self._category_pnl.values())
        total_trades = sum(self._category_trades.values())
        log.info("─── Category P&L | session total: $%+.2f across %d trades ───",
                 total_pnl, total_trades)
        ranked = sorted(self._category_pnl.items(), key=lambda x: x[1], reverse=True)
        for cat, pnl in ranked:
            n    = self._category_trades[cat]
            wins = self._category_wins[cat]
            wr   = wins / n if n > 0 else 0.0
            log.info(
                "  %-22s  %+.2f   %d trade%s  %.0f%% win",
                cat, pnl, n, "s" if n != 1 else "", wr * 100,
            )

    async def _sync_balance(self) -> None:
        try:
            data = await self._gate.get_balance(self._sess)
            # Kalshi v2 API returns balance as integer cents in "balance" field.
            # e.g. $500.00 is returned as 50000 (cents).
            # Some environments also return "balance_dollars" as a string — handle both.
            raw_balance = data.get("balance")
            balance_dollars_str = data.get("balance_dollars")
            if balance_dollars_str is not None:
                # Explicit dollars string — use directly
                balance_val = Decimal(str(balance_dollars_str))
            elif raw_balance is not None:
                # Integer cents — divide by 100
                balance_val = Decimal(str(raw_balance)) / 100
            else:
                log.warning("Balance sync: no balance field in response: %s", data)
                balance_val = self._sizer.balance  # keep current
            self._sizer.sync_balance(balance_val)
            log.info("Balance: $%.2f | %s", self._sizer.balance, self._sizer)
        except Exception as exc:
            log.error("Balance sync failed: %s", exc)
