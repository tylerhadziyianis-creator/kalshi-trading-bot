"""
whale_registry.py — Whale Tracking, Multi-Category Convergence & Performance Decay

Data Model
──────────
• WhaleProfile stores a rolling deque of trade results (timestamp, correct).
• Weight starts at 1.0 and decays to DECAY_MULTIPLIER × weight when 7-day
  win rate < DECAY_WINRATE.  It recovers slowly (+5% per decay_pass).
• Weight floor = 0.10 — a whale is never fully silenced, just quieted.

Convergence Logic
─────────────────
When ≥2 whales from *different* categories bet the same side on the same
market, the composite confidence score is multiplied by CONVERGENCE_BOOST (2×).

The intuition: an Economics whale and a Geopolitics whale agreeing on an
oil-price event carries more signal than two whales from the same category
(which might reflect shared information or correlated bias).

Signal Construction
───────────────────
Confidence ∈ [0, 1]:
    raw  = |yes_weight − no_weight| / total_weight
    conf = min(1.0, raw × convergence_multiplier)

Win probability passed to Kelly:
    p_win = clip(market_price + conf × 0.15, 0.01, 0.97)

This is deliberately conservative — we don't fully trust whale signals.
"""
from __future__ import annotations

import asyncio
import logging
from collections import defaultdict, deque
from dataclasses import dataclass, field
from datetime import datetime, timezone
from decimal import Decimal
from enum import Enum
from typing import Optional

from config import BotConfig
from whale_watchlist import WatchlistManager

log = logging.getLogger(__name__)


# ═══════════════════════════════════════════════════════════════════════════════
#  Enums & Value Objects
# ═══════════════════════════════════════════════════════════════════════════════

class WhaleCategory(str, Enum):
    ECONOMICS   = "Economics"
    GEOPOLITICS = "Geopolitics"
    CRYPTO      = "Crypto"
    POLITICS    = "Politics"
    SPORTS      = "Sports"
    SCIENCE     = "Science"
    WEATHER     = "Weather"
    UNKNOWN     = "Unknown"


@dataclass
class TradeRecord:
    market_ticker: str
    side:          str          # "yes" | "no"
    price_dollars: Decimal      # yes_price_dollars or no_price_dollars (v2 string field)
    notional_usd:  Decimal      # price × contracts
    contracts:     int
    timestamp:     datetime


@dataclass
class MarketSignal:
    market_ticker:       str
    market_category:     WhaleCategory # New: Category of the market
    side:                str
    base_price:          Decimal     # average whale entry price (dollars)
    raw_confidence:      float       # pre-convergence
    final_confidence:    float       # post-convergence + decay
    win_probability:     float       # for Kelly Criterion input
    contributing_whales: list[str]
    categories_present:  set[WhaleCategory]
    convergence_applied: bool


# ═══════════════════════════════════════════════════════════════════════════════
#  Whale Profile
# ═══════════════════════════════════════════════════════════════════════════════

@dataclass
class WhaleProfile:
    whale_id:  str
    category:  WhaleCategory        = WhaleCategory.UNKNOWN
    weight:    float                = 1.0
    # (timestamp, was_correct) rolling result log
    results:   deque                = field(default_factory=lambda: deque(maxlen=500))
    trades:    deque                = field(default_factory=lambda: deque(maxlen=500))
    clv_data:  deque                = field(default_factory=lambda: deque(maxlen=500))
    # Per-category performance tracking: {"CRYPTO": {"resolved": 10, "wins": 7}, ...}
    category_stats: dict            = field(default_factory=dict)

    def add_result(
        self,
        correct:       bool,
        ts:            datetime,
        entry_price:   Decimal,
        closing_price: Decimal,
        market_cat:    str = "GENERIC",
    ) -> None:
        self.results.append((ts, correct))
        self.clv_data.append((ts, entry_price, closing_price))
        # Update per-category stats
        if market_cat not in self.category_stats:
            self.category_stats[market_cat] = {"resolved": 0, "wins": 0}
        self.category_stats[market_cat]["resolved"] += 1
        if correct:
            self.category_stats[market_cat]["wins"] += 1

    def category_win_rate(self, cat: str) -> Optional[float]:
        """Win rate for a specific market category. None if insufficient data."""
        stats = self.category_stats.get(cat, {})
        resolved = stats.get("resolved", 0)
        if resolved < 3:
            return None
        return stats.get("wins", 0) / resolved

    def win_rate(self, lookback_days: int) -> Optional[float]:
        """Overall win rate. Returns None if < 5 resolved results in window."""
        cutoff  = datetime.now(timezone.utc).timestamp() - lookback_days * 86_400
        recent  = [c for ts, c in self.results if ts.timestamp() >= cutoff]
        if len(recent) < 5:
            return None
        return sum(recent) / len(recent)

    def overall_resolved(self) -> int:
        return len(self.results)

    def category_summary(self) -> str:
        parts = []
        for cat, stats in self.category_stats.items():
            r = stats["resolved"]
            w = stats["wins"]
            parts.append(f"{cat}:{w}/{r}")
        return " | ".join(parts) if parts else "no_history"


# ═══════════════════════════════════════════════════════════════════════════════
#  Whale Registry
# ═══════════════════════════════════════════════════════════════════════════════

class WhaleRegistry:
    """
    Thread-safe (asyncio.Lock) registry mapping whale_id → WhaleProfile.

    Public API
    ──────────
    observe_trade(whale_id, trade)       → Optional[MarketSignal]
    record_result(whale_id, correct)     → None  (call on market resolution)
    get_signal(ticker)                   → Optional[MarketSignal]
    decay_pass()                         → None  (call hourly)
    """

    # Map ticker prefix → broad category
    PREFIX_TO_CATEGORY: dict[str, WhaleCategory] = {
        "KXBTCD":  WhaleCategory.CRYPTO,
        "KXETHD":  WhaleCategory.CRYPTO,
        "KXINX":   WhaleCategory.ECONOMICS,
        "KXFED":   WhaleCategory.ECONOMICS,
        "KXOIL":   WhaleCategory.ECONOMICS,
        "KXGEO":   WhaleCategory.GEOPOLITICS,
        "KXELEC":  WhaleCategory.POLITICS,
        "KXSCI":   WhaleCategory.SCIENCE,
        "KXWX":    WhaleCategory.WEATHER,
        "KXSPORT": WhaleCategory.SPORTS,
    }

    def __init__(self, cfg: BotConfig) -> None:
        self._cfg      = cfg
        self._whales:  dict[str, WhaleProfile]        = {}
        # market_ticker → [(whale_id, side, price_dollars, weight, timestamp)]
        self._positions: dict[str, list[tuple]]       = defaultdict(list)
        self._lock     = asyncio.Lock()
        # Watchlist for persistent performance tracking and weight overrides
        self._watchlist = WatchlistManager(
            path                  = cfg.watchlist_file,
            min_resolved_promote  = cfg.watchlist_min_resolved_promote,
            min_winrate_promote   = cfg.watchlist_min_winrate_promote,
            max_winrate_demote    = cfg.watchlist_max_winrate_demote,
            min_resolved_demote   = cfg.watchlist_min_resolved_demote,
            proven_boost          = cfg.watchlist_proven_boost,
            demoted_weight        = cfg.watchlist_demoted_weight,
        )
        self._watchlist.load()
        # Track last-seen timestamp per whale for eviction
        self._last_seen: dict[str, float] = {}

    # ── public ────────────────────────────────────────────────────────────────

    # Max age for a whale position to contribute to a signal (4 hours)
    _POSITION_TTL_SEC: int = 4 * 3600

    async def observe_trade(
        self, whale_id: str, trade: TradeRecord
    ) -> Optional[MarketSignal]:
        """
        Register a whale trade.  Returns a MarketSignal if confidence is
        non-zero, else None.  The caller checks confidence against its own
        threshold.
        """
        import time as _time
        async with self._lock:
            # Check watchlist — banned whales are silently ignored
            override = self._watchlist.get_weight_override(whale_id)
            if override == 0.0:
                log.debug("Watchlist: BANNED whale %s — ignoring trade", whale_id[:16])
                return None

            # Ensure watchlist entry exists (new whales auto-added as 'watching')
            self._watchlist.ensure_tracked(whale_id)

            whale = self._ensure_whale(whale_id, trade.market_ticker)
            whale.trades.append(trade)
            self._last_seen[whale_id] = _time.time()

            # Apply watchlist weight override
            if override is not None:
                effective_weight = whale.weight * override
            else:
                effective_weight = whale.weight

            # Store position with timestamp so we can expire stale entries
            self._positions[trade.market_ticker].append(
                (whale_id, trade.side, trade.price_dollars, effective_weight,
                 trade.timestamp.timestamp())
            )
            # Purge positions older than TTL before building signal
            self._purge_stale_positions(trade.market_ticker)
            return self._build_signal(trade.market_ticker)

    def _purge_stale_positions(self, ticker: str) -> None:
        """Remove whale positions older than _POSITION_TTL_SEC."""
        import time
        cutoff = time.time() - self._POSITION_TTL_SEC
        before = len(self._positions[ticker])
        self._positions[ticker] = [
            p for p in self._positions[ticker] if p[4] >= cutoff
        ]
        purged = before - len(self._positions[ticker])
        if purged:
            log.debug("Purged %d stale positions from %s", purged, ticker)

    async def record_result(
        self,
        whale_id:      str,
        market_ticker: str,
        correct:       bool,
        entry_price:   Decimal,
        closing_price: Decimal,
    ) -> None:
        from fundamental_model import detect_category
        market_cat = detect_category(market_ticker)
        async with self._lock:
            whale = self._whales.get(whale_id)
            if whale:
                whale.add_result(
                    correct, datetime.now(timezone.utc),
                    entry_price, closing_price,
                    market_cat=market_cat,
                )
                log.info(
                    "Whale result recorded: %s | ticker=%s | cat=%s | correct=%s | cat_stats=%s",
                    whale_id[:8], market_ticker, market_cat, correct, whale.category_summary(),
                )
            # Feed result into watchlist for persistent performance tracking
            self._watchlist.record_result(whale_id, market_cat, correct)
            # Auto-update watchlist statuses and save
            n_changes = self._watchlist.auto_update_status()
            if n_changes:
                self._watchlist.save()

            # Clean resolved position
            self._positions[market_ticker] = [
                p for p in self._positions[market_ticker] if p[0] != whale_id
            ]

    async def update_settlement_price(self, market_ticker: str, closing_price: Decimal) -> None:
        async with self._lock:
            # Iterate through all whales and their trades to find relevant ones
            for whale_id, whale in self._whales.items():
                # Find trades for this market that are still in the whale's trade history
                # and haven't had their closing price recorded yet.
                for i, (ts, correct, entry_price, old_closing_price) in enumerate(whale.results):
                    # Assuming entry_price is the whale's trade price for this market
                    # and old_closing_price is 0.0 if not yet settled
                    if old_closing_price == Decimal("0.0") and entry_price == closing_price: # Simplified check
                        whale.results[i] = (ts, correct, entry_price, closing_price)
                        log.debug("CLV: Whale %s | Market %s | Entry %s | Closing %s",
                                  whale_id, market_ticker, entry_price, closing_price)

    async def get_signal(self, market_ticker: str) -> Optional[MarketSignal]:
        async with self._lock:
            return self._build_signal(market_ticker)

    async def decay_pass(self) -> None:
        """Apply performance decay to all tracked whales and evict stale ones. Call hourly."""
        import time as _time
        async with self._lock:
            for whale in self._whales.values():
                self._apply_decay(whale)

            # Evict whale profiles not seen in WHALE_EVICTION_DAYS to prevent unbounded growth
            eviction_cutoff = _time.time() - self._cfg.whale_eviction_days * 86_400
            stale_ids = [
                wid for wid, ts in self._last_seen.items()
                if ts < eviction_cutoff and wid in self._whales
            ]
            for wid in stale_ids:
                del self._whales[wid]
                del self._last_seen[wid]

        if stale_ids:
            log.info(
                "Evicted %d stale whale profiles (inactive > %d days)",
                len(stale_ids), self._cfg.whale_eviction_days,
            )
        log.info(
            "Decay pass complete. %d whales tracked | watchlist: %s",
            len(self._whales), self._watchlist.count(),
        )
        # Persist watchlist after decay pass
        self._watchlist.save()

    def whale_count(self) -> int:
        return len(self._whales)

    def get_whale_profile(self, whale_id: str):
        """Public accessor for whale profile (used by signal_engine)."""
        return self._whales.get(whale_id)

    @property
    def watchlist(self) -> WatchlistManager:
        """Access the underlying watchlist manager (for bot_engine reporting)."""
        return self._watchlist

    # ── internals ─────────────────────────────────────────────────────────────

    def _ensure_whale(self, whale_id: str, ticker: str) -> WhaleProfile:
        if whale_id not in self._whales:
            cat = self._infer_category(ticker)
            self._whales[whale_id] = WhaleProfile(whale_id=whale_id, category=cat)
            log.debug("New whale: %s | category=%s", whale_id, cat.value)
        return self._whales[whale_id]

    def _infer_category(self, ticker: str) -> WhaleCategory:
        for prefix, cat in self.PREFIX_TO_CATEGORY.items():
            if ticker.upper().startswith(prefix):
                return cat
        return WhaleCategory.UNKNOWN

    def _apply_decay(self, whale: WhaleProfile) -> None:
        wr = whale.win_rate(self._cfg.whale_lookback_days)
        if wr is None:
            return

        # If whale hasn't met minimum sample size, effectively disable its weight
        if len(whale.results) < self._cfg.min_whale_sample_size:
            whale.weight = 0.0
            log.debug("WHALE_DISABLED %s | resolved_trades=%d < min_sample_size=%d",
                      whale.whale_id, len(whale.results), self._cfg.min_whale_sample_size)
            return
        if wr < self._cfg.decay_winrate:
            prev = whale.weight
            whale.weight = max(0.10, whale.weight * self._cfg.decay_multiplier)
            log.info(
                "DECAY %s | wr=%.1f%% < %.0f%% | weight %.3f → %.3f | cats: %s",
                whale.whale_id[:8], wr * 100, self._cfg.decay_winrate * 100,
                prev, whale.weight, whale.category_summary(),
            )
        else:
            whale.weight = min(1.0, whale.weight * 1.05)
            log.debug(
                "RECOVER %s | wr=%.1f%% | weight → %.3f | cats: %s",
                whale.whale_id[:8], wr * 100, whale.weight, whale.category_summary(),
            )

    def _build_signal(self, ticker: str) -> Optional[MarketSignal]:
        positions = self._positions.get(ticker)
        if not positions:
            return None

        yes_w, no_w = 0.0, 0.0
        yes_prices, no_prices = [], []
        yes_cats:  set[WhaleCategory] = set()
        no_cats:   set[WhaleCategory] = set()
        yes_ids,   no_ids   = [], []

        for entry in positions:
            whale_id, side, price, weight = entry[0], entry[1], entry[2], entry[3]
            cat = self._whales[whale_id].category
            if side == "yes":
                yes_w += weight
                yes_prices.append(price)
                yes_cats.add(cat)
                yes_ids.append(whale_id)
            else:
                no_w += weight
                no_prices.append(price)
                no_cats.add(cat)
                no_ids.append(whale_id)

        total = yes_w + no_w
        if total == 0:
            return None

        raw_conf = abs(yes_w - no_w) / total

        if yes_w >= no_w:
            dom_side, dom_prices, dom_cats, dom_ids = "yes", yes_prices, yes_cats, yes_ids
        else:
            dom_side, dom_prices, dom_cats, dom_ids = "no",  no_prices,  no_cats,  no_ids

        avg_price = sum(dom_prices) / len(dom_prices)

        # Convergence: 2+ distinct categories on dominant side
        convergence = len(dom_cats) >= 2
        # Calculate individual whale weights contributing to the signal
        contributing_whale_weights = [self._whales[wid].weight for wid in dom_ids]
        # If any contributing whale is disabled (weight 0), the signal is 0
        if any(w == 0.0 for w in contributing_whale_weights):
            final_conf = 0.0
        else:
            multiplier  = self._cfg.convergence_boost if convergence else 1.0
            final_conf  = min(1.0, raw_conf * multiplier)

        if convergence:
            log.info(
                "CONVERGENCE %s | side=%s | cats=%s | conf %.3f→%.3f",
                ticker, dom_side,
                {c.value for c in dom_cats}, raw_conf, final_conf,
            )

        price_f = float(avg_price)
        # Win probability calibration:
        # - Base: the current market price IS the implied probability (efficient market prior)
        # - Adjustment: confidence represents how much we think the whale knows above the crowd
        # - We apply a conservative 10% maximum adjustment, shrunk by (1 - price) to avoid
        #   pushing near-certain markets past 97¢, and shrunk further at low confidence
        # - This keeps p_win honest: at conf=0.5, we only nudge 5% above market price
        # - p_win is always clamped to [0.52, 0.97] — we never bet with <2% edge
        adjustment = final_conf * 0.10 * (1.0 - price_f)  # smaller adjustment near extremes
        p_win = max(0.52, min(0.97, price_f + adjustment))

        return MarketSignal(
            market_ticker       = ticker,
            market_category     = self._infer_category(ticker),
            side                = dom_side,
            base_price          = Decimal(str(round(float(avg_price), 4))),
            raw_confidence      = raw_conf,
            final_confidence    = final_conf,
            win_probability     = p_win,
            contributing_whales = dom_ids,
            categories_present  = dom_cats,
            convergence_applied = convergence,
        )
