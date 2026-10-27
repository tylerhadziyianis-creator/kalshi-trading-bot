"""
ob_tracker.py — Order Book Imbalance Tracker

Derives two microstructure signals from the orderbook_delta stream:

  1. Spread compression
     spread = 1 - yes_bid - no_bid
     Tight spread (< 3¢) = liquid, efficient market — low signal
     Wide spread (> 10¢) = illiquid, expect larger moves
     Rapidly TIGHTENING spread = informed buyers stepping in

  2. Mid-price velocity
     mid = yes_bid (YES bid as proxy for mid-price)
     Direction and speed of recent mid-price moves = momentum signal

These are supplementary signals — they feed into the signal engine
alongside whale flow and the fundamental model. Neither alone is
sufficient to trade; combined they raise composite confidence.

Note: Kalshi's orderbook_delta only provides best bid prices, not
depth/quantities. True order book imbalance (bid qty vs ask qty) is
not available on the public WebSocket. These are the best proxies
achievable with available data.
"""
from __future__ import annotations

import logging
import time
from collections import deque
from dataclasses import dataclass
from decimal import Decimal
from typing import Optional

log = logging.getLogger(__name__)

_HISTORY_WINDOW:    int   = 300     # 5 minutes of price history
_MAX_HISTORY_TICKS: int   = 500     # cap memory per ticker
_MIN_TICKS:         int   = 5       # need at least 5 ticks for velocity
_SPREAD_WIDE:       float = 0.10    # > 10¢ = wide
_SPREAD_TIGHT:      float = 0.03    # < 3¢ = tight


@dataclass
class OBSignal:
    ticker:         str
    spread:         float   # current bid-ask spread (1 - yes_bid - no_bid)
    spread_z:       float   # spread vs recent average (positive = widening)
    mid_velocity:   float   # mid-price change per minute (positive = rising)
    mid_direction:  str     # "up" | "down" | "flat"
    confidence:     float   # 0.0–1.0


class OBTracker:
    """
    Tracks best-bid prices and derives microstructure signals.
    Feed every OrderbookSnapshot event from ws_feed into update().
    """

    def __init__(self) -> None:
        # ticker → deque of (ts, yes_bid, no_bid, spread)
        self._history: dict[str, deque] = {}

    # ── public API ────────────────────────────────────────────────────────────

    def update(self, ticker: str, yes_bid: Optional[Decimal], no_bid: Optional[Decimal]) -> None:
        """Ingest a new orderbook snapshot. Call on every OB event."""
        if yes_bid is None or no_bid is None:
            return

        yb     = float(yes_bid)
        nb     = float(no_bid)
        spread = max(0.0, 1.0 - yb - nb)
        now    = time.time()

        if ticker not in self._history:
            self._history[ticker] = deque(maxlen=_MAX_HISTORY_TICKS)

        self._history[ticker].append((now, yb, nb, spread))
        self._prune(ticker)

    def get_signal(self, ticker: str) -> Optional[OBSignal]:
        """
        Returns an OBSignal for the current microstructure state.
        Returns None if insufficient data.
        """
        hist = self._history.get(ticker)
        if not hist or len(hist) < _MIN_TICKS:
            return None

        recent = list(hist)
        latest = recent[-1]
        _, yes_bid_now, _, spread_now = latest

        # ── Spread analysis ───────────────────────────────────────────────────
        spreads   = [r[3] for r in recent]
        avg_spread = sum(spreads) / len(spreads)
        std_spread = (sum((s - avg_spread) ** 2 for s in spreads) / len(spreads)) ** 0.5
        spread_z   = (spread_now - avg_spread) / std_spread if std_spread > 0 else 0.0

        # ── Mid-price velocity ────────────────────────────────────────────────
        # Use yes_bid as mid-price proxy (YES bid ≈ mid if spread is tight)
        old_ts,  old_yb = recent[0][0],  recent[0][1]
        time_diff = latest[0] - old_ts
        if time_diff < 1.0:
            return None   # too little time elapsed

        velocity = (yes_bid_now - old_yb) / (time_diff / 60.0)  # per minute

        if abs(velocity) < 0.001:
            direction = "flat"
        elif velocity > 0:
            direction = "up"
        else:
            direction = "down"

        # ── Confidence ────────────────────────────────────────────────────────
        # Higher confidence when:
        # - spread is tightening (spread_z < -1: spread narrower than average)
        # - clear directional velocity
        # - sufficient history
        history_conf = min(1.0, len(recent) / 50)
        velocity_conf = min(1.0, abs(velocity) / 0.01)  # full conf at 1¢/min move
        spread_conf   = min(1.0, abs(spread_z) / 2.0)
        confidence    = (history_conf * 0.3 + velocity_conf * 0.4 + spread_conf * 0.3)

        return OBSignal(
            ticker        = ticker,
            spread        = spread_now,
            spread_z      = spread_z,
            mid_velocity  = velocity,
            mid_direction = direction,
            confidence    = confidence,
        )

    # ── internals ─────────────────────────────────────────────────────────────

    def _prune(self, ticker: str) -> None:
        cutoff = time.time() - _HISTORY_WINDOW
        hist   = self._history.get(ticker)
        if not hist:
            return
        while hist and hist[0][0] < cutoff:
            hist.popleft()
