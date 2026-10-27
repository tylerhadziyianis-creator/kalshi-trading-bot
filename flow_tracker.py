"""
flow_tracker.py — Rolling Directional Flow Imbalance Tracker

Tracks the directional bias of large trades per market over a rolling
time window. Strong imbalance (e.g. 80% of notional going NO) signals
that informed money has a collective directional view — even without
knowing who the individual traders are.

This is the primary edge available on Kalshi where trader IDs are
anonymous. It works because large trades aggregate into a measurable
directional signal regardless of individual identity.

Usage:
    tracker = FlowTracker()
    tracker.record("KXFED-26JUN-T3.75", "no", 1487.07)
    signal = tracker.get_signal("KXFED-26JUN-T3.75")
    if signal:
        # signal.side, signal.imbalance, signal.confidence
"""
from __future__ import annotations

import logging
import time
from collections import defaultdict
from dataclasses import dataclass
from typing import Optional

log = logging.getLogger(__name__)

_WINDOW_SECONDS:  int   = int(7200)     # 2-hour rolling window
_MIN_TOTAL_FLOW:  float = 500.0         # need at least $500 of flow
_MIN_IMBALANCE:   float = 0.65          # 65%+ one direction = signal
_FULL_CONF_FLOW:  float = 20_000.0      # $20k total flow = full volume confidence


@dataclass
class FlowSignal:
    ticker:      str
    side:        str       # dominant side: "yes" | "no"
    imbalance:   float     # fraction going dominant side (0.65–1.0)
    confidence:  float     # composite confidence (0.0–1.0)
    yes_flow:    float     # $ total YES notional in window
    no_flow:     float     # $ total NO notional in window
    trade_count: int       # number of trades in window
    window_sec:  int       # rolling window size used


class FlowTracker:
    """
    Maintains a rolling window of large-trade flow per market.
    Produces a FlowSignal when directional imbalance is strong enough.

    Thread/async safe for reads — record() is called from the event
    loop and get_signal() is called from the signal engine, both in
    the same asyncio event loop so no lock is needed.
    """

    def __init__(
        self,
        window_seconds: int   = _WINDOW_SECONDS,
        min_total_flow: float = _MIN_TOTAL_FLOW,
        min_imbalance:  float = _MIN_IMBALANCE,
    ) -> None:
        self._window    = window_seconds
        self._min_flow  = min_total_flow
        self._min_imb   = min_imbalance
        self._trades: dict[str, list[dict]] = defaultdict(list)

    # ── public API ────────────────────────────────────────────────────────────

    def record(self, ticker: str, side: str, notional: float) -> None:
        """Record a large trade. Call this for every whale/track event."""
        self._trades[ticker].append({
            "side":     side,
            "notional": float(notional),
            "ts":       time.time(),
        })
        self._prune(ticker)

    def get_signal(self, ticker: str) -> Optional[FlowSignal]:
        """
        Returns a FlowSignal if directional imbalance is strong enough.
        Returns None if flow is balanced, insufficient, or no data.
        """
        self._prune(ticker)
        trades = self._trades.get(ticker, [])
        if not trades:
            return None

        yes_flow = sum(t["notional"] for t in trades if t["side"] == "yes")
        no_flow  = sum(t["notional"] for t in trades if t["side"] == "no")
        total    = yes_flow + no_flow

        if total < self._min_flow:
            return None

        yes_frac = yes_flow / total
        no_frac  = no_flow  / total
        dominant = "yes" if yes_frac >= no_frac else "no"
        imbalance = max(yes_frac, no_frac)

        if imbalance < self._min_imb:
            return None

        # Confidence = blend of flow volume and imbalance strength
        # - vol_conf:       scales 0→1 as total flow grows to $20k
        # - imbalance_conf: scales 0→1 as imbalance grows from min to 100%
        vol_conf       = min(1.0, total / _FULL_CONF_FLOW)
        imbalance_conf = (imbalance - self._min_imb) / (1.0 - self._min_imb)
        confidence     = vol_conf * 0.40 + imbalance_conf * 0.60

        log.debug(
            "FlowTracker %s: dominant=%s imbalance=%.1f%% conf=%.3f "
            "yes=$%.0f no=$%.0f trades=%d",
            ticker, dominant, imbalance * 100, confidence,
            yes_flow, no_flow, len(trades),
        )

        return FlowSignal(
            ticker      = ticker,
            side        = dominant,
            imbalance   = imbalance,
            confidence  = confidence,
            yes_flow    = yes_flow,
            no_flow     = no_flow,
            trade_count = len(trades),
            window_sec  = self._window,
        )

    def summary(self) -> dict:
        """Current flow state for all tracked tickers (for logging/debug)."""
        out = {}
        for ticker in list(self._trades.keys()):
            sig = self.get_signal(ticker)
            if sig:
                out[ticker] = {
                    "side":       sig.side,
                    "imbalance":  round(sig.imbalance, 3),
                    "confidence": round(sig.confidence, 3),
                    "yes_flow":   round(sig.yes_flow, 2),
                    "no_flow":    round(sig.no_flow, 2),
                    "trades":     sig.trade_count,
                }
        return out

    # ── internals ─────────────────────────────────────────────────────────────

    def _prune(self, ticker: str) -> None:
        cutoff = time.time() - self._window
        self._trades[ticker] = [
            t for t in self._trades[ticker]
            if t["ts"] >= cutoff
        ]
        if not self._trades[ticker]:
            del self._trades[ticker]
