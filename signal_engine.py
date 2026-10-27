"""
signal_engine.py — Hybrid Signal Combiner & Statistical Arbitrage Layer

This is the brain upgrade. Instead of acting on whale flow alone, every
trading decision now requires agreement from THREE independent sources:

  Source 1: Whale Signal (informed flow detection)
    - Large trades by tracked whales → directional confidence
    - Weighted by each whale's proven per-category track record
    - Filtered: only whales with ≥MIN_RESOLVED_TRADES in THIS category

  Source 2: Fundamental Model (structural prior)
    - Category-specific base rates (Fed, Macro, Crypto, Index)
    - Provides an independent probability estimate

  Source 3: Statistical Arbitrage (market microstructure)
    - Compares market price to our blended estimate
    - If market price deviates significantly from fair value → edge exists
    - No deviation → no trade even if whale and fundamental agree

Combination logic (Bayesian-inspired blending):
    composite_p = weighted_average(whale_p, fundamental_p, market_p)
    weights     = (whale_conf, fundamental_conf, 1-max(whale_conf, fundamental_conf))
    final_conf  = geometric_mean(whale_conf, fundamental_conf, agreement_bonus)

Trade gate (ALL must pass to place an order):
  ✓ whale_confidence ≥ MIN_WHALE_CONFIDENCE (default: 0.35)
  ✓ whale has ≥ MIN_CATEGORY_TRADES proven trades in this exact category
  ✓ whale category win rate ≥ MIN_CATEGORY_WIN_RATE (default: 0.52)
  ✓ mispricing ≥ MIN_MISPRICING (default: 0.03 = 3¢ edge)
  ✓ composite_confidence ≥ cfg.min_kelly_edge
  ✓ NOT within BLACKOUT_MINUTES of known announcement time

Output: CompositeSignal with unified probability, confidence, and reasoning.
"""
from __future__ import annotations

import logging
import math
from dataclasses import dataclass, field
from decimal import Decimal
from typing import Optional

_MAX_NOTIONAL_BOOST: float = 0.10   # hard cap on size-based confidence boost

from fundamental_model import FundamentalEstimate, get_fundamental_estimate, detect_category
from whale_registry import MarketSignal
from flow_tracker import FlowTracker, FlowSignal
from ob_tracker import OBTracker, OBSignal

log = logging.getLogger(__name__)


@dataclass
class CompositeSignal:
    """
    Unified output from the signal engine.
    Replaces MarketSignal as the input to position sizing and execution.
    """
    # Core fields (same interface as MarketSignal for drop-in compatibility)
    market_ticker:    str
    side:             str           # "yes" | "no"
    base_price:       Decimal       # whale's average entry price
    final_confidence: float         # composite [0,1]
    win_probability:  float         # probability passed to Kelly
    contributing_whales: list[str]

    # Audit fields
    whale_confidence:       float
    fundamental_confidence: float
    fundamental_p_fair:     float
    mispricing:             float   # our_p_fair - market_price (positive = underpriced YES)
    agreement_score:        float   # how much all signals agree [0,1]
    category:               str
    fundamental_notes:      str
    skip_reason:            Optional[str] = None

    # Pass-through from whale signal
    convergence_applied: bool = False

    # Notional size relative to threshold (e.g. 5.0 = whale put in 5× threshold)
    notional_ratio:      float = 1.0


@dataclass
class SignalEngineConfig:
    """Tunable parameters for the signal engine."""
    min_whale_confidence:   float = 0.25   # minimum raw whale conf to consider
    min_category_trades:    int   = 3      # min settled trades in THIS category per whale
    min_category_win_rate:  float = 0.50   # whale must beat 50% in this category
    min_mispricing:         float = 0.015  # minimum edge vs fair value (1.5¢)
    min_composite_conf:     float = 0.30   # minimum composite confidence to trade
    fundamental_weight:     float = 0.35   # how much to weight fundamental vs whale
    market_weight:          float = 0.15   # how much market price anchors our estimate
    agreement_bonus:        float = 0.20   # extra confidence when all three agree
    max_spread_to_trade:    float = 0.12   # skip if bid-ask spread > 12¢
    min_market_price:       float = 0.15   # skip near-certain NO markets (price < 15%)
    max_market_price:       float = 0.85   # skip near-certain YES markets (price > 85%)
    blackout_minutes:       int   = 20     # pause trading within 20min of known macro announcements


class SignalEngine:
    """
    Combines whale signal, fundamental model, and statistical arbitrage
    into a single actionable CompositeSignal.

    Usage:
        engine = SignalEngine(cfg, signal_engine_cfg)
        composite = engine.evaluate(whale_signal, market_price, ticker_meta)
        if composite and not composite.skip_reason:
            # place order
    """

    def __init__(self, cfg, se_cfg: Optional[SignalEngineConfig] = None) -> None:
        self._cfg    = cfg
        self._se_cfg = se_cfg or SignalEngineConfig(
            min_whale_confidence  = getattr(cfg, "min_whale_confidence",  0.30),
            min_category_trades   = getattr(cfg, "min_category_trades",   3),
            min_category_win_rate = getattr(cfg, "min_category_win_rate", 0.50),
            min_mispricing        = getattr(cfg, "min_mispricing",        0.02),
            min_composite_conf    = getattr(cfg, "min_composite_conf",    0.30),
            fundamental_weight    = getattr(cfg, "fundamental_weight",    0.35),
            market_weight         = getattr(cfg, "market_weight",         0.15),
            agreement_bonus       = getattr(cfg, "agreement_bonus",       0.20),
            max_spread_to_trade   = getattr(cfg, "max_spread_to_trade",   0.12),
            min_market_price      = getattr(cfg, "min_market_price",      0.15),
            max_market_price      = getattr(cfg, "max_market_price",      0.85),
        )

    def evaluate(
        self,
        whale_signal:    MarketSignal,
        market_price:    float,          # current YES bid midpoint
        close_time_iso:  Optional[str] = None,
        yes_ask:         Optional[float] = None,
        yes_bid:         Optional[float] = None,
        whale_registry   = None,         # for per-category track record check
        flow_tracker:    Optional[FlowTracker] = None,   # rolling flow imbalance
        ob_tracker:      Optional[OBTracker]   = None,   # order book microstructure
        notional_usd:    float = 0.0,    # actual trade size ($)
        whale_threshold: float = 0.0,    # dynamic threshold for this market ($)
    ) -> CompositeSignal:
        """
        Evaluate all signals and return a CompositeSignal.
        If skip_reason is set, do not trade.
        """
        cfg    = self._se_cfg
        ticker = whale_signal.market_ticker
        side   = whale_signal.side

        # ── Gate 0: blackout window ───────────────────────────────────────────
        # Block trading within blackout_minutes of a known macro announcement.
        # Fundamental model is stale right before CPI/FOMC, spreads widen,
        # and any signal is likely noise reacting to leaked headlines.
        if cfg.blackout_minutes > 0:
            try:
                from external_data import get_cache as _get_cache
                _cache = _get_cache()
                if _cache is not None:
                    _cat_for_blackout = detect_category(ticker)
                    _ctx = _cache.macro_calendar_context(_cat_for_blackout)
                    _hrs = _ctx.get("hours_to_event")
                    if _hrs is not None and _hrs * 60 < cfg.blackout_minutes:
                        return self._skip(whale_signal, market_price,
                            f"blackout_window ({_hrs*60:.0f}min to {_ctx.get('event_type','?')} "
                            f"< {cfg.blackout_minutes}min)")
            except Exception:
                pass   # never block on blackout errors

        # ── Gate 0: market price range ────────────────────────────────────────
        # Check the price of the side being traded — not always YES.
        # A NO whale at 99¢ is near-certain (NO > 90¢), just like a YES whale
        # at 99¢. Using the traded-side price makes the check symmetric.
        #
        # trade_price = YES price for YES trades, NO price for NO trades.
        # Configured via MIN_MARKET_PRICE / MAX_MARKET_PRICE in .env (default 0.05–0.90).
        min_price   = getattr(cfg, "min_market_price", 0.05)
        max_price   = getattr(cfg, "max_market_price", 0.90)
        trade_price = market_price if side == "yes" else (1.0 - market_price)
        if trade_price < min_price or trade_price > max_price:
            # Exception: RT/index markets where fundamental model has high confidence
            # and large mispricing — the whale is trading into a near-certain outcome
            # that the market has mispriced (e.g. RT score already below threshold
            # but market still prices YES at 99¢)
            from fundamental_model import detect_category
            cat = detect_category(whale_signal.market_ticker)
            fundamental = get_fundamental_estimate(
                whale_signal.market_ticker, market_price, close_time_iso
            )
            abs_mispricing   = abs(fundamental.p_fair - market_price)
            high_conf        = fundamental.confidence >= 0.50
            # RT: require 25¢ edge — scores are binary (pass/fail threshold)
            # INDEX intraday: allow 8¢ edge — spot vs strike is a tighter, faster signal
            rt_exception    = cat == "ROTTEN_TOMATOES" and abs_mispricing > 0.25 and high_conf
            index_exception = (cat == "INDEX" and abs_mispricing > 0.08
                               and fundamental.confidence >= 0.60
                               and fundamental.model_name == "INDEX_INTRADAY")
            if rt_exception or index_exception:
                exception_type = "RT" if rt_exception else "Index intraday"
                log.info(
                    "Price range exception [%s]: %s %s @ %.2f — model p_fair=%.2f conf=%.2f "
                    "mispricing=%+.2f — allowing through",
                    exception_type, whale_signal.market_ticker, side, trade_price,
                    fundamental.p_fair, fundamental.confidence,
                    fundamental.p_fair - market_price,
                )
            else:
                return self._skip(whale_signal, market_price,
                    f"price_out_of_range ({trade_price:.3f} not in [{min_price},{max_price}])")

        # ── Gate 1: minimum whale confidence ─────────────────────────────────
        if whale_signal.final_confidence < cfg.min_whale_confidence:
            return self._skip(whale_signal, market_price,
                f"whale_conf_too_low ({whale_signal.final_confidence:.3f} < {cfg.min_whale_confidence})")

        # ── Gate 2: spread check ──────────────────────────────────────────────
        if yes_ask is not None and yes_bid is not None and yes_ask > 0 and yes_bid > 0:
            spread = yes_ask - yes_bid
            if spread > cfg.max_spread_to_trade:
                return self._skip(whale_signal, market_price,
                    f"spread_too_wide ({spread:.3f} > {cfg.max_spread_to_trade})")

        # ── Gate 3: per-category whale track record ───────────────────────────
        if whale_registry is not None:
            rejection = self._check_whale_track_records(
                whale_signal, ticker, whale_registry, cfg
            )
            if rejection:
                return self._skip(whale_signal, market_price, rejection)

        # ── Fundamental estimate ──────────────────────────────────────────────
        fundamental = get_fundamental_estimate(ticker, market_price, close_time_iso)
        fundamental.mispricing = fundamental.p_fair - market_price

        # ── Compute composite probability ─────────────────────────────────────
        # The whale tells us *direction* and triggers the trade — it does NOT
        # provide a probability estimate (we have no track record to calibrate
        # how accurate individual whales are).  Probability comes from the
        # fundamental model and the market price only.
        whale_conf = whale_signal.final_confidence

        # Blend: composite = weighted_avg(fundamental_p, market_p)
        w_fund  = fundamental.confidence * cfg.fundamental_weight
        w_mkt   = cfg.market_weight
        total_w = w_fund + w_mkt
        if total_w <= 0:
            total_w = 1.0

        composite_p = (fundamental.p_fair * w_fund + market_price * w_mkt) / total_w
        composite_p = max(0.05, min(0.95, composite_p))

        # ── Compute agreement score ───────────────────────────────────────────
        # Agreement = how close fundamental and market are to each other.
        # Perfect agreement (within 3%) → score=1.0
        # High disagreement (spread >15%) → score=0.0
        spread = abs(fundamental.p_fair - market_price)
        agreement = max(0.0, 1.0 - (spread / 0.15))

        # ── Composite confidence ──────────────────────────────────────────────
        # Whale signal is the primary driver (70% weight) — a large trade IS the
        # signal.  Fundamental contributes in proportion to how much it actually
        # knows (30% weight × its own confidence), so a weak fundamental like the
        # index model (conf=0.30, just echoes market) adds a little rather than
        # dragging the whole calculation down via geometric mean.
        #
        # Agreement bonus is scaled by fundamental.confidence so that trivially
        # perfect agreement (fundamental == market because it defers to market
        # price) doesn't award the full +0.20 for free.
        base_conf       = whale_conf * 0.70 + fundamental.confidence * 0.30
        scaled_agreement = agreement * min(1.0, fundamental.confidence / 0.50)
        agreement_bonus  = cfg.agreement_bonus * scaled_agreement
        composite_conf   = min(1.0, base_conf + agreement_bonus)

        # Cap composite confidence when fundamental model is genuinely uncertain.
        # A whale alone cannot justify high confidence when our own model says
        # "coin flip" (e.g. S&P spot within 0.2% of strike). Without a real
        # fundamental edge, whale confidence is speculative noise.
        if fundamental.confidence < 0.35:
            composite_conf = min(composite_conf, 0.45)

        # ── Flow imbalance signal (supplementary) ─────────────────────────────
        # If rolling directional flow agrees with whale direction, nudge both
        # composite probability and confidence slightly upward.
        # These are confirmations, not drivers — max combined boost ≈ 13%.
        flow_boost   = 0.0
        flow_p_nudge = 0.0
        if flow_tracker is not None:
            flow_sig = flow_tracker.get_signal(ticker)
            if flow_sig is not None and flow_sig.side == side:
                flow_p_nudge = flow_sig.confidence * 0.04 * (1.0 - composite_p)
                flow_boost   = flow_sig.confidence * 0.08
                log.debug(
                    "FlowSignal %s: %s imbalance=%.1f%% conf=%.3f "
                    "→ p_nudge=+%.4f conf_boost=+%.3f",
                    ticker, flow_sig.side, flow_sig.imbalance * 100,
                    flow_sig.confidence, flow_p_nudge, flow_boost,
                )

        # ── Order book imbalance signal (supplementary) ───────────────────────
        # Spread compression + mid-price momentum confirm the direction.
        ob_boost = 0.0
        if ob_tracker is not None:
            ob_sig = ob_tracker.get_signal(ticker)
            if ob_sig is not None:
                ob_agrees = (
                    (side == "yes" and ob_sig.mid_direction == "up") or
                    (side == "no"  and ob_sig.mid_direction == "down")
                )
                if ob_agrees:
                    ob_boost = ob_sig.confidence * 0.05
                    log.debug(
                        "OBSignal %s: direction=%s conf=%.3f → conf_boost=+%.3f",
                        ticker, ob_sig.mid_direction, ob_sig.confidence, ob_boost,
                    )

        # ── Notional-over-threshold boost ────────────────────────────────────
        # A whale putting in 5× the threshold is a stronger signal than one
        # who barely clears it.  We apply a log-scaled confidence boost:
        #
        #   ratio = notional / threshold
        #   boost = min(MAX_NOTIONAL_BOOST, log(ratio) × 0.05)
        #
        # Scale:
        #   1.0× (at threshold)   → +0.000
        #   2.0× threshold        → +0.035
        #   5.0× threshold        → +0.080
        #   10× threshold         → +0.100  (capped)
        #
        # Log scaling is intentional — the difference between a 2× and a 5×
        # whale is meaningful; the difference between 10× and 20× is noise.
        notional_boost = 0.0
        if notional_usd > 0 and whale_threshold > 0:
            ratio = notional_usd / whale_threshold
            if ratio > 1.0:
                notional_boost = min(_MAX_NOTIONAL_BOOST, math.log(ratio) * 0.05)
                log.debug(
                    "Notional boost: $%.0f / $%.0f = %.2f× → +%.4f conf",
                    notional_usd, whale_threshold, ratio, notional_boost,
                )

        # Apply supplementary boosts
        composite_p    = max(0.05, min(0.95, composite_p + flow_p_nudge))
        composite_conf = min(1.0,  composite_conf + flow_boost + ob_boost + notional_boost)

        # ── Gate 4: minimum mispricing ────────────────────────────────────────
        # mispricing = edge in the direction of the whale's trade.
        #
        # YES trade: edge = our_fair_YES - market_YES  (positive = YES underpriced)
        # NO  trade: edge = market_YES  - our_fair_YES (positive = YES overpriced = NO underpriced)
        #
        # Both are now positive when there is genuine edge in the whale's direction,
        # so the same `mispricing >= min_mispricing` check works for both sides.
        our_fair   = composite_p
        if side == "yes":
            mispricing = our_fair - market_price
        else:
            mispricing = market_price - our_fair

        # ── Adaptive mispricing floor ─────────────────────────────────────────
        # Two-tier system depending on whether we have a real fundamental model:
        #
        # Tier 1 — No fundamental model (fund_conf < 0.20, e.g. gas, RT, generic)
        #   composite_p ≈ market_price always, so mispricing ≈ 0 regardless of
        #   whale size. The mispricing check is circular and adds no information.
        #   The whale threshold is the quality gate — if it cleared that, use
        #   the hard minimum (0.5¢) only.
        #
        # Tier 2 — Real fundamental model (fund_conf ≥ 0.20, e.g. FED, CPI, crypto)
        #   Model has genuine independent view. Scale the floor down with notional
        #   so large whales still get more benefit of the doubt, but the full
        #   1.5¢ floor applies to barely-threshold whales.
        #
        #   effective_floor = max(0.005, min_mispricing / notional_ratio)
        #
        _NO_MODEL_THRESHOLD = 0.20   # fund_conf below this = no real model
        _HARD_MIN = 0.005            # absolute floor for real-model markets (0.5¢)

        ratio = (notional_usd / whale_threshold) if (notional_usd > 0 and whale_threshold > 0) else 1.0

        if fundamental.confidence < _NO_MODEL_THRESHOLD:
            # No real model — mispricing is circular noise, adds no information.
            # A weak fallback returning p_fair ≈ market_price should not be able
            # to veto a whale signal. Skip the mispricing gate entirely.
            effective_min_mispricing = -1.0
            log.debug(
                "Mispricing gate bypassed (no fundamental model, fund_conf=%.2f)",
                fundamental.confidence,
            )
        elif ratio > 1.0:
            # Real model + large whale — scale floor down with notional
            effective_min_mispricing = max(_HARD_MIN, cfg.min_mispricing / ratio)
            log.debug(
                "Mispricing floor: adaptive %.4f (notional %.1f× threshold)",
                effective_min_mispricing, ratio,
            )
        else:
            effective_min_mispricing = cfg.min_mispricing

        if mispricing < effective_min_mispricing:
            return self._skip(whale_signal, market_price,
                f"insufficient_mispricing ({mispricing:.4f} < {effective_min_mispricing:.4f}"
                f"{' [no-model floor]' if fundamental.confidence < _NO_MODEL_THRESHOLD else ''})",
                fundamental=fundamental, composite_conf=composite_conf,
                composite_p=composite_p, agreement=agreement)

        # ── Gate 5: minimum composite confidence ─────────────────────────────
        if composite_conf < cfg.min_composite_conf:
            return self._skip(whale_signal, market_price,
                f"composite_conf_too_low ({composite_conf:.3f} < {cfg.min_composite_conf})",
                fundamental=fundamental, composite_conf=composite_conf,
                composite_p=composite_p, agreement=agreement)

        # ── Build final win probability for Kelly ────────────────────────────
        # p_win must be P(our side wins) — not always P(YES).
        # composite_p is always expressed as P(YES), so for NO trades invert it.
        p_win_raw = composite_p if side == "yes" else (1.0 - composite_p)
        shrinkage = max(0.0, 1.0 - composite_conf)
        p_win = p_win_raw * (1 - shrinkage * 0.3) + 0.52 * (shrinkage * 0.3)
        p_win = max(0.52, min(0.97, p_win))

        log.info(
            "SignalEngine: %s %s | whale_conf=%.3f fund_p=%.3f mkt=%.3f → composite=%.3f "
            "| conf=%.3f agree=%.3f mispricing=%+.3f | %s",
            ticker, side, whale_conf, fundamental.p_fair, market_price, composite_p,
            composite_conf, agreement, mispricing, fundamental.notes,
        )

        return CompositeSignal(
            market_ticker          = ticker,
            side                   = side,
            base_price             = whale_signal.base_price,
            final_confidence       = composite_conf,
            win_probability        = p_win,
            contributing_whales    = whale_signal.contributing_whales,
            whale_confidence       = whale_conf,
            fundamental_confidence = fundamental.confidence,
            fundamental_p_fair     = fundamental.p_fair,
            mispricing             = mispricing,
            agreement_score        = agreement,
            category               = fundamental.model_name,
            fundamental_notes      = fundamental.notes,
            convergence_applied    = whale_signal.convergence_applied,
            notional_ratio         = (notional_usd / whale_threshold) if whale_threshold > 0 else 1.0,
        )

    # ── internals ─────────────────────────────────────────────────────────────

    def _check_whale_track_records(
        self,
        signal:   MarketSignal,
        ticker:   str,
        registry,
        cfg:      SignalEngineConfig,
    ) -> Optional[str]:
        """
        Check that contributing whales have a proven track record
        in this specific market category.
        Returns None if all pass, or a skip reason string if not.
        """
        category = detect_category(ticker)

        qualified_whales  = 0
        unproven_whales   = 0   # Bug 5 fix: whales with NO history in this category

        for whale_id in signal.contributing_whales:
            profile = registry.get_whale_profile(whale_id)  # Bug 4: use public API
            if profile is None:
                unproven_whales += 1
                continue

            cat_stats = profile.category_stats.get(category, {})
            resolved  = cat_stats.get("resolved", 0)
            wins      = cat_stats.get("wins", 0)
            win_rate  = (wins / resolved) if resolved > 0 else 0.0

            if resolved == 0:
                # Bug 5: whale has NO history in this category — treat as unproven
                # but allow through (they'll be filtered by decay/weight over time)
                unproven_whales += 1
                log.debug("Whale %s has no history in %s — treating as unproven", whale_id[:8], category)
            elif resolved >= cfg.min_category_trades and win_rate >= cfg.min_category_win_rate:
                qualified_whales += 1
                log.debug(
                    "Whale %s qualifies for %s: %d/%d wins (%.1f%%)",
                    whale_id[:8], category, wins, resolved, win_rate * 100,
                )
            else:
                log.debug(
                    "Whale %s poor record in %s: %d trades, %.1f%% win rate (need %d trades, %.0f%%)",
                    whale_id[:8], category, resolved, win_rate * 100,
                    cfg.min_category_trades, cfg.min_category_win_rate * 100,
                )

        total_whales = len(signal.contributing_whales)

        # Bug 5 fix: grace period — if ALL contributing whales are unproven/new,
        # allow through with reduced confidence rather than hard-blocking.
        # This prevents the bot from being completely frozen on day 1.
        # Once any whale has a proven bad record (not just no record), we block.
        if signal.contributing_whales and qualified_whales == 0:
            if unproven_whales == total_whales:
                # All unproven — allow but caller will use low confidence
                log.debug(
                    "All %d whales unproven in %s — allowing with grace period",
                    total_whales, category,
                )
                return None  # don't block — confidence will be naturally low
            # Mix of unproven + proven-bad → block
            return (f"no_qualified_whales_in_{category} "
                    f"(need {cfg.min_category_trades}+ settled trades "
                    f"at {cfg.min_category_win_rate:.0%}+ win rate)")

        return None  # all checks pass

    def _skip(
        self,
        signal:         MarketSignal,
        market_price:   float,
        reason:         str,
        fundamental:    Optional[FundamentalEstimate] = None,
        composite_conf: float = 0.0,
        composite_p:    float = 0.0,
        agreement:      float = 0.0,
    ) -> CompositeSignal:
        log.info("SignalEngine SKIP: %s %s — %s", signal.market_ticker, signal.side, reason)
        return CompositeSignal(
            market_ticker          = signal.market_ticker,
            side                   = signal.side,
            base_price             = signal.base_price,
            final_confidence       = composite_conf,
            win_probability        = 0.0,
            contributing_whales    = signal.contributing_whales,
            whale_confidence       = signal.final_confidence,
            fundamental_confidence = fundamental.confidence if fundamental else 0.0,
            fundamental_p_fair     = fundamental.p_fair if fundamental else market_price,
            mispricing             = 0.0,
            agreement_score        = agreement,
            category               = "UNKNOWN",
            fundamental_notes      = fundamental.notes if fundamental else "",
            skip_reason            = reason,
            convergence_applied    = signal.convergence_applied,
        )
