"""
fundamental_model.py — Category-Specific Fundamental Probability Models

Each market category has a distinct information structure. This module
provides calibrated base-rate estimates for each category so the bot
has an INDEPENDENT probability estimate to compare against:

  1. Market price (crowd consensus)
  2. Whale signal (informed flow)
  3. Fundamental model (this module) ← structural prior

The composite signal engine blends all three. If all three agree,
confidence is high. If they conflict, confidence is low or zero.

Categories implemented:
  FED     — Fed rate decisions (uses meeting calendar, current rate context)
  MACRO   — CPI, GDP, employment (mean reversion + surprise history)
  CRYPTO  — BTC/ETH price levels (momentum + volatility regime)
  INDEX   — S&P 500 levels (earnings season, VIX context)
  GENERIC — everything else (market price = best estimate, low confidence)

Design principles:
  - All estimates are probabilities in [0.05, 0.95] — never certain
  - Confidence reflects how much structural information we actually have
  - Models are SIMPLE and robust — complex models overfit, simple ones calibrate
  - Every estimate is explainable and auditable in the trade log
"""
from __future__ import annotations

import logging
import math
from dataclasses import dataclass
from datetime import datetime, timezone
from decimal import Decimal
from typing import Optional

try:
    from external_data import get_cache
except ImportError:
    def get_cache(): return None

log = logging.getLogger(__name__)


@dataclass
class FundamentalEstimate:
    """
    Output of the fundamental model for a given market.

    p_fair      : our model's fair probability for YES outcome
    confidence  : how much we trust this estimate (0 = no info, 1 = high conviction)
    model_name  : which model produced this (for logging/audit)
    notes       : human-readable explanation
    mispricing  : p_fair - market_price (positive = market underpricing YES)
    """
    p_fair:      float
    confidence:  float
    model_name:  str
    notes:       str
    mispricing:  float = 0.0   # filled in by caller after market_price is known


# ── Category detection ────────────────────────────────────────────────────────

_CATEGORY_PREFIXES: list[tuple[str, str]] = [
    ("KXFED",       "FED"),
    ("KXCPI",       "MACRO"),
    ("KXGDP",       "MACRO"),
    ("KXPCE",       "MACRO"),
    ("KXJOBS",      "MACRO"),
    ("KXUNR",       "MACRO"),
    ("KXPAYROLLS",  "MACRO"),
    ("KXBTCD",      "CRYPTO"),
    ("KXBTCW",      "CRYPTO"),
    ("KXETHD",      "CRYPTO"),
    ("KXETHW",      "CRYPTO"),
    ("KXINX",       "INDEX"),
    ("KXSPX",       "INDEX"),
    ("KXNDX",       "INDEX"),
    ("KXDOW",       "INDEX"),
    # Commodities — explicit prefixes for all live markets
    ("KXWTI",       "COMMODITY"),
    ("KXBRENTW",    "COMMODITY"),
    ("KXBRENT",     "COMMODITY"),
    ("KXOIL",       "COMMODITY"),
    ("KXGOLD",      "COMMODITY"),
    ("KXNATGAS",    "COMMODITY"),
    ("KXGAS",       "COMMODITY"),
    ("KXAAAGASW",   "COMMODITY"),   # AAA gas price weekly
    ("KXAAAGASD",   "COMMODITY"),
    # Rotten Tomatoes score markets
    ("KXRT",        "ROTTEN_TOMATOES"),
    # Silver/Copper/other metals
    ("KXSILVERW",   "COMMODITY"),
    ("KXSILVER",    "COMMODITY"),
    ("KXCOPPERW",   "COMMODITY"),
    ("KXCOPPER",    "COMMODITY"),
    ("KXGOLDW",     "COMMODITY"),
]


def detect_category(ticker: str) -> str:
    t = ticker.upper()
    for prefix, cat in _CATEGORY_PREFIXES:
        if t.startswith(prefix):
            return cat
    return "GENERIC"


# ── Ticker parser helpers ─────────────────────────────────────────────────────

def _parse_threshold(ticker: str) -> Optional[float]:
    """
    Extract numeric threshold from ticker.
    e.g. KXBTCD-25DEC-T100000 → 100000.0
         KXFED-25DEC-T4.25    → 4.25
         KXINX-25DEC-T5000    → 5000.0
    Handles both -T prefix and -B prefix (below/above style FED tickers).
    """
    t = ticker.upper()
    for sep in ("-T", "-B"):
        parts = t.split(sep)
        if len(parts) >= 2:
            try:
                return float(parts[-1].replace(",", ""))
            except ValueError:
                continue
    # Bare numeric suffix: e.g. KXAAAGASW-26JUN08-4.200 (no T/B prefix)
    parts = t.split("-")
    if parts:
        try:
            val = float(parts[-1].replace(",", ""))
            # Only treat as threshold if it looks like a price/level (not a date like "26JUN08")
            if parts[-1].replace(".", "").replace(",", "").isdigit():
                return val
        except ValueError:
            pass
    return None


def _parse_fed_meeting_month(ticker: str) -> Optional[str]:
    """
    Parse KXFED-25JUN-B4.25 → "JUN25"  (CME ZQ contract month key)
    Kalshi encodes date as YYMTH; CME stores as MTH+YY (e.g. JUN25).
    """
    parts = ticker.upper().split("-")
    if len(parts) < 2:
        return None
    date_part = parts[1]   # e.g. "25JUN" or "26MAR"
    if len(date_part) >= 5:
        year  = date_part[:2]    # "25"
        month = date_part[2:]    # "JUN"
        if month.isalpha():
            return f"{month}{year}"  # "JUN25"
    return None


def _commodity_spot_from_ticker(ticker: str, cache) -> Optional[float]:
    """
    Look up the appropriate spot price for a commodity ticker.
    Returns None if external data unavailable.
    """
    if cache is None:
        return None
    t = ticker.upper()
    if "WTI" in t:
        return cache.wti_price()
    if "BRENT" in t:
        return cache.brent_price()
    if "GOLDW" in t or "GOLD" in t:
        return cache.gold_price()
    if "SILVERW" in t or "SILVER" in t:
        return cache.silver_price()
    if "COPPERW" in t or "COPPER" in t:
        return cache.copper_price()
    if "NATGAS" in t:
        return cache.natgas_price()
    if "AAAGASW" in t or "AAAGASD" in t:
        return cache.natgas_price()   # AAA gas tracks natgas roughly
    return None


def _distance_to_probability(pct_above: float, days: float) -> float:
    """
    Convert (spot - threshold) / threshold to a YES probability for
    'will price be above threshold at expiry?' markets.

    Uses a simple vol-scaled formula:
      - Commodity daily vol ≈ 2% (crude oil), 3% (natgas)
      - Expected range over N days = 2% * sqrt(N)
      - P(above) = normal CDF(pct_above / expected_range)

    Capped at [0.05, 0.95] — never express certainty.
    """
    daily_vol = 0.022   # ~2.2% daily for crude oil (conservative)
    period_vol = daily_vol * math.sqrt(max(1.0, days))
    # z-score: how many std-devs is the threshold from current spot?
    if period_vol <= 0:
        return 0.50
    # For "will price stay ABOVE threshold": P = Φ(z) where z = (spot - threshold) / vol
    # Already expressed as pct_above = (spot - threshold) / threshold
    # So actual vol in price terms ≈ period_vol (as fraction of spot)
    z = pct_above / period_vol
    # Approximate normal CDF with logistic sigmoid (no scipy needed)
    p = 1.0 / (1.0 + math.exp(-1.7 * z))
    return max(0.05, min(0.95, p))


def _days_to_expiry(close_time_iso: Optional[str]) -> float:
    """Days until market closes. Returns 30 if unknown."""
    if not close_time_iso:
        return 30.0
    try:
        close = datetime.fromisoformat(close_time_iso.replace("Z", "+00:00"))
        delta = (close - datetime.now(timezone.utc)).total_seconds() / 86400
        return max(0.0, delta)
    except Exception:
        return 30.0


# ── Fed model ─────────────────────────────────────────────────────────────────

# Historical Fed decision base rates (2015-2024):
# - When market prices hike at >80% probability: hike occurs ~87% of the time
# - When market prices hold at >80% probability: hold occurs ~91% of the time
# - Cut decisions are most uncertain: ~75% accuracy even at high probability
# Source: CME FedWatch tool accuracy studies

_FED_ACCURACY_BY_MARKET_PROB = [
    (0.90, 0.93),   # if market says 90%+ → 93% accurate
    (0.75, 0.87),   # if market says 75%+ → 87% accurate
    (0.60, 0.78),   # if market says 60%+ → 78% accurate
    (0.50, 0.60),   # near 50% → barely better than coin flip
]


def estimate_fed(
    ticker: str,
    market_price: float,
    close_time_iso: Optional[str] = None,
) -> FundamentalEstimate:
    """
    Fed rate decision model — enhanced with FOMC calendar.

    Uses exact hours-to-FOMC from our hardcoded meeting schedule.
    Key insight: Fed markets are among the best-calibrated prediction
    markets in existence. Our edge is detecting when whale flow arrives
    near the meeting and deviates from the settled market consensus.

    Confidence tiers by hours-to-decision:
      >168h (1 week out) : 0.25 — limited info, market dominates
      24-168h            : 0.40 — approaching, insider flow meaningful
      4-24h              : 0.60 — high information period
      <4h                : 0.35 — noise spikes pre-announcement
    """
    days  = _days_to_expiry(close_time_iso)
    cache = get_cache()

    # Use FOMC calendar if available, else fall back to close_time
    hours_to_fomc: Optional[float] = None
    if cache is not None:
        ctx = cache.macro_calendar_context("FED")
        hours_to_fomc = ctx.get("hours_to_event")
    if hours_to_fomc is None:
        hours_to_fomc = days * 24

    # ── Confidence tiers based on proximity to FOMC ───────────────────────────
    if hours_to_fomc < 2:
        confidence = 0.30
        time_note = f"{hours_to_fomc:.1f}h to FOMC (noise zone)"
    elif hours_to_fomc < 24:
        confidence = 0.58
        time_note = f"{hours_to_fomc:.1f}h to FOMC"
    elif hours_to_fomc < 168:
        confidence = 0.42
        time_note = f"{hours_to_fomc:.0f}h to FOMC"
    else:
        confidence = 0.28
        time_note = f"{hours_to_fomc/24:.0f}d to FOMC"

    # ── Try CME 30-Day Fed Funds futures for independent p_fair ──────────────
    # This is the key upgrade: instead of echoing market_price back, we use
    # the independently-traded ZQ futures market as our structural prior.
    cme_prob: Optional[float] = None
    model_name = "FED_CALENDAR"
    if cache is not None:
        meeting_month = _parse_fed_meeting_month(ticker)
        threshold_rate = _parse_threshold(ticker)
        if meeting_month and threshold_rate is not None:
            cme_prob = cache.cme_fed_probability(meeting_month, threshold_rate)

    if cme_prob is not None:
        p_fair = cme_prob
        # CME futures are highly liquid — boost confidence significantly
        confidence = min(0.75, confidence + 0.25)
        model_name = "FED_CME_FUTURES"
        notes = (
            f"Fed[CME]: {meeting_month} rate≤{threshold_rate:.2f}% "
            f"→ CME_p={cme_prob:.1%}, mkt={market_price:.1%} | {time_note}"
        )
        if abs(cme_prob - market_price) > 0.05:
            notes += f" ⚡ DIVERGENCE {(cme_prob - market_price):+.1%}"
    else:
        # No CME data — fall back to calendar-based shrinkage
        shrinkage_factor = min(0.15, hours_to_fomc / (30 * 24) * 0.15)
        p_fair = market_price * (1 - shrinkage_factor) + 0.50 * shrinkage_factor
        if market_price > 0.80 or market_price < 0.20:
            confidence = min(0.65, confidence + 0.07)
        notes = f"Fed[calendar]: mkt={market_price:.1%} | {time_note}"

    p_fair = max(0.05, min(0.95, p_fair))
    confidence = min(0.75, confidence)
    return FundamentalEstimate(
        p_fair=p_fair, confidence=confidence,
        model_name=model_name, notes=notes,
    )


# ── Macro model ───────────────────────────────────────────────────────────────

def _nowcast_to_probability(nowcast_val: float, threshold: float, surprise_std: float) -> float:
    """
    Convert a nowcast estimate to P(actual >= threshold) using a normal distribution.

    Intuition: if the Cleveland Fed expects CPI MoM = +0.28% and the Kalshi
    market asks "will CPI be above 0.3%?", the answer depends on:
      - Distance: nowcast (0.28) vs threshold (0.30) → -0.02 below
      - Uncertainty: historical CPI surprise std dev ≈ 0.10% MoM

    P(actual >= threshold) = P(Z >= (threshold - nowcast) / surprise_std)
                           = 1 - Φ((threshold - nowcast) / surprise_std)

    Uses logistic approximation for Φ (no scipy needed).
    """
    z = (threshold - nowcast_val) / max(surprise_std, 0.001)
    # P(above threshold) = 1 - Φ(z) ≈ sigmoid(-1.7 * z) for logistic approx
    p = 1.0 / (1.0 + math.exp(1.7 * z))
    return max(0.05, min(0.95, p))


def _parse_macro_ticker(ticker: str) -> tuple[str, Optional[float]]:
    """
    Returns (event_type, threshold) from a macro ticker.

    Examples:
      KXCPI-25JUN-T0.3      → ("CPI_MOM",  0.3)
      KXCPIYOY-25JUN-T3.5   → ("CPI_YOY",  3.5)
      KXPCE-25JUN-T0.2      → ("PCE_MOM",  0.2)
      KXPAYROLLS-25JUN-T150 → ("PAYROLLS", 150.0)
      KXGDP-25Q2-T2.0       → ("GDP",      2.0)
    """
    t = ticker.upper()
    threshold = _parse_threshold(ticker)
    if "CPIYOY" in t:
        return ("CPI_YOY", threshold)
    if "CPI" in t:
        return ("CPI_MOM", threshold)
    if "PCE" in t:
        return ("PCE_MOM", threshold)
    if "PAYROLLS" in t or "JOBS" in t or "NFP" in t:
        return ("PAYROLLS", threshold)
    if "GDP" in t:
        return ("GDP", threshold)
    if "UNR" in t or "UNEMP" in t:
        return ("UNEMPLOYMENT", threshold)
    if "PPI" in t:
        return ("PPI_MOM", threshold)
    return ("MACRO", threshold)


# Historical surprise std deviations (how much actual vs consensus deviates, 1σ)
# Source: BLS/BEA historical data, 2015-2024
_SURPRISE_STD = {
    "CPI_MOM":      0.10,   # CPI MoM: ±0.10% typical surprise (1σ)
    "CPI_YOY":      0.12,   # CPI YoY: slightly wider
    "PCE_MOM":      0.08,   # PCE: tighter than CPI (less volatile series)
    "PAYROLLS":    75.0,    # NFP: ±75k typical surprise (1σ)
    "GDP":          0.50,   # GDP: ±0.5pp advance estimate revision range
    "UNEMPLOYMENT": 0.10,   # UE rate: ±0.1pp
    "PPI_MOM":      0.15,   # PPI: wider than CPI
}


def estimate_macro(
    ticker: str,
    market_price: float,
    close_time_iso: Optional[str] = None,
) -> FundamentalEstimate:
    """
    Macro data release model (CPI, GDP, jobs, PCE).

    Real model: uses Cleveland Fed nowcast for CPI/PCE, converts to probability
    using historical surprise volatility. For other series (GDP, payrolls),
    applies calendar-based confidence with mean-reversion at extremes.

    Pipeline:
      1. Identify event type and threshold from ticker
      2. Pull Cleveland Fed nowcast if available
      3. Convert (nowcast, threshold, surprise_std) → p_fair
      4. Scale confidence by hours-to-release
    """
    days  = _days_to_expiry(close_time_iso)
    cache = get_cache()

    event_type, threshold = _parse_macro_ticker(ticker)

    # ── Calendar context ──────────────────────────────────────────────────────
    hours_to_event: Optional[float] = None
    cal_event = "MACRO"

    if cache is not None:
        ctx = cache.macro_calendar_context("MACRO")
        hours_to_event = ctx.get("hours_to_event")
        cal_event      = ctx.get("event_type", "MACRO")
    if hours_to_event is None:
        hours_to_event = days * 24

    # ── Confidence tier from proximity to release ─────────────────────────────
    if hours_to_event < 2:
        base_confidence = 0.20    # noise zone — too close, don't trust model
        time_note = f"{hours_to_event:.1f}h (noise zone)"
    elif hours_to_event < 24:
        base_confidence = 0.50    # imminent — nowcast most accurate
        time_note = f"{hours_to_event:.1f}h to {cal_event}"
    elif hours_to_event < 72:
        base_confidence = 0.38
        time_note = f"{hours_to_event:.0f}h to {cal_event}"
    elif hours_to_event < 168:
        base_confidence = 0.28
        time_note = f"{hours_to_event/24:.1f}d to {cal_event}"
    else:
        time_factor     = max(0.1, 1.0 - (hours_to_event / (14 * 24)) * 0.7)
        base_confidence = 0.20 * time_factor
        time_note = f"{hours_to_event/24:.0f}d to {cal_event}"

    # ── Cleveland Fed nowcast path (CPI/PCE) ──────────────────────────────────
    nowcast_data = cache.cleveland_nowcast() if cache is not None else {}
    nowcast_val: Optional[float] = None

    if event_type == "CPI_MOM":
        nowcast_val = nowcast_data.get("cpi_mom")
        # Augment with UMich inflation expectations as second data point
        if cache is not None:
            umich = cache.umich_inflation_exp()
            if umich is not None and nowcast_val is not None:
                # Blend: 70% Cleveland Fed nowcast + 30% UMich expectations
                # UMich is 1yr expectation, convert to rough MoM proxy (/12)
                umich_mom = umich / 12.0
                nowcast_val = 0.70 * nowcast_val + 0.30 * umich_mom
                log.debug("CPI blend: cleveland=%.2f umich_mom=%.2f blended=%.2f",
                          nowcast_data.get("cpi_mom"), umich_mom, nowcast_val)
    elif event_type == "CPI_YOY":
        nowcast_val = nowcast_data.get("cpi_yoy")
        # Use UMich directly for YoY — it's a 1yr expectation
        if cache is not None and nowcast_val is None:
            umich = cache.umich_inflation_exp()
            if umich is not None:
                nowcast_val = umich
                log.debug("CPI YoY: using UMich expectations=%.1f%%", umich)
    elif event_type == "PCE_MOM":
        nowcast_val = nowcast_data.get("pce_mom")
    elif event_type == "PAYROLLS":
        # Use ADP as leading indicator for NFP
        if cache is not None:
            adp = cache.adp_jobs()
            if adp and adp.get("jobs"):
                nowcast_val = float(adp["jobs"]) / 1000.0  # convert to thousands
                log.debug("Payrolls: using ADP=%dk as NFP proxy", int(nowcast_val))

    if nowcast_val is not None and threshold is not None:
        surprise_std = _SURPRISE_STD.get(event_type, 0.10)
        p_fair       = _nowcast_to_probability(nowcast_val, threshold, surprise_std)
        source_flag  = nowcast_data.get("source", "?")
        # Slight confidence penalty if using actual print (not forecast)
        conf_mult = 0.85 if source_flag == "bls_actual" else 1.0
        confidence = min(0.60, base_confidence * conf_mult)

        notes = (
            f"Macro[{event_type}]: nowcast={nowcast_val:.2f}% "
            f"vs threshold={threshold:.2f}% "
            f"→ p_fair={p_fair:.1%} | {time_note} ({source_flag})"
        )
        if abs(p_fair - market_price) > 0.08:
            notes += f" ⚡ vs mkt={market_price:.1%}"

        log.debug(
            "Macro nowcast[%s]: nowcast=%.2f threshold=%.2f std=%.2f "
            "p_fair=%.3f conf=%.2f | %s",
            event_type, nowcast_val, threshold, surprise_std,
            p_fair, confidence, time_note,
        )
        p_fair = max(0.05, min(0.95, p_fair))
        return FundamentalEstimate(
            p_fair=p_fair, confidence=confidence,
            model_name="MACRO_NOWCAST", notes=notes,
        )

    # ── Fallback: mean-reversion + calendar confidence ────────────────────────
    # No nowcast available — macro markets systematically over-price certainty
    # near extremes (>85% / <15%). Apply a reversion correction.
    log.info(
        "Macro/%s: no nowcast data available — falling back to calendar model "
        "(conf will be capped at 0.15, mispricing gate bypassed) | %s",
        event_type, time_note,
    )
    if market_price > 0.85:
        p_fair = market_price * 0.92 + 0.50 * 0.08
        notes  = f"Macro/{event_type}: extreme YES, mean-reversion | {time_note}"
    elif market_price < 0.15:
        p_fair = market_price * 0.92 + 0.50 * 0.08
        notes  = f"Macro/{event_type}: extreme NO, mean-reversion | {time_note}"
    else:
        p_fair = market_price
        notes  = f"Macro/{event_type}: no nowcast, calendar only | {time_note}"

    # Cap at 0.15 — below the signal_engine's _NO_MODEL_THRESHOLD (0.20).
    # Without real nowcast data, p_fair ≈ market_price (circular). Keeping
    # confidence below 0.20 routes these to the conviction bypass path instead
    # of blocking on a meaningless mispricing check against echoed market price.
    confidence = min(0.15, base_confidence)
    p_fair     = max(0.05, min(0.95, p_fair))
    return FundamentalEstimate(
        p_fair=p_fair, confidence=confidence,
        model_name="MACRO_CALENDAR", notes=notes,
    )


# ── Crypto model ──────────────────────────────────────────────────────────────

def estimate_crypto(
    ticker: str,
    market_price: float,
    close_time_iso: Optional[str] = None,
) -> FundamentalEstimate:
    """
    Crypto price level model (BTC/ETH daily/weekly).

    Incorporates three signals when external data is available:
    1. Fear & Greed Index: extreme fear (0-20) → bearish bias; greed (80-100) → bullish
    2. Funding rates: high positive funding → longs overextended → mean reversion risk
    3. Weekly vs daily timeframe: weekly = mean reversion, daily = momentum

    Base model without external data: market price with calibrated confidence.
    """
    days      = _days_to_expiry(close_time_iso)
    is_weekly = "W" in ticker.upper()
    cache     = get_cache()

    # ── Base estimate (no external data) ─────────────────────────────────────
    if is_weekly:
        if market_price > 0.80:
            p_fair, notes = 0.75, "Crypto weekly: extreme YES, mean-reversion bias"
        elif market_price < 0.20:
            p_fair, notes = 0.25, "Crypto weekly: extreme NO, mean-reversion bias"
        else:
            p_fair, notes = market_price, "Crypto weekly: near-consensus"
        confidence = 0.30
    else:
        p_fair, confidence = market_price, 0.35
        notes = f"Crypto daily: market price base (days={days:.1f})"

    # ── Augment with fear & greed ─────────────────────────────────────────────
    if cache is not None:
        fg = cache.crypto_fear_greed()
        if fg is not None:
            # Extreme fear (0-20): market likely oversold → YES probability nudges up
            # Extreme greed (80-100): market likely overbought → YES probability nudges down
            # (For "will price be above X" markets, greed=bullish but also overextended)
            if fg <= 20:
                fg_adjustment = +0.04   # fear → contrarian buy
                fg_note = f"extreme_fear({fg})"
            elif fg <= 35:
                fg_adjustment = +0.02
                fg_note = f"fear({fg})"
            elif fg >= 80:
                fg_adjustment = -0.03   # greed → caution
                fg_note = f"extreme_greed({fg})"
            elif fg >= 65:
                fg_adjustment = -0.01
                fg_note = f"greed({fg})"
            else:
                fg_adjustment = 0.0
                fg_note = f"neutral({fg})"

            p_fair = max(0.05, min(0.95, p_fair + fg_adjustment))
            confidence = min(0.50, confidence + 0.05)   # slight boost for having data
            notes += f" | fear_greed={fg_note}"

        # ── Augment with funding rate ─────────────────────────────────────────
        funding = cache.btc_funding_rate()
        if funding is not None:
            # High positive funding (>50% annualized): longs paying heavily
            # → market overextended bullish → mean reversion risk for YES markets
            if funding > 50:
                fund_adj = -0.03
                fund_note = f"high_funding({funding:.0f}%pa)"
            elif funding > 25:
                fund_adj = -0.01
                fund_note = f"elevated_funding({funding:.0f}%pa)"
            elif funding < -10:
                fund_adj = +0.02   # negative funding = shorts paying → bullish signal
                fund_note = f"neg_funding({funding:.0f}%pa)"
            else:
                fund_adj = 0.0
                fund_note = f"normal_funding({funding:.0f}%pa)"

            p_fair = max(0.05, min(0.95, p_fair + fund_adj))
            notes += f" | {fund_note}"

    confidence = min(0.50, confidence)
    p_fair = max(0.05, min(0.95, p_fair))
    return FundamentalEstimate(
        p_fair=p_fair, confidence=confidence,
        model_name="CRYPTO_ENHANCED", notes=notes,
    )


# ── Index model ───────────────────────────────────────────────────────────────

def _fetch_spx_spot() -> Optional[float]:
    """
    Fetch live S&P 500 spot price from Yahoo Finance.
    Uses ^GSPC ticker. Returns None on failure.
    """
    try:
        import urllib.request, json
        url = "https://query1.finance.yahoo.com/v8/finance/chart/%5EGSPC?interval=1m&range=1d"
        req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"})
        with urllib.request.urlopen(req, timeout=3) as resp:
            data = json.loads(resp.read())
        price = (
            data["chart"]["result"][0]["meta"].get("regularMarketPrice")
            or data["chart"]["result"][0]["meta"].get("previousClose")
        )
        return float(price) if price else None
    except Exception:
        return None


def _intraday_index_probability(
    spot: float,
    strike: float,
    minutes_to_close: float,
    is_above: bool = True,
) -> float:
    """
    Probability that S&P finishes above (or below) a strike given
    current spot and minutes remaining.

    Uses intraday vol model:
      - S&P intraday vol ≈ 0.8% per full trading day (390 minutes)
      - Scales as sqrt(minutes_remaining / 390)
      - P(above strike) = normal CDF((spot - strike) / (spot * period_vol))

    This correctly prices near-certain outcomes:
      - Spot at 7,450 vs strike 7,387 with 5 minutes left = very high p
      - Spot at 7,400 vs strike 7,500 with 30 minutes left = moderate p
    """
    DAILY_VOL = 0.008   # 0.8% intraday daily vol for S&P 500
    period_vol = DAILY_VOL * math.sqrt(max(1.0, minutes_to_close) / 390.0)

    pct_distance = (spot - strike) / strike   # positive = spot above strike
    if not is_above:
        pct_distance = -pct_distance          # flip for "below" markets

    z = pct_distance / max(period_vol, 0.0001)
    p = 1.0 / (1.0 + math.exp(-1.7 * z))
    return max(0.05, min(0.95, p))


def estimate_index(
    ticker: str,
    market_price: float,
    close_time_iso: Optional[str] = None,
) -> FundamentalEstimate:
    """
    Equity index level model (S&P 500, Nasdaq, Dow).

    Intraday model (< 4 hours to close):
      - Fetches live S&P 500 spot from Yahoo Finance
      - Compares spot to strike threshold in ticker
      - Uses time-scaled intraday vol to compute P(above/below strike)
      - This is the high-value path: same-day markets with known spot

    Multi-day model (>= 4 hours):
      - No real structural edge — defer to market price
      - Low confidence routes to conviction bypass if whale signal is strong
    """
    days  = _days_to_expiry(close_time_iso)
    hours = days * 24
    minutes_to_close = days * 24 * 60

    threshold = _parse_threshold(ticker)

    # ── Intraday real model (< 4 hours to close) ──────────────────────────────
    if hours < 4 and threshold is not None:
        spot = _fetch_spx_spot()
        if spot is not None:
            # Determine if this is an "above" or "below" market from ticker
            # KXINX tickers use -B prefix (e.g. B7587 = "above 7587")
            is_above = "-B" in ticker.upper() or "-T" in ticker.upper()

            p_fair = _intraday_index_probability(
                spot=spot,
                strike=threshold,
                minutes_to_close=max(1.0, minutes_to_close),
                is_above=is_above,
            )

            # Confidence scales with how decisive the outcome is
            pct_distance = abs(spot - threshold) / threshold

            # Hard gate: within 0.2% of strike is a coin flip.
            # Whale signals here are speculative noise — market is correctly
            # pricing near 50/50. Return low confidence so composite stays
            # below MIN_COMPOSITE_CONF and no trade fires.
            if pct_distance < 0.002:
                return FundamentalEstimate(
                    p_fair=0.50,
                    confidence=0.20,
                    model_name="INDEX_INTRADAY",
                    notes=(
                        f"Index[intraday]: spot={spot:.2f} strike={threshold:.2f} "
                        f"({(spot-threshold)/threshold:+.2%}) — within 0.2% gate, "
                        f"coin-flip territory, skip"
                    ),
                )

            if pct_distance > 0.02:        # >2% away — near certain
                confidence = 0.80
            elif pct_distance > 0.01:      # 1-2% away — strong lean
                confidence = 0.70
            elif pct_distance > 0.005:     # 0.5-1% away — moderate
                confidence = 0.60
            else:                          # 0.2-0.5% away — lean but uncertain
                confidence = 0.45

            direction = "above" if (spot > threshold) == is_above else "below"
            notes = (
                f"Index[intraday]: spot={spot:.2f} strike={threshold:.2f} "
                f"({(spot-threshold)/threshold:+.2%} {direction}) "
                f"→ p_fair={p_fair:.1%} | {minutes_to_close:.0f}min remaining"
            )
            log.debug(
                "Index intraday[%s]: spot=%.2f strike=%.2f pct=%+.3f "
                "p_fair=%.3f conf=%.2f mins=%.0f",
                ticker, spot, threshold,
                (spot - threshold) / threshold,
                p_fair, confidence, minutes_to_close,
            )
            return FundamentalEstimate(
                p_fair=max(0.05, min(0.95, p_fair)),
                confidence=confidence,
                model_name="INDEX_INTRADAY",
                notes=notes,
            )

    # ── Multi-day model: use VIX to scale confidence ──────────────────────────
    # VIX tells us how wide the daily range is likely to be
    # High VIX (>25) = big moves expected = low confidence on strike bets
    # Low VIX (<15) = calm market = higher confidence
    cache = get_cache()
    vix = cache.vix() if cache is not None else None

    p_fair = market_price
    confidence = 0.20

    vix_note = ""
    if vix is not None and threshold is not None:
        # Fetch live spot to compute distance
        spot = _fetch_spx_spot()
        if spot is not None:
            days_val = _days_to_expiry(close_time_iso)
            # Scale daily vol by VIX: VIX/100 = annualized vol, daily = VIX/(100*sqrt(252))
            daily_vol = vix / (100 * math.sqrt(252))
            period_vol = daily_vol * math.sqrt(max(1.0, days_val))
            pct_distance = (spot - threshold) / threshold
            z = pct_distance / max(period_vol, 0.0001)
            p_fair = 1.0 / (1.0 + math.exp(-1.7 * z))
            p_fair = max(0.05, min(0.95, p_fair))

            # Confidence based on VIX regime and distance
            abs_dist = abs(pct_distance)
            if vix < 15:
                conf_vix = 0.45
            elif vix < 20:
                conf_vix = 0.35
            elif vix < 30:
                conf_vix = 0.28
            else:
                conf_vix = 0.20   # high vol = low confidence

            # Boost if far from strike
            if abs_dist > 0.05:
                conf_vix = min(0.55, conf_vix + 0.10)
            elif abs_dist > 0.02:
                conf_vix = min(0.50, conf_vix + 0.05)

            confidence = conf_vix
            vix_note = f" | VIX={vix:.1f} spot={spot:.0f} dist={pct_distance:+.2%}"

    notes = f"Index: {hours:.0f}h to close — VIX-scaled model{vix_note}"

    p_fair = max(0.05, min(0.95, p_fair))
    return FundamentalEstimate(
        p_fair=p_fair, confidence=confidence,
        model_name="INDEX_VIX_SCALED", notes=notes,
    )


# ── Commodity model ───────────────────────────────────────────────────────────

def estimate_commodity(
    ticker: str,
    market_price: float,
    close_time_iso: Optional[str] = None,
) -> FundamentalEstimate:
    """
    Commodity price level model (WTI, Brent, Gold, Nat Gas).

    Real model: fetch current spot price from Yahoo Finance and compare it
    to the strike threshold embedded in the ticker (e.g. KXWTI-25JUN-T75
    asks "will WTI close above $75?").

    Uses vol-scaled probability formula:
      - Commodity daily vol ~2.2% for crude oil
      - Expected range over N days = vol * sqrt(N)
      - P(above threshold) = normal CDF((spot - threshold) / expected_range)

    If no external data available, falls back to mean-reversion correction
    at price extremes (>85% / <15%).
    """
    days  = _days_to_expiry(close_time_iso)
    cache = get_cache()

    threshold  = _parse_threshold(ticker)
    spot_price = _commodity_spot_from_ticker(ticker, cache)

    # ── Real model: spot vs threshold ─────────────────────────────────────────
    if spot_price is not None and threshold is not None and threshold > 0:
        pct_above  = (spot_price - threshold) / threshold
        p_fair     = _distance_to_probability(pct_above, days)
        confidence = 0.50

        # Confidence rises when spot is decisively far from threshold
        abs_pct = abs(pct_above)
        if abs_pct > 0.10:       # >10% away — outcome nearly certain
            confidence = 0.60
        elif abs_pct > 0.05:     # 5-10% away — strong lean
            confidence = 0.55
        elif abs_pct < 0.01:     # ±1% — right at the threshold, genuine coin flip
            confidence = 0.35

        direction = "above" if pct_above >= 0 else "below"
        notes = (
            f"Commodity: spot=${spot_price:.2f}, strike=${threshold:.2f} "
            f"({pct_above:+.1%} {direction}), "
            f"p_fair={p_fair:.1%}, days={days:.1f}"
        )
        model_name = "COMMODITY_SPOT"

        log.debug(
            "Commodity[%s]: spot=%.2f threshold=%.2f pct=%+.2f%% "
            "p_fair=%.3f conf=%.2f days=%.1f",
            ticker, spot_price, threshold, pct_above * 100,
            p_fair, confidence, days,
        )
        p_fair = max(0.05, min(0.95, p_fair))
        return FundamentalEstimate(
            p_fair=p_fair, confidence=confidence,
            model_name=model_name, notes=notes,
        )

    # ── Fallback: mean-reversion correction at extremes ───────────────────────
    # Markets priced >85% or <15% in commodity markets are systematically
    # overconfident — reversion toward 70%/30% is historically observed.
    if market_price > 0.85:
        p_fair = 0.78
        notes  = f"Commodity: extreme YES ({market_price:.0%}), mean-reversion → {p_fair:.0%}"
        confidence = 0.30
    elif market_price < 0.15:
        p_fair = 0.22
        notes  = f"Commodity: extreme NO ({market_price:.0%}), mean-reversion → {p_fair:.0%}"
        confidence = 0.30
    else:
        p_fair     = market_price
        confidence = 0.15   # below _NO_MODEL_THRESHOLD (0.20) → routes to conviction bypass
        notes      = "Commodity: no spot data, market price as base"

    p_fair = max(0.05, min(0.95, p_fair))
    return FundamentalEstimate(
        p_fair=p_fair, confidence=confidence,
        model_name="COMMODITY_MEANREV", notes=notes,
    )


# ── Rotten Tomatoes score cache ───────────────────────────────────────────────
# _fetch_rt_score() does a live HTTP request. Cache results for 30 minutes so
# whale signal evaluation doesn't block the event loop on every RT market hit.
_RT_SCORE_CACHE: dict[str, tuple[Optional[int], Optional[int], float]] = {}
_RT_CACHE_TTL = 1800   # 30 minutes

# ── Rotten Tomatoes model ─────────────────────────────────────────────────────

# Map Kalshi KXRT ticker suffix → RT movie slug
# Add new movies here as Kalshi lists them
_RT_SLUG_MAP: dict[str, str] = {
    "MAS": "masters_of_the_universe_2026",
    "SCA": "scary_movie_2026",
    "PRE": "pressure_2026",
    "BAC": "backrooms_2026",
    "MIC": "michael_2026",
    # Slugs below need verification — will use genre prior fallback until confirmed
    # "DIS": "disclosure_2026",      # 404 — slug unknown
    # "POW": "power_2026",           # 404 — slug unknown
    # "STO": "stories_2026",         # 404 — slug unknown
    # "HOW": "how_to_make_a_killing_2026",
    # "DRA": "the_drama_2026",
}

# Genre base rates: historical average RT scores by genre
# Used when no live score is available yet
_GENRE_BASE_RATES: dict[str, float] = {
    "action":    0.63,
    "horror":    0.62,
    "comedy":    0.55,
    "drama":     0.72,
    "animation": 0.78,
    "default":   0.65,
}


def _fetch_rt_score(movie_slug: str) -> tuple[Optional[int], Optional[int]]:
    """
    Scrape current Tomatometer score and review count from RT.
    Returns (score_pct, review_count) or (None, None) on failure.

    Results are cached for _RT_CACHE_TTL seconds (30 min) so the event loop
    is not blocked by a 4-second HTTP call on every whale signal evaluation.
    """
    import time, urllib.request, re

    # ── Cache check ────────────────────────────────────────────────────────────
    cached = _RT_SCORE_CACHE.get(movie_slug)
    if cached is not None:
        score, reviews, ts = cached
        if time.time() - ts < _RT_CACHE_TTL:
            log.debug("RT cache hit for %s: score=%s reviews=%s", movie_slug, score, reviews)
            return score, reviews

    # ── Live fetch ─────────────────────────────────────────────────────────────
    url = f"https://www.rottentomatoes.com/m/{movie_slug}"
    req = urllib.request.Request(url, headers={
        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 Chrome/120.0 Safari/537.36",
        "Accept": "text/html,application/xhtml+xml",
        "Accept-Language": "en-US,en;q=0.9",
    })
    try:
        with urllib.request.urlopen(req, timeout=4) as resp:
            html = resp.read().decode("utf-8", errors="ignore")
        # Primary pattern: JSON in page metadata
        m = re.search(r'Tomatometer","ratingCount":(\d+),"ratingValue":"(\d+)"', html)
        if m:
            score, reviews = int(m.group(2)), int(m.group(1))
            _RT_SCORE_CACHE[movie_slug] = (score, reviews, time.time())
            return score, reviews
        # Secondary: score value in JSON
        m = re.search(r'"score":(\d+)', html)
        if m:
            score = int(m.group(1))
            _RT_SCORE_CACHE[movie_slug] = (score, None, time.time())
            return score, None
        # Cache the miss so we don't hammer the site on every signal
        _RT_SCORE_CACHE[movie_slug] = (None, None, time.time())
        return None, None
    except Exception as exc:
        log.debug("RT fetch failed for %s: %s", movie_slug, exc)
        _RT_SCORE_CACHE[movie_slug] = (None, None, time.time())
        return None, None


def _rt_score_to_probability(
    current_score: float,
    threshold: float,
    review_count: Optional[int],
    days_to_close: float,
) -> tuple[float, float]:
    """
    Convert current RT score to P(final score >= threshold).

    Key insight: RT scores stabilize as review count grows.
    - <10 reviews: high variance, score could move ±20 points
    - 10-30 reviews: moderate variance, ±10 points
    - 30-100 reviews: low variance, ±5 points
    - >100 reviews: very stable, ±2-3 points

    Returns (p_fair, confidence)
    """
    n = review_count or 0

    # Score volatility based on review count
    if n < 10:
        score_std = 18.0
        conf_base = 0.35
    elif n < 30:
        score_std = 10.0
        conf_base = 0.50
    elif n < 100:
        score_std = 5.0
        conf_base = 0.65
    else:
        score_std = 2.5
        conf_base = 0.78

    # Also account for days remaining — more time = more reviews = more movement
    if days_to_close > 7:
        score_std = max(score_std, 12.0)
        conf_base = min(conf_base, 0.40)
    elif days_to_close > 2:
        score_std *= 1.3
        conf_base *= 0.85

    # P(final >= threshold) given current score and uncertainty
    distance = current_score - threshold
    z = distance / max(score_std, 0.1)
    p = 1.0 / (1.0 + math.exp(-1.7 * z))
    p = max(0.05, min(0.95, p))

    return p, conf_base


def estimate_rotten_tomatoes(
    ticker: str,
    market_price: float,
    close_time_iso: Optional[str] = None,
) -> FundamentalEstimate:
    """
    Rotten Tomatoes score market model.

    Fetches live Tomatometer score and computes P(final score >= threshold)
    using review count to estimate score stability.

    High confidence when: many reviews in + score far from threshold
    Low confidence when: few reviews in or close to threshold
    """
    days = _days_to_expiry(close_time_iso)
    threshold = _parse_threshold(ticker)

    # Extract movie code from ticker e.g. KXRT-MAS-70 → "MAS"
    parts = ticker.upper().split("-")
    movie_code = parts[1] if len(parts) >= 2 else ""
    slug = _RT_SLUG_MAP.get(movie_code)

    if threshold is None:
        return FundamentalEstimate(
            p_fair=market_price, confidence=0.10,
            model_name="RT_NO_THRESHOLD",
            notes="RT: could not parse threshold from ticker",
        )

    # ── Try to fetch live RT score ─────────────────────────────────────────────
    if slug:
        score, review_count = _fetch_rt_score(slug)
        if score is not None:
            p_fair, confidence = _rt_score_to_probability(
                current_score=float(score),
                threshold=threshold,
                review_count=review_count,
                days_to_close=days,
            )
            distance = score - threshold
            notes = (
                f"RT[{movie_code}]: score={score}% (n={review_count or '?'}) "
                f"vs threshold={threshold:.0f}% ({distance:+.0f}pts) "
                f"→ p_fair={p_fair:.1%} | {days:.1f}d remaining"
            )
            log.debug(
                "RT[%s]: score=%d threshold=%.0f distance=%+.0f "
                "p_fair=%.3f conf=%.2f reviews=%s days=%.1f",
                movie_code, score, threshold, distance,
                p_fair, confidence, review_count, days,
            )
            if abs(p_fair - market_price) > 0.10:
                notes += f" ⚡ vs mkt={market_price:.1%}"
            return FundamentalEstimate(
                p_fair=p_fair, confidence=confidence,
                model_name="RT_LIVE_SCORE", notes=notes,
            )

    # ── Fallback: no live score available ─────────────────────────────────────
    # Use genre base rate as prior — most movies score 60-70%
    base_rate = _GENRE_BASE_RATES["default"]
    if threshold <= 40:
        p_fair = 0.85   # nearly any movie clears a 40% bar
    elif threshold <= 60:
        p_fair = 0.65
    elif threshold <= 75:
        p_fair = 0.45
    elif threshold <= 85:
        p_fair = 0.25
    else:
        p_fair = 0.10

    notes = (
        f"RT[{movie_code}]: no live score "
        f"(slug={'unknown' if not slug else 'fetch_failed'}) "
        f"→ genre_prior={p_fair:.1%} for threshold={threshold:.0f}%"
    )
    return FundamentalEstimate(
        p_fair=max(0.05, min(0.95, p_fair)),
        confidence=0.25,
        model_name="RT_GENRE_PRIOR",
        notes=notes,
    )


# ── Generic fallback ──────────────────────────────────────────────────────────

def estimate_generic(
    ticker: str,
    market_price: float,
    close_time_iso: Optional[str] = None,
) -> FundamentalEstimate:
    """Fallback for unknown categories. Market price = best estimate."""
    p_fair = market_price
    confidence = 0.10  # very low — we have no structural view
    notes = "Generic: no fundamental model for this category"
    p_fair = max(0.05, min(0.95, p_fair))
    return FundamentalEstimate(
        p_fair=p_fair, confidence=confidence,
        model_name="GENERIC", notes=notes,
    )


# ── Public interface ──────────────────────────────────────────────────────────

_ESTIMATORS = {
    "FED":               estimate_fed,
    "MACRO":             estimate_macro,
    "CRYPTO":            estimate_crypto,
    "INDEX":             estimate_index,
    "COMMODITY":         estimate_commodity,
    "ROTTEN_TOMATOES":   estimate_rotten_tomatoes,
    "GENERIC":           estimate_generic,
}


def get_fundamental_estimate(
    ticker: str,
    market_price: float,
    close_time_iso: Optional[str] = None,
) -> FundamentalEstimate:
    """
    Main entry point. Returns a FundamentalEstimate for any ticker.

    Args:
        ticker        : Kalshi market ticker (e.g. "KXBTCD-25DEC-T100000")
        market_price  : current YES price in dollars (0.0 – 1.0)
        close_time_iso: market close time as ISO-8601 string (optional)

    Returns:
        FundamentalEstimate with p_fair, confidence, model_name, notes
    """
    category = detect_category(ticker)
    estimator = _ESTIMATORS.get(category, estimate_generic)
    estimate = estimator(ticker, market_price, close_time_iso)

    # Compute mispricing vs current market price
    estimate.mispricing = estimate.p_fair - market_price

    log.debug(
        "Fundamental[%s] %s: p_fair=%.3f conf=%.2f mispricing=%+.3f | %s",
        category, ticker, estimate.p_fair, estimate.confidence,
        estimate.mispricing, estimate.notes,
    )
    return estimate
