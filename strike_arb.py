"""
strike_arb.py — Intra-Series Strike Arbitrage Scanner

Detects logical inconsistencies between multiple strikes of the same
Kalshi event. When the implied probabilities across strikes violate
monotonicity, at least one market is mispriced — a tradeable edge
requiring no whale signal at all.

Supports two market directions detected via Kalshi API fields:

  "Below" (cap_strike set): YES = P(value ≤ cap_strike)
    Prices must increase with cap_strike.
    Violation: hi_cap YES < lo_cap YES → buy hi_cap YES + lo_cap NO.

  "Above" (floor_strike set): YES = P(value ≥ floor_strike)
    Prices must decrease with floor_strike.
    Violation: lo_floor YES < hi_floor YES → buy lo_floor YES + hi_floor NO.

Markets are grouped by event_ticker (the authoritative Kalshi API field that
groups all strikes of the same event), with a fallback to ticker-string parsing
for backward compatibility.

Usage:
    scanner = StrikeArbScanner(sess, cfg)
    opportunities = await scanner.scan_once()
    for opp in opportunities:
        # opp.ticker, opp.side, opp.fair_value, opp.market_price, opp.edge
"""
from __future__ import annotations

import asyncio
import calendar
import logging
import re
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Optional

from config import VERBOSE_LOGS

log = logging.getLogger(__name__)

_MIN_EDGE:           float = 0.02   # minimum mispricing to flag (2¢)
_MAX_EDGE:           float = 0.15   # skip if edge > 15¢ — almost certainly stale/illiquid price
_MIN_VOLUME:         int   = 500    # skip markets with < 500 contracts traded (thin/stale)
_MIN_GROUP_VOLUME:   int   = 1000   # skip entire group if total volume < 1000 contracts
_MAX_PAGES_PER_SERIES: int = 5      # pagination cap per series (5 × 200 = 1000 markets)
_MAX_DAYS_TO_EXPIRY: int   = 60     # skip markets settling more than 60 days out
_MAX_GROUP_DAYS:     int   = 7      # skip entire group if all markets are > N days out (illiquid)

# Series that carry threshold/strike markets on Kalshi.
# The generic /markets endpoint returns KXMVE sports parlays — strike markets
# are only accessible via series_ticker= filter (same approach as MarketSelector).
_THRESHOLD_SERIES: list[str] = [
    "KXFED",   "KXCPI",  "KXGDP",   "KXINX",   # confirmed active with strikes
    "KXBTCD",  "KXETHD",                          # crypto — low volume but check
    "KXPRES",  "KXGEO",  "KXELECT",              # political
]

_MONTH_MAP: dict[str, int] = {
    "JAN": 1, "FEB": 2, "MAR": 3, "APR": 4, "MAY": 5,  "JUN": 6,
    "JUL": 7, "AUG": 8, "SEP": 9, "OCT": 10, "NOV": 11, "DEC": 12,
}


def _days_until(market: dict) -> float:
    """
    Return days until this market settles.  Returns 999.0 if unknown.

    Priority:
      1. close_time / expiration_time field from the API response (most accurate)
      2. Ticker name parsing as fallback (approximate)

    Ticker date formats handled:
      KXFED-26JUN-T3.75     → YYMON  → end of June 2026
      KXGDP-26JUL30-T1.5   → YYMONDD → July 30, 2026
      KXINX-26MAY28H1600-B  → YYMONDDH → May 28, 2026
    """
    now = datetime.now(timezone.utc)

    # 1. Use API-provided timestamp
    for key in ("close_time", "expiration_time", "settlement_time"):
        raw = market.get(key)
        if raw:
            try:
                t = datetime.fromisoformat(str(raw).replace("Z", "+00:00"))
                return max(0.0, (t - now).total_seconds() / 86400)
            except Exception:
                pass

    # 2. Parse ticker date
    ticker = market.get("ticker", "").upper()
    parts  = ticker.split("-")
    if len(parts) >= 2:
        date_part = parts[1]

        # YYMONDD[H...] — e.g. 26JUL30, 26MAY28H1600
        m = re.match(r'^(\d{2})([A-Z]{3})(\d{1,2})', date_part)
        if m:
            yy, mon, dd = m.groups()
            mo = _MONTH_MAP.get(mon)
            if mo:
                try:
                    t = datetime(2000 + int(yy), mo, int(dd), 20, 0, tzinfo=timezone.utc)
                    return max(0.0, (t - now).total_seconds() / 86400)
                except ValueError:
                    pass

        # YYMON — e.g. 27APR, 26JUN  (no day → use end of month)
        m = re.match(r'^(\d{2})([A-Z]{3})$', date_part)
        if m:
            yy, mon = m.groups()
            mo = _MONTH_MAP.get(mon)
            if mo:
                year     = 2000 + int(yy)
                last_day = calendar.monthrange(year, mo)[1]
                try:
                    t = datetime(year, mo, last_day, 20, 0, tzinfo=timezone.utc)
                    return max(0.0, (t - now).total_seconds() / 86400)
                except ValueError:
                    pass

    return 999.0  # unknown expiry — will be filtered by _MAX_DAYS_TO_EXPIRY


@dataclass
class ArbOpportunity:
    ticker:         str
    side:           str     # "yes" | "no"
    market_price:   float   # current market price of the primary leg
    fair_value:     float   # implied fair value from comparison leg
    edge:           float   # size of the mispricing (positive = underpriced primary)
    series:         str
    lower_strike:   float
    upper_strike:   float
    notes:          str
    days_to_expiry: float = 999.0   # days until settlement (used for priority sorting)
    direction:      str   = "below" # "below" (cap_strike) | "above" (floor_strike)
    # Hedge leg — the second leg of the locked two-leg arb.
    # combined_cost = market_price + hedge_price < 1.0 always (by violation condition).
    # Minimum payout at settlement = $1.00 per contract pair — true risk-free arb.
    hedge_ticker:   str   = ""    # second-leg market ticker
    hedge_price:    float = 0.0   # cost of NO on the hedge market = 1 - hedge_yes_price

    @property
    def edge_per_day(self) -> float:
        """Daily return proxy — larger = higher priority."""
        return self.edge / max(0.5, self.days_to_expiry)

    @property
    def is_two_leg(self) -> bool:
        """True when the hedge leg is populated — enables locked arb execution."""
        return bool(self.hedge_ticker) and self.hedge_price > 0.0

    @property
    def locked_profit_pct(self) -> float:
        """Guaranteed profit as % of combined cost when both legs fill."""
        if not self.is_two_leg:
            return 0.0
        combined = self.market_price + self.hedge_price
        return (1.0 - combined) / combined if combined < 1.0 else 0.0


def _extract_strike(ticker: str) -> Optional[float]:
    """Extract numeric threshold from ticker, e.g. KXFED-26JUN-T3.75 → 3.75"""
    match = re.search(r"-T([\d.]+)$", ticker.upper())
    if match:
        try:
            return float(match.group(1))
        except ValueError:
            pass
    return None


def _extract_series_base(ticker: str) -> Optional[str]:
    """
    Extract series base (everything before the strike).
    e.g. KXFED-26JUN-T3.75 → KXFED-26JUN
    """
    match = re.match(r"^(.*)-T[\d.]+$", ticker.upper())
    return match.group(1) if match else None


class StrikeArbScanner:
    """
    Periodically scans active Kalshi markets for inter-strike mispricing.
    Covers both "below" (cap_strike) and "above" (floor_strike) threshold
    markets discovered via the Kalshi REST API.

    Markets are grouped by event_ticker (API field) for correctness, falling
    back to ticker-string parsing when event_ticker is absent.

    Runs as a background task in bot_engine.  When it finds an opportunity,
    it places it in the output queue for bot_engine to size and execute.
    """

    def __init__(self, sess, cfg) -> None:
        self._sess = sess
        self._cfg  = cfg
        self._opportunities: asyncio.Queue = asyncio.Queue(maxsize=100)

        # ── Price / series caches ─────────────────────────────────────────────
        # Populated by REST scan; updated per-tick by WS events.
        self._price_cache:    dict[str, float] = {}  # ticker.upper() → yes_price
        self._days_cache:     dict[str, float] = {}  # ticker.upper() → days_to_expiry
        self._ticker_strike:  dict[str, float] = {}  # ticker.upper() → strike value
        # group_key format: "<event_ticker_or_series_base>:<direction>"
        # direction is "below" or "above"
        self._series_index:   dict[str, list[str]] = {}  # group_key → [tickers sorted by strike]
        self._ticker_to_group: dict[str, str] = {}  # ticker.upper() → group_key

        # Optional callback: called with each violation ticker so the WS feed
        # can subscribe that market immediately, before the consumer processes it.
        self._subscribe_callback = None
        # Throttle all summary logs to once every 5 minutes.
        # Violations also log immediately when the violation set changes.
        self._last_summary_log:   float     = 0.0
        self._last_violation_log: float     = 0.0
        self._last_violation_fp:  frozenset = frozenset()

    def set_subscribe_callback(self, callback) -> None:
        """
        Register an async callback (ticker: str) -> None that will be called
        for every ticker involved in a newly detected violation.  The scanner
        calls this in scan_loop right after finding opportunities so both arb
        legs are subscribed before the consumer even dequeues the opportunity.
        """
        self._subscribe_callback = callback

    def is_indexed_ticker(self, ticker: str) -> bool:
        """
        Return True if this ticker is tracked in any arb group (either direction).
        Used by bot_engine to gate the WS-triggered fast-check without a regex.
        O(1) dict lookup — safe to call on every orderbook_delta event.
        """
        return ticker.upper() in self._ticker_to_group

    @property
    def opportunities(self) -> asyncio.Queue:
        return self._opportunities

    async def scan_once(self) -> list[ArbOpportunity]:
        """
        Run one full scan pass.  Returns list of arbitrage opportunities found.

        Fetches all open markets in a single API call, classifies each as
        "below" (cap_strike) or "above" (floor_strike), groups by event_ticker,
        and checks monotonicity within each group for each direction.

        Bracket markets (both floor_strike and cap_strike set) are skipped —
        they belong to sum_arb.  No hardcoded series list required.
        """
        opps: list[ArbOpportunity] = []

        # ── Series-based fetch ────────────────────────────────────────────────
        # Kalshi's generic /markets endpoint returns KXMVE sports parlays, not
        # financial strike markets.  Fetch each known threshold series directly.
        markets: list[dict] = []
        for series in _THRESHOLD_SERIES:
            cursor: Optional[str] = None
            for _ in range(_MAX_PAGES_PER_SERIES):
                try:
                    params: dict = {"series_ticker": series, "limit": "200"}
                    if cursor:
                        params["cursor"] = cursor
                    resp   = await self._sess.get("/markets", params=params)
                    page   = resp.get("markets") or []
                    markets.extend(page)
                    cursor = resp.get("cursor")
                    if not cursor or not page:
                        break
                except Exception as exc:
                    log.debug("StrikeArb: fetch failed for series %s: %s", series, exc)
                    break
        if not markets:
            log.warning("StrikeArb: no markets fetched — check API connectivity")
            return []

        # ── Classify + group ──────────────────────────────────────────────────
        # group_key = "<event_ticker_or_base>:<direction>"
        groups: dict[str, list[dict]] = {}

        for m in markets:
            ticker       = m.get("ticker", "")
            floor_strike = m.get("floor_strike")   # not None → "above" market
            cap_strike   = m.get("cap_strike")     # not None → "below" market

            strike    = None
            direction = None

            if cap_strike is not None and floor_strike is None:
                # "Below" market: YES = P(value ≤ cap_strike)
                try:
                    strike    = float(cap_strike)
                    direction = "below"
                except (TypeError, ValueError):
                    pass
            elif floor_strike is not None and cap_strike is None:
                # "Above" market: YES = P(value ≥ floor_strike)
                try:
                    strike    = float(floor_strike)
                    direction = "above"
                except (TypeError, ValueError):
                    pass
            # Bracket markets (both set) → skip; sum_arb handles them.
            # Markets with neither → fall back to ticker-string parsing below.

            if strike is None:
                # Fallback: ticker ends in -T<number> → treat as "below"
                if re.search(r"-T[\d.]+$", ticker.upper()):
                    strike    = _extract_strike(ticker)
                    direction = "below"
                else:
                    continue   # not a threshold market we can classify

            if strike is None or direction is None:
                continue

            # Grouping key: prefer event_ticker (authoritative), fall back to
            # ticker parsing.  Include direction so "above" and "below" markets
            # for the same event are checked in separate groups.
            event_ticker = (m.get("event_ticker") or "").strip()
            if event_ticker:
                group_key = f"{event_ticker}:{direction}"
            else:
                base = _extract_series_base(ticker)
                if base is None:
                    continue   # no event_ticker and no -T<number> pattern
                group_key = f"{base}:{direction}"

            # Skip thin/stale markets
            raw_vol = m.get("volume") or m.get("volume_24h")
            if raw_vol is not None:
                try:
                    if int(raw_vol) < _MIN_VOLUME:
                        log.debug(
                            "StrikeArb: skipping %s — volume %s < %d (stale market)",
                            ticker, raw_vol, _MIN_VOLUME,
                        )
                        continue
                except (TypeError, ValueError):
                    pass

            # Skip markets settling too far in the future
            days = _days_until(m)
            if days > _MAX_DAYS_TO_EXPIRY:
                log.debug(
                    "StrikeArb: skipping %s — settles in %.0f days (max=%d)",
                    ticker, days, _MAX_DAYS_TO_EXPIRY,
                )
                continue

            m["_strike"]         = strike
            m["_days_to_expiry"] = days
            m["_direction"]      = direction
            groups.setdefault(group_key, []).append(m)

        n_below = sum(1 for k in groups if k.endswith(":below"))
        n_above = sum(1 for k in groups if k.endswith(":above"))
        n_mkts  = sum(len(v) for v in groups.values())
        _log_now = time.monotonic() - self._last_summary_log >= 300   # every 5 min
        if _log_now and VERBOSE_LOGS:
            log.info(
                "StrikeArb scan: %d threshold markets — %d below-groups, %d above-groups"
                " (from %d total fetched)",
                n_mkts, n_below, n_above, len(markets),
            )
            if n_mkts == 0:
                log.warning(
                    "StrikeArb: no threshold markets found from %d total fetched "
                    "(cap_strike/floor_strike absent or all below volume floor %d)",
                    len(markets), _MIN_VOLUME,
                )

        # ── Rebuild price / series caches from fresh REST data ────────────────
        # WS updates will layer on top of these each time orderbook_delta fires.
        self._series_index.clear()
        self._ticker_to_group.clear()
        self._ticker_strike.clear()
        for group_key, gm in groups.items():
            sorted_t = [
                m["ticker"]
                for m in sorted(gm, key=lambda x: x["_strike"])
            ]
            self._series_index[group_key] = sorted_t
            for m in gm:
                t = m["ticker"].upper()
                p = self._get_yes_price(m)
                if p is not None:
                    self._price_cache[t] = p
                self._days_cache[t]      = m.get("_days_to_expiry", 999.0)
                self._ticker_to_group[t] = group_key
                self._ticker_strike[t]   = m["_strike"]

        # ── Check monotonicity in each group ──────────────────────────────────
        for group_key, group_markets in groups.items():
            if len(group_markets) < 2:
                continue
            min_days = min(m.get("_days_to_expiry", 999.0) for m in group_markets)
            if min_days > _MAX_GROUP_DAYS:
                continue
            # Skip entire group if total volume is too thin — arb signals from
            # illiquid groups are almost always stale prices, not real violations
            group_vol = sum(
                int(m.get("volume") or m.get("volume_24h") or 0)
                for m in group_markets
            )
            if group_vol < _MIN_GROUP_VOLUME:
                log.debug(
                    "StrikeArb: skipping group %s — total volume %d < %d",
                    group_key, group_vol, _MIN_GROUP_VOLUME,
                )
                continue
            direction = "above" if group_key.endswith(":above") else "below"
            # Derive human-readable series label from group_key prefix
            prefix = group_key.split(":")[0]
            series = prefix.split("-")[0] if "-" in prefix else prefix

            if direction == "below":
                group_opps = self._check_monotonicity(group_key, group_markets, series)
            else:
                group_opps = self._check_above_monotonicity(group_key, group_markets, series)
            opps.extend(group_opps)

        if opps:
            # Sort by edge-per-day descending: imminent large-edge arbs get priority.
            opps.sort(key=lambda o: o.edge_per_day, reverse=True)
            violation_fp = frozenset((o.ticker, o.hedge_ticker) for o in opps)
            _now = time.monotonic()
            _viol_changed  = violation_fp != self._last_violation_fp
            _viol_due      = _now - self._last_violation_log >= 300
            if _viol_changed or _viol_due:
                log.info(
                    "StrikeArb: %d violation(s) found — %s",
                    len(opps),
                    [
                        f"{o.ticker}({o.direction})+{o.hedge_ticker}"
                        f" edge={o.edge:+.3f} combined=${o.market_price + o.hedge_price:.3f}"
                        for o in opps
                    ],
                )
                self._last_violation_log = _now
                self._last_violation_fp  = violation_fp
            self._last_summary_log = _now
        elif _log_now and VERBOSE_LOGS:
            log.info(
                "StrikeArb: no violations across %d groups (min_edge=%.2f)",
                len(groups), _MIN_EDGE,
            )
            self._last_summary_log = time.monotonic()

        return opps

    async def scan_loop(self, interval_seconds: int = 60) -> None:
        """Background task — scan every interval_seconds."""
        log.info("StrikeArbScanner: starting scan loop (interval=%ds)", interval_seconds)
        while True:
            await asyncio.sleep(interval_seconds)
            try:
                opps = await self.scan_once()
                for opp in opps:
                    # Subscribe both legs immediately — don't wait for the
                    # consumer to hit the OB gate before subscribing.  By the
                    # time the consumer runs, OB data is already flowing.
                    if self._subscribe_callback:
                        try:
                            await self._subscribe_callback(opp.ticker)
                            if opp.hedge_ticker:
                                await self._subscribe_callback(opp.hedge_ticker)
                        except Exception:
                            pass   # subscribe failure is non-fatal
                    try:
                        self._opportunities.put_nowait(opp)
                    except asyncio.QueueFull:
                        log.debug("Strike arb queue full — dropping opportunity")
            except asyncio.CancelledError:
                return
            except Exception as exc:
                log.warning("Strike arb scan loop error: %s", exc, exc_info=True)

    # ── Real-time WS price update ─────────────────────────────────────────────

    def update_ws_price(self, ticker: str, yes_price: float) -> list[ArbOpportunity]:
        """
        Called by bot_engine whenever an orderbook_delta event arrives for a
        tracked market.  Updates the price cache and instantly runs an in-memory
        monotonicity check for that market's group.

        Returns any violations found (caller enqueues them).  Pure in-memory —
        no API calls, runs in microseconds.

        Use is_indexed_ticker() to gate calls to this method.
        """
        t = ticker.upper()
        self._price_cache[t] = yes_price

        group_key = self._ticker_to_group.get(t)
        if group_key is None:
            return []   # ticker not yet indexed (first REST scan hasn't run)

        return self._fast_check(group_key)

    def _fast_check(self, group_key: str) -> list[ArbOpportunity]:
        """
        In-memory monotonicity check using the latest cached prices.
        Uses data from the last REST scan + any WS updates since then.
        No API calls — safe to call on every orderbook_delta event.
        """
        tickers = self._series_index.get(group_key, [])
        if len(tickers) < 2:
            return []

        direction = "above" if group_key.endswith(":above") else "below"

        markets = []
        for t in tickers:
            price  = self._price_cache.get(t.upper())
            strike = self._ticker_strike.get(t.upper())
            days   = self._days_cache.get(t.upper(), 999.0)
            if price is None or strike is None or days <= 0.0:
                continue
            markets.append({
                "ticker":             t,
                "_strike":            strike,
                "_days_to_expiry":    days,
                "last_price_dollars": price,  # _get_yes_price reads this key
            })

        if len(markets) < 2:
            return []

        prefix = group_key.split(":")[0]
        series = prefix.split("-")[0] if "-" in prefix else prefix

        if direction == "below":
            return self._check_monotonicity(group_key, markets, series)
        else:
            return self._check_above_monotonicity(group_key, markets, series)

    # ── Monotonicity checkers ─────────────────────────────────────────────────

    def _check_monotonicity(
        self,
        group_key: str,
        markets: list[dict],
        series: str,
    ) -> list[ArbOpportunity]:
        """
        "Below" markets: P(value ≤ cap_strike) must increase with cap_strike.
        YES prices must be non-decreasing as strikes increase.

        For each potentially-underpriced hi_strike, we find the best available
        lo_strike: the one with the HIGHEST yes_price among all lower strikes.
        This maximises edge and locked profit per dollar — a non-adjacent pair
        can dominate the adjacent pair when the non-adjacent lo is more expensive.

        Trade structure:
          Buy hi_strike YES  (underpriced leg)
          Buy lo_strike NO   (hedge = 1 − lo_yes_price)
          Combined cost < $1.00, guaranteed $1.00 payout.
        """
        sorted_markets = sorted(markets, key=lambda m: m["_strike"])
        opps = []

        for i in range(1, len(sorted_markets)):
            hi     = sorted_markets[i]
            hi_yes = self._get_yes_price(hi)
            if hi_yes is None or hi_yes <= 0.0:
                continue

            # ── Find best lo: highest yes_price among ALL lower strikes ───────
            best_lo     = None
            best_lo_yes = -1.0
            for j in range(i):
                lo     = sorted_markets[j]
                lo_yes = self._get_yes_price(lo)
                if lo_yes is None or lo_yes <= 0.0:
                    continue
                if lo_yes > best_lo_yes:
                    best_lo_yes = lo_yes
                    best_lo     = lo

            if best_lo is None:
                continue

            # Violation: hi_yes < best_lo_yes (higher cap_strike is cheaper)
            violation = best_lo_yes - hi_yes   # positive = violation
            if violation > _MAX_EDGE:
                log.debug(
                    "StrikeArb(below): %s edge=%.3f exceeds max=%.2f — "
                    "likely stale demo price, not real arb",
                    hi["ticker"], violation, _MAX_EDGE,
                )
                continue
            if violation > _MIN_EDGE:
                hedge_price = round(1.0 - best_lo_yes, 4)
                opp = ArbOpportunity(
                    ticker          = hi["ticker"],
                    side            = "yes",
                    market_price    = hi_yes,
                    fair_value      = best_lo_yes,
                    edge            = violation,
                    series          = series,
                    lower_strike    = best_lo["_strike"],
                    upper_strike    = hi["_strike"],
                    days_to_expiry  = hi.get("_days_to_expiry", 999.0),
                    direction       = "below",
                    notes           = (
                        f"P(≤{hi['_strike']})={hi_yes:.3f} < "
                        f"P(≤{best_lo['_strike']})={best_lo_yes:.3f} — "
                        f"higher cap underpriced by {violation:.3f}"
                    ),
                    hedge_ticker    = best_lo["ticker"],
                    hedge_price     = hedge_price,
                )
                opps.append(opp)
                combined_cost = hi_yes + hedge_price
                log.debug(
                    "STRIKE ARB (below): %s YES@%.3f + %s NO@%.3f = combined $%.3f | "
                    "edge=%.3f locked_profit=%.1f%% | %s",
                    hi["ticker"], hi_yes,
                    best_lo["ticker"], hedge_price,
                    combined_cost,
                    violation,
                    opp.locked_profit_pct * 100,
                    opp.notes,
                )

        return opps

    def _check_above_monotonicity(
        self,
        group_key: str,
        markets: list[dict],
        series: str,
    ) -> list[ArbOpportunity]:
        """
        "Above" markets: P(value ≥ floor_strike) must decrease with floor_strike.
        YES prices must be non-increasing as floor_strikes increase.

        Rationale: every outcome where value ≥ X2 is also an outcome where
        value ≥ X1 (when X1 < X2), so P(≥X1) ≥ P(≥X2) always.

        For each potentially-underpriced lo_floor, we find the best available
        hi_floor: the one with the HIGHEST yes_price among all higher strikes.
        A higher hi_floor yes_price → larger violation and more locked profit.

        Trade structure:
          Buy lo_floor YES  (underpriced leg: lower floor, should be more likely)
          Buy hi_floor NO   (hedge = 1 − hi_yes_price)
          Combined cost < $1.00, guaranteed $1.00 payout.

        Settlement proof:
          • value ≥ hi_floor:  lo YES pays $1, hi NO pays $0  → $1 ✓
          • lo_floor ≤ v < hi_floor:  lo YES pays $1, hi NO pays $1  → $2 ✓
          • value < lo_floor:  lo YES pays $0, hi NO pays $1  → $1 ✓
        """
        sorted_markets = sorted(markets, key=lambda m: m["_strike"])
        opps = []

        for i in range(len(sorted_markets) - 1):
            lo     = sorted_markets[i]
            lo_yes = self._get_yes_price(lo)
            if lo_yes is None or lo_yes <= 0.0:
                continue

            # ── Find best hi: highest yes_price among ALL higher floor_strikes ─
            # Higher hi_yes → bigger violation → larger locked profit.
            best_hi     = None
            best_hi_yes = -1.0
            for j in range(i + 1, len(sorted_markets)):
                hi     = sorted_markets[j]
                hi_yes = self._get_yes_price(hi)
                if hi_yes is None or hi_yes <= 0.0:
                    continue
                if hi_yes > best_hi_yes:
                    best_hi_yes = hi_yes
                    best_hi     = hi

            if best_hi is None:
                continue

            # Expected: lo_yes >= hi_yes (lower floor = more likely)
            # Violation: lo_yes < hi_yes — lower floor is underpriced
            violation = best_hi_yes - lo_yes   # positive = violation
            if violation > _MAX_EDGE:
                log.debug(
                    "StrikeArb(above): %s edge=%.3f exceeds max=%.2f — "
                    "likely stale demo price, not real arb",
                    lo["ticker"], violation, _MAX_EDGE,
                )
                continue
            if violation > _MIN_EDGE:
                hedge_price = round(1.0 - best_hi_yes, 4)
                opp = ArbOpportunity(
                    ticker          = lo["ticker"],
                    side            = "yes",
                    market_price    = lo_yes,
                    fair_value      = best_hi_yes,
                    edge            = violation,
                    series          = series,
                    lower_strike    = lo["_strike"],
                    upper_strike    = best_hi["_strike"],
                    days_to_expiry  = lo.get("_days_to_expiry", 999.0),
                    direction       = "above",
                    notes           = (
                        f"P(≥{lo['_strike']})={lo_yes:.3f} < "
                        f"P(≥{best_hi['_strike']})={best_hi_yes:.3f} — "
                        f"lower floor underpriced by {violation:.3f}"
                    ),
                    hedge_ticker    = best_hi["ticker"],
                    hedge_price     = hedge_price,
                )
                opps.append(opp)
                combined_cost = lo_yes + hedge_price
                log.debug(
                    "STRIKE ARB (above): %s YES@%.3f + %s NO@%.3f = combined $%.3f | "
                    "edge=%.3f locked_profit=%.1f%% | %s",
                    lo["ticker"], lo_yes,
                    best_hi["ticker"], hedge_price,
                    combined_cost,
                    violation,
                    opp.locked_profit_pct * 100,
                    opp.notes,
                )

        return opps

    def _get_yes_price(self, market: dict) -> Optional[float]:
        """Extract YES mid-price from market data."""
        for key in ("yes_bid_dollars", "yes_ask_dollars", "last_price_dollars"):
            v = market.get(key)
            if v is not None:
                try:
                    return float(v)
                except (TypeError, ValueError):
                    pass
        return None
