"""
sum_arb.py — Sum-to-1 Arbitrage Scanner

Kalshi hosts "multivariate event" (MVE) markets where multiple YES/NO
markets each represent one mutually exclusive outcome of a single event.
At fair value the YES prices must sum to exactly 1.00.

When they sum to less than 1.00 by a sufficient margin, buying every leg
as a taker guarantees a profit at settlement regardless of which outcome
resolves, because exactly one leg pays $1.00 per contract.

Example (hypothetical KXFED June meeting):
  "Fed cuts 50bps"  YES = $0.25
  "Fed cuts 25bps"  YES = $0.30
  "Fed holds"       YES = $0.20
  "Fed raises 25bps" YES = $0.15
  ─────────────────────────────
  Sum = $0.90   →   buy all 4 legs for $0.90 total
  Settlement:   exactly one pays $1.00
  Net profit:   $0.10 / contract pair  (11.1% return)

Grouping
────────
Markets are grouped by `mve_collection_ticker`, Kalshi's native field for
multi-outcome event collections.  Threshold markets (tickers ending in
-T<number>) are explicitly excluded — they are cumulative probabilities,
not mutually exclusive outcomes, and do NOT satisfy the sum-to-1 property.

Guards
──────
• Sum must be ≤ (1.0 - MIN_SUM_PROFIT) before flagging an opportunity.
• OB liquidity is required for ALL legs before any orders are placed.
• Groups with > MAX_LEGS markets are skipped (execution risk too high).
• Each leg must have volume ≥ MIN_LEG_VOLUME (price is real, not stale).
• Enabled only when ENABLE_SUM_ARB=true in .env (off by default).
"""
from __future__ import annotations

import asyncio
import logging
import re
from dataclasses import dataclass, field
from decimal import Decimal
from typing import Optional

log = logging.getLogger(__name__)

# ── Tunables ──────────────────────────────────────────────────────────────────
_MIN_SUM_PROFIT:  float = 0.08   # require ≥ 8¢ guaranteed profit per dollar
_MAX_LEGS:        int   = 5      # skip groups with more than 5 outcomes
_MIN_LEGS:        int   = 2      # need at least 2 outcomes to form a group
_MIN_LEG_VOLUME:  int   = 30     # skip legs with < 30 contracts traded (stale price)
_MAX_SINGLE_PRICE: float = 0.97  # skip any leg priced above 97¢ (near-certain winner)


# ═══════════════════════════════════════════════════════════════════════════════
#  Data classes
# ═══════════════════════════════════════════════════════════════════════════════

@dataclass
class SumArbLeg:
    ticker:      str
    yes_price:   float   # current YES price used to detect the opportunity
    hedge_side:  str = "yes"  # always buying YES on each leg


@dataclass
class SumArbOpportunity:
    collection_ticker: str           # mve_collection_ticker grouping key
    legs:              list[SumArbLeg]
    yes_sum:           float          # sum of YES prices across all legs
    guaranteed_profit: float          # 1.0 - yes_sum  (per contract, pre-spread)

    @property
    def n_legs(self) -> int:
        return len(self.legs)

    @property
    def profit_pct(self) -> float:
        """Guaranteed return as a percentage of combined cost."""
        return self.guaranteed_profit / self.yes_sum if self.yes_sum > 0 else 0.0

    def __repr__(self) -> str:
        legs_str = " + ".join(f"{l.ticker}@{l.yes_price:.2f}" for l in self.legs)
        return (
            f"SumArb[{self.collection_ticker}] "
            f"sum={self.yes_sum:.3f} profit={self.guaranteed_profit:.3f} "
            f"({self.profit_pct*100:.1f}%) | {legs_str}"
        )


# ═══════════════════════════════════════════════════════════════════════════════
#  Scanner
# ═══════════════════════════════════════════════════════════════════════════════

class SumArbScanner:
    """
    Periodically scans Kalshi MVE collections for sum-to-1 violations.

    Runs as a background task in bot_engine (disabled unless
    cfg.enable_sum_arb is True).
    """

    def __init__(self, sess, cfg) -> None:
        self._sess = sess
        self._cfg  = cfg
        self._opportunities: asyncio.Queue = asyncio.Queue(maxsize=50)

    @property
    def opportunities(self) -> asyncio.Queue:
        return self._opportunities

    # ── public ────────────────────────────────────────────────────────────────

    async def scan_once(self) -> list[SumArbOpportunity]:
        """
        Single scan pass.  Fetches all open markets, groups by
        mve_collection_ticker, and checks sum-to-1 condition.
        """
        try:
            resp    = await self._sess.get("/markets", params={"status": "open", "limit": "1000"})
            markets = resp.get("markets") or []
        except Exception as exc:
            log.debug("SumArb: market fetch failed: %s", exc)
            return []

        # ── Group by mve_collection_ticker ────────────────────────────────────
        groups: dict[str, list[dict]] = {}
        for m in markets:
            coll = m.get("mve_collection_ticker") or ""
            if not coll:
                continue                          # not an MVE market
            ticker = m.get("ticker", "")
            if re.search(r"-T[\d.]+$", ticker.upper()):
                continue                          # threshold / cumulative — skip
            if m.get("status", "").lower() not in ("open", "active"):
                continue
            raw_vol = m.get("volume") or m.get("volume_24h")
            if raw_vol is not None:
                try:
                    if int(raw_vol) < _MIN_LEG_VOLUME:
                        continue
                except (TypeError, ValueError):
                    pass
            groups.setdefault(coll, []).append(m)

        log.debug("SumArb: %d MVE collections found", len(groups))

        opps: list[SumArbOpportunity] = []
        for coll, group_markets in groups.items():
            opp = self._check_group(coll, group_markets)
            if opp:
                opps.append(opp)

        if opps:
            opps.sort(key=lambda o: o.guaranteed_profit, reverse=True)
            log.debug(
                "SumArb: %d opportunities found: %s",
                len(opps), [repr(o) for o in opps],
            )

        return opps

    async def scan_loop(self, interval_seconds: int = 30) -> None:
        """Background task — scan every interval_seconds."""
        log.info("SumArbScanner: starting scan loop (interval=%ds)", interval_seconds)
        while True:
            await asyncio.sleep(interval_seconds)
            try:
                opps = await self.scan_once()
                for opp in opps:
                    try:
                        self._opportunities.put_nowait(opp)
                    except asyncio.QueueFull:
                        log.debug("SumArb queue full — dropping opportunity")
            except asyncio.CancelledError:
                return
            except Exception as exc:
                log.debug("SumArb scan loop error: %s", exc)

    # ── internals ─────────────────────────────────────────────────────────────

    def _check_group(
        self,
        coll: str,
        markets: list[dict],
    ) -> Optional[SumArbOpportunity]:
        """
        Check whether the YES prices across this MVE group sum below threshold.
        Returns an opportunity if they do, None otherwise.
        """
        if not (_MIN_LEGS <= len(markets) <= _MAX_LEGS):
            log.debug(
                "SumArb: skipping %s — %d legs (need %d–%d)",
                coll, len(markets), _MIN_LEGS, _MAX_LEGS,
            )
            return None

        legs: list[SumArbLeg] = []
        for m in markets:
            price = self._get_yes_price(m)
            if price is None or price <= 0.0:
                log.debug("SumArb: %s has no yes price — skip group %s", m.get("ticker"), coll)
                return None   # can't evaluate the group without all prices
            if price > _MAX_SINGLE_PRICE:
                log.debug(
                    "SumArb: %s YES=%.3f near certainty — skip group %s",
                    m.get("ticker"), price, coll,
                )
                return None   # one leg at 97¢+ means near-certain winner; skip
            legs.append(SumArbLeg(ticker=m.get("ticker", ""), yes_price=price))

        yes_sum = sum(l.yes_price for l in legs)
        guaranteed_profit = 1.0 - yes_sum

        if guaranteed_profit < _MIN_SUM_PROFIT:
            log.debug(
                "SumArb: %s sum=%.3f profit=%.3f < min=%.3f — not enough edge",
                coll, yes_sum, guaranteed_profit, _MIN_SUM_PROFIT,
            )
            return None

        # Sanity: sum must be < 1.0 AND > 0 — values outside suggest non-exhaustive group
        if yes_sum >= 1.0 or yes_sum <= 0.0:
            log.debug("SumArb: %s sum=%.3f out of range — skip", coll, yes_sum)
            return None

        opp = SumArbOpportunity(
            collection_ticker  = coll,
            legs               = legs,
            yes_sum            = yes_sum,
            guaranteed_profit  = guaranteed_profit,
        )
        log.info("SUM ARB: %s", repr(opp))
        return opp

    def _get_yes_price(self, market: dict) -> Optional[float]:
        """
        Extract YES ask price (cost to BUY YES immediately).

        We prefer ask > last > bid because we're buying every leg as a taker.
        Using bid prices would make the sum appear lower than the true execution
        cost, generating phantom opportunities that fail at the OB gate.
        """
        for key in ("yes_ask_dollars", "last_price_dollars", "yes_bid_dollars"):
            v = market.get(key)
            if v is not None:
                try:
                    return float(v)
                except (TypeError, ValueError):
                    pass
        return None
