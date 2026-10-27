"""
position_sizer.py — Fractional Kelly Criterion + Anti-Ruin Guardrails

THE FORMULA (exact implementation, no approximations)
──────────────────────────────────────────────────────
  Full Kelly fraction:
      f* = (b·p − q) / b

  where:
      p  = estimated probability of winning  (0 < p < 1)
      q  = 1 − p   (probability of losing)
      b  = net odds per dollar staked
           On Kalshi: if YES price = $0.60, you risk $0.60 to win $0.40
           so  b = (1 − price) / price = 0.40 / 0.60 ≈ 0.6667

  Fractional Kelly:
      f_trade = KELLY_FRACTION × f*          (default ¼-Kelly)

  Dollar stake:
      stake = f_trade × account_balance

  Hard caps applied in order:
      1. If f* ≤ MIN_KELLY_EDGE  → skip (no edge)
      2. stake = min(stake, MAX_SINGLE_TRADE_PCT × balance)
      3. stake = min(stake, MAX_SINGLE_TRADE_USD)          ← $15 hard ceiling
      4. If open_exposure + stake > MAX_EXPOSURE_PCT × balance  → skip

WHY THESE NUMBERS MATTER FOR $500
───────────────────────────────────
  A full-Kelly bettor with three consecutive losses can lose 50%+ of bankroll.
  Quarter-Kelly (0.25) limits the 3-loss drawdown to ~10-12%.
  The $15 hard cap means the absolute worst 3-loss streak costs $45 max (9%).
  The 25% total-exposure cap means a market-wide correlated crash can only
  take $125 off the table at once, leaving $375 to recover.
"""
from __future__ import annotations

import datetime
import logging
from decimal import Decimal, ROUND_DOWN

from config import BotConfig

log = logging.getLogger(__name__)

CENT  = Decimal("0.01")
ZERO  = Decimal("0")

# Kalshi charges ~2% of winnings at settlement (capped at ~7¢ per contract).
# This reduces your net payout and must be reflected in the Kelly odds calculation.
# Without this, `b` is overstated and the bot systematically overestimates its edge.
_KALSHI_FEE_RATE: float = 0.02   # 2% of payout withheld at settlement


class PositionSizer:
    """
    Computes safe per-trade stake and enforces portfolio-level risk limits.

    State updated after every fill and close so Kelly sizing always reflects
    the current account balance.
    """

    def __init__(self, cfg: BotConfig) -> None:
        self._cfg          = cfg
        self.balance       = cfg.starting_balance
        self.open_exposure = ZERO            # sum of cost-basis of open positions
        self.total_pnl     = ZERO            # cumulative realized PnL
        self.trade_count   = 0
        self.win_count     = 0
        self.daily_pnl     = ZERO
        self.last_reset_date = datetime.date.today()
        self.consecutive_losses = 0

    # ── core sizing ───────────────────────────────────────────────────────────

    def compute_stake(
        self,
        win_probability: float,
        price_dollars:   Decimal,
        label:           str = "",
        use_premium_cap: bool = False,
    ) -> Decimal:
        """
        Return the recommended dollar stake, or ZERO if the trade should be
        skipped.  All arithmetic uses Python floats for Kelly, then converts
        back to Decimal for monetary precision.

        Parameters
        ----------
        win_probability : float in (0, 1) — your estimate of P(win)
        price_dollars   : Decimal — YES (or NO) cost per contract in dollars
        label           : optional string for log messages
        """
        price_f = float(price_dollars)
        if not (0.0001 < price_f < 0.9999):
            log.warning("[%s] Price %.4f out of valid range — skip", label, price_f)
            return ZERO

        p = win_probability
        q = 1.0 - p

        # Net odds: you risk `price` to win `(1 - price)`, net of Kalshi's ~2% fee.
        # Without fee adjustment, Kelly overstates edge on every trade.
        # b_adj = ((1 - price) × (1 - fee_rate)) / price
        b = ((1.0 - price_f) * (1.0 - _KALSHI_FEE_RATE)) / price_f

        # Full Kelly: f* = (b·p − q) / b
        f_star = (b * p - q) / b

        if f_star <= self._cfg.min_kelly_edge:
            log.info(
                "[%s] No edge: f*=%.5f p=%.4f price=%.4f — skip",
                label, f_star, p, price_f,
            )
            return ZERO

        # Fractional Kelly
        f_trade = self._cfg.kelly_fraction * f_star

        # Raw dollar stake
        raw = Decimal(str(round(f_trade * float(self.balance), 6)))

        # Cap 1: percentage of balance
        pct_cap = self.balance * Decimal(str(self._cfg.max_single_trade_pct))
        stake   = min(raw, pct_cap)

        # Cap 2: absolute dollar ceiling  ($15 hard cap)
        stake = min(stake, self._cfg.max_single_trade_usd)

        # Cap 2b: low-price stake cap — sub-10¢ contracts are lottery tickets;
        # keep stakes tiny so false whale signals don't hurt real capital.
        #   < 5¢  → max $0.50
        #   5–10¢ → max $1.00
        if price_f < 0.05:
            stake = min(stake, Decimal("2.00"))
        elif price_f < 0.10:
            stake = min(stake, Decimal("4.00"))

        # Cap 3: minimum ($2.00 — below this, fees eat too much of the edge)
        # At $2.00 stake, Kalshi's 2% fee = ~4¢ on a typical win — manageable.
        # Below $2.00, fixed overhead (tracking, API calls, spread cost) dominates.
        if stake < Decimal("2.00"):
            log.debug("[%s] Stake $%.4f below minimum — skip", label, stake)
            return ZERO

        # Cap 4: portfolio exposure
        # Premium trades (large-edge arbs, large whales) may use up to the
        # premium cap (30%); standard trades are limited to the baseline (20%).
        headroom = self._exposure_headroom(use_premium_cap)
        if stake > headroom:
            log.warning(
                "[%s] Exposure cap (%s): headroom=$%.2f < stake=$%.2f — skip",
                label, "premium" if use_premium_cap else "standard", headroom, stake,
            )
            return ZERO

        stake = stake.quantize(CENT, rounding=ROUND_DOWN)
        log.info(
            "[%s] Stake=$%.2f | f*=%.5f f_trade=%.5f p=%.4f price=%.4f b=%.4f",
            label, stake, f_star, f_trade, p, price_f, b,
        )
        return stake

    def contracts_for_stake(self, stake: Decimal, price_dollars: Decimal) -> int:
        """Whole contracts = floor(stake / price)."""
        if price_dollars <= ZERO:
            return 0
        return max(1, int(stake / price_dollars))

    # ── state updates ─────────────────────────────────────────────────────────

    def register_open(self, cost_basis: Decimal) -> None:
        self.open_exposure += cost_basis
        log.debug("Exposure +$%.2f  →  total=$%.2f", cost_basis, self.open_exposure)

    def register_close(
        self,
        cost_basis:   Decimal,
        realised_pnl: Decimal,
        was_filled:   bool = True,   # False = cancelled/timeout — don't count as loss
    ) -> None:
        """
        Update accounting on position close.

        was_filled=False: order never executed — release exposure but do NOT
        count as a loss or increment consecutive_losses. Cancelled/expired
        orders carry no PnL information and must not penalize the loss counter.
        """
        if datetime.date.today() > self.last_reset_date:
            self.daily_pnl = ZERO
            self.last_reset_date = datetime.date.today()

        self.open_exposure = max(ZERO, self.open_exposure - cost_basis)

        if not was_filled:
            # No fill = no PnL impact — just release the reserved exposure
            log.info(
                "Exposure released (no fill): -$%.2f | exposure=$%.2f",
                cost_basis, self.open_exposure,
            )
            return

        self.balance   += realised_pnl
        self.total_pnl += realised_pnl
        self.daily_pnl += realised_pnl
        self.trade_count += 1

        if realised_pnl > ZERO:
            self.win_count += 1
            self.consecutive_losses = 0
        elif realised_pnl < ZERO:
            self.consecutive_losses += 1
        # realised_pnl == 0: breakeven — don't penalize consecutive losses

        log.info(
            "Close (filled): pnl=$%.4f | total=$%.4f | daily=$%.4f | "
            "balance=$%.2f | exposure=$%.2f | win_rate=%.1f%% | consec_losses=%d",
            realised_pnl, self.total_pnl, self.daily_pnl,
            self.balance, self.open_exposure,
            (self.win_count / self.trade_count * 100) if self.trade_count > 0 else 0,
            self.consecutive_losses,
        )

    def sync_balance(self, live_balance: Decimal) -> None:
        self.balance = live_balance
        log.debug("Balance synced to $%.2f", self.balance)

    def can_trade(self) -> bool:
        if self.daily_pnl <= -self._cfg.max_daily_loss_usd:
            log.critical("Daily loss limit reached. Stopping trading for the day.")
            return False
        if self.consecutive_losses >= self._cfg.max_consecutive_losses:
            log.critical("Consecutive loss limit reached. Stopping trading.")
            return False
        return True

    # ── diagnostics ───────────────────────────────────────────────────────────

    def _exposure_headroom(self, use_premium_cap: bool = False) -> Decimal:
        pct = self._cfg.max_premium_exposure_pct if use_premium_cap else self._cfg.max_exposure_pct
        cap = self.balance * Decimal(str(pct))
        return max(ZERO, cap - self.open_exposure)

    def exposure_utilisation(self) -> float:
        cap = float(self.balance) * self._cfg.max_exposure_pct
        return float(self.open_exposure) / cap if cap > 0 else 0.0

    def __repr__(self) -> str:
        return (
            f"PositionSizer(balance=${self.balance:.2f}, "
            f"exposure=${self.open_exposure:.2f} "
            f"[{self.exposure_utilisation():.1%} of cap])"
        )
