#!/usr/bin/env python3
"""
backtester.py — Historical Signal Replay & Parameter Sensitivity Analysis

Reads trade_logs/trades_YYYY-MM-DD.jsonl files produced by trade_logger.py,
replays every detected whale trade through the current signal pipeline,
and measures what PnL would have been under different parameter settings.

Usage:
    # Basic backtest on all available logs
    python backtester.py

    # Backtest specific date range
    python backtester.py --from 2026-01-01 --to 2026-01-31

    # Parameter sweep (finds optimal MIN_MISPRICING and MIN_WHALE_CONFIDENCE)
    python backtester.py --sweep

    # Show skip reason breakdown
    python backtester.py --skips

Output files (written to ./backtest_results/):
    summary.txt          — overall PnL, win rate, Sharpe estimate
    trades.csv           — per-trade results
    param_sweep.csv      — parameter sensitivity grid (if --sweep)
    skip_analysis.txt    — why trades were rejected (if --skips)
"""
from __future__ import annotations

import argparse
import csv
import json
import os
import sys
from collections import defaultdict
from dataclasses import dataclass, field
from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path
from typing import Optional

# ── Local imports ─────────────────────────────────────────────────────────────
sys.path.insert(0, str(Path(__file__).parent))
from fundamental_model import get_fundamental_estimate, detect_category

LOG_DIR     = Path(os.getenv("TRADE_LOG_DIR", "./trade_logs"))
OUTPUT_DIR  = Path("./backtest_results")


# ── Data structures ───────────────────────────────────────────────────────────

@dataclass
class LoggedWhale:
    ts:           str
    ticker:       str
    side:         str
    notional:     float
    contracts:    int
    whale_price:  float


@dataclass
class LoggedSettlement:
    ts:               str
    ticker:           str
    side:             str
    settlement_value: float
    entry_price:      float
    contracts:        int
    realized_pnl:     float


@dataclass
class BacktestTrade:
    ticker:           str
    side:             str
    entry_price:      float
    contracts:        int
    settlement_value: float
    realized_pnl:     float
    whale_conf:       float
    fund_conf:        float
    composite_conf:   float
    mispricing:       float
    category:         str
    skip_reason:      Optional[str]


@dataclass
class BacktestParams:
    min_whale_confidence: float = 0.30
    min_mispricing:       float = 0.02
    min_composite_conf:   float = 0.30
    fundamental_weight:   float = 0.35
    market_weight:        float = 0.15
    agreement_bonus:      float = 0.20


@dataclass
class BacktestResult:
    params:             BacktestParams
    total_trades:       int   = 0
    winning_trades:     int   = 0
    total_pnl:          float = 0.0
    total_wagered:      float = 0.0
    skipped:            int   = 0
    skip_reasons:       dict  = field(default_factory=dict)
    trades:             list  = field(default_factory=list)

    @property
    def win_rate(self) -> float:
        return self.winning_trades / self.total_trades if self.total_trades else 0.0

    @property
    def roi(self) -> float:
        return self.total_pnl / self.total_wagered if self.total_wagered else 0.0

    @property
    def avg_pnl(self) -> float:
        return self.total_pnl / self.total_trades if self.total_trades else 0.0

    def sharpe_estimate(self) -> float:
        """Rough Sharpe ratio from per-trade PnL distribution."""
        if len(self.trades) < 3:
            return 0.0
        pnls = [t.realized_pnl for t in self.trades if not t.skip_reason]
        if not pnls:
            return 0.0
        mean = sum(pnls) / len(pnls)
        variance = sum((p - mean) ** 2 for p in pnls) / len(pnls)
        std = variance ** 0.5
        return (mean / std) if std > 0 else 0.0


# ── Log reader ────────────────────────────────────────────────────────────────

def load_logs(from_date: Optional[str] = None, to_date: Optional[str] = None) -> tuple[list, list]:
    """Load whale events and settlement events from JSONL logs."""
    whales:      list[LoggedWhale]      = []
    settlements: list[LoggedSettlement] = []

    if not LOG_DIR.exists():
        print(f"No log directory found at {LOG_DIR}")
        print("Run the bot with TRADE_LOG_DIR set to generate logs.")
        return whales, settlements

    jsonl_files = sorted(LOG_DIR.glob("trades_*.jsonl"))
    if not jsonl_files:
        print(f"No trade log files found in {LOG_DIR}")
        return whales, settlements

    for path in jsonl_files:
        date_str = path.stem.replace("trades_", "")
        if from_date and date_str < from_date:
            continue
        if to_date and date_str > to_date:
            continue

        with open(path) as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                try:
                    event = json.loads(line)
                except json.JSONDecodeError:
                    continue

                etype = event.get("event_type", "")

                if etype == "WHALE_DETECTED":
                    whales.append(LoggedWhale(
                        ts          = event.get("ts", ""),
                        ticker      = event.get("ticker", ""),
                        side        = event.get("side", "yes"),
                        notional    = float(event.get("whale_notional") or 0),
                        contracts   = int(event.get("whale_contracts") or 0),
                        whale_price = float(event.get("whale_price") or 0),
                    ))

                elif etype == "MARKET_SETTLED":
                    settlements.append(LoggedSettlement(
                        ts               = event.get("ts", ""),
                        ticker           = event.get("ticker", ""),
                        side             = event.get("side", "yes"),
                        settlement_value = float(event.get("settlement_value") or 0),
                        entry_price      = float(event.get("entry_price") or 0),
                        contracts        = int(event.get("fill_contracts") or 0),
                        realized_pnl     = float(event.get("realized_pnl") or 0),
                    ))

    print(f"Loaded {len(whales)} whale events, {len(settlements)} settlements "
          f"from {len(jsonl_files)} log files")
    return whales, settlements


# ── Signal simulation ─────────────────────────────────────────────────────────

def simulate_signal(
    whale: LoggedWhale,
    params: BacktestParams,
    whale_conf: float = 0.50,   # default for replay (no live registry)
) -> BacktestTrade:
    """
    Replay a single whale event through the signal pipeline.
    Uses the fundamental model but skips live whale registry
    (we don't have historical whale profiles in replay mode).
    """
    market_price = whale.whale_price
    ticker       = whale.ticker
    category     = detect_category(ticker)

    # Fundamental estimate
    fund = get_fundamental_estimate(ticker, market_price)

    # Composite probability (mirrors signal_engine logic)
    whale_adjustment = whale_conf * 0.10 * (1.0 - market_price)
    whale_p = (market_price + whale_adjustment if whale.side == "yes"
               else market_price - whale_adjustment)
    whale_p = max(0.05, min(0.95, whale_p))

    w_whale = whale_conf * (1 - params.fundamental_weight - params.market_weight)
    w_fund  = fund.confidence * params.fundamental_weight
    w_mkt   = params.market_weight
    total_w = w_whale + w_fund + w_mkt or 1.0

    composite_p = (whale_p * w_whale + fund.p_fair * w_fund + market_price * w_mkt) / total_w
    composite_p = max(0.05, min(0.95, composite_p))

    spread_3 = max(
        abs(whale_p - fund.p_fair),
        abs(whale_p - market_price),
        abs(fund.p_fair - market_price),
    )
    agreement = max(0.0, 1.0 - (spread_3 / 0.15))

    import math
    base_conf = math.sqrt(whale_conf * max(0.01, fund.confidence))
    composite_conf = min(1.0, base_conf + params.agreement_bonus * agreement)

    mispricing = composite_p - market_price if whale.side == "yes" else market_price - (1 - composite_p)

    # Apply gates
    skip_reason = None
    if whale_conf < params.min_whale_confidence:
        skip_reason = f"whale_conf_too_low ({whale_conf:.3f})"
    elif mispricing < params.min_mispricing:
        skip_reason = f"insufficient_mispricing ({mispricing:.4f})"
    elif composite_conf < params.min_composite_conf:
        skip_reason = f"composite_conf_too_low ({composite_conf:.3f})"

    return BacktestTrade(
        ticker         = ticker,
        side           = whale.side,
        entry_price    = market_price,
        contracts      = whale.contracts,
        settlement_value = 0.0,   # filled in later from settlement log
        realized_pnl   = 0.0,
        whale_conf     = whale_conf,
        fund_conf      = fund.confidence,
        composite_conf = composite_conf,
        mispricing     = mispricing,
        category       = category,
        skip_reason    = skip_reason,
    )


# ── Backtest runner ───────────────────────────────────────────────────────────

def run_backtest(
    whales:      list[LoggedWhale],
    settlements: list[LoggedSettlement],
    params:      BacktestParams,
    default_whale_conf: float = 0.50,
) -> BacktestResult:
    result = BacktestResult(params=params)

    # Build settlement lookup: ticker → LoggedSettlement
    settlement_map: dict[str, LoggedSettlement] = {}
    for s in settlements:
        settlement_map[s.ticker] = s

    seen_tickers: set[str] = set()

    for whale in whales:
        trade = simulate_signal(whale, params, whale_conf=default_whale_conf)

        if trade.skip_reason:
            result.skipped += 1
            result.skip_reasons[trade.skip_reason] = result.skip_reasons.get(trade.skip_reason, 0) + 1
            continue

        # Only trade each ticker once per backtest run (avoid double-counting)
        if whale.ticker in seen_tickers:
            continue
        seen_tickers.add(whale.ticker)

        # Attach settlement outcome
        settlement = settlement_map.get(whale.ticker)
        if settlement:
            trade.settlement_value = settlement.settlement_value
            trade.realized_pnl     = settlement.realized_pnl
            result.total_trades   += 1
            result.total_wagered  += abs(trade.entry_price * trade.contracts)
            result.total_pnl      += trade.realized_pnl
            if trade.realized_pnl > 0:
                result.winning_trades += 1
            result.trades.append(trade)
        # If no settlement found, trade is still open — skip PnL attribution

    return result


# ── Output formatters ─────────────────────────────────────────────────────────

def print_summary(result: BacktestResult) -> None:
    p = result.params
    print("\n" + "═" * 60)
    print("BACKTEST SUMMARY")
    print("═" * 60)
    print(f"  Parameters:")
    print(f"    MIN_WHALE_CONFIDENCE : {p.min_whale_confidence:.2f}")
    print(f"    MIN_MISPRICING       : {p.min_mispricing:.3f}")
    print(f"    MIN_COMPOSITE_CONF   : {p.min_composite_conf:.2f}")
    print(f"    FUNDAMENTAL_WEIGHT   : {p.fundamental_weight:.2f}")
    print()
    print(f"  Results:")
    print(f"    Settled trades       : {result.total_trades}")
    print(f"    Skipped signals      : {result.skipped}")
    print(f"    Win rate             : {result.win_rate:.1%}")
    print(f"    Total PnL            : ${result.total_pnl:.4f}")
    print(f"    Total wagered        : ${result.total_wagered:.2f}")
    print(f"    ROI                  : {result.roi:.2%}")
    print(f"    Avg PnL/trade        : ${result.avg_pnl:.4f}")
    print(f"    Sharpe estimate      : {result.sharpe_estimate():.3f}")
    print()
    if result.skip_reasons:
        print("  Skip reasons:")
        for reason, count in sorted(result.skip_reasons.items(), key=lambda x: -x[1]):
            print(f"    {reason:<45} {count:>4}")
    print("═" * 60)


def save_trades_csv(result: BacktestResult, path: Path) -> None:
    with open(path, "w", newline="") as f:
        w = csv.writer(f)
        w.writerow(["ticker", "side", "category", "entry_price", "contracts",
                    "settlement", "pnl", "whale_conf", "fund_conf",
                    "composite_conf", "mispricing", "won"])
        for t in result.trades:
            w.writerow([
                t.ticker, t.side, t.category,
                round(t.entry_price, 4), t.contracts,
                round(t.settlement_value, 2), round(t.realized_pnl, 4),
                round(t.whale_conf, 3), round(t.fund_conf, 3),
                round(t.composite_conf, 3), round(t.mispricing, 4),
                int(t.realized_pnl > 0),
            ])


def run_param_sweep(
    whales: list[LoggedWhale],
    settlements: list[LoggedSettlement],
) -> None:
    """Grid search over key parameters to find optimal settings."""
    OUTPUT_DIR.mkdir(exist_ok=True)
    results = []

    mispricing_values     = [0.01, 0.015, 0.02, 0.025, 0.03, 0.04, 0.05]
    whale_conf_values     = [0.20, 0.25, 0.30, 0.35, 0.40, 0.45]
    fundamental_weights   = [0.25, 0.35, 0.45, 0.55]

    total = len(mispricing_values) * len(whale_conf_values) * len(fundamental_weights)
    done  = 0

    for mw in mispricing_values:
        for wc in whale_conf_values:
            for fw in fundamental_weights:
                params = BacktestParams(
                    min_whale_confidence = wc,
                    min_mispricing       = mw,
                    fundamental_weight   = fw,
                )
                r = run_backtest(whales, settlements, params)
                results.append({
                    "min_mispricing":       mw,
                    "min_whale_confidence": wc,
                    "fundamental_weight":   fw,
                    "trades":               r.total_trades,
                    "win_rate":             round(r.win_rate, 4),
                    "roi":                  round(r.roi, 4),
                    "total_pnl":            round(r.total_pnl, 4),
                    "sharpe":               round(r.sharpe_estimate(), 4),
                    "skipped":              r.skipped,
                })
                done += 1
                if done % 20 == 0:
                    print(f"  Sweep: {done}/{total} combinations tested...")

    # Sort by Sharpe (best risk-adjusted return)
    results.sort(key=lambda x: x["sharpe"], reverse=True)

    sweep_path = OUTPUT_DIR / "param_sweep.csv"
    with open(sweep_path, "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=list(results[0].keys()))
        w.writeheader()
        w.writerows(results)

    print(f"\nParameter sweep saved to {sweep_path}")
    print("\nTop 5 parameter combinations (by Sharpe):")
    print(f"  {'mispricing':>12} {'whale_conf':>12} {'fund_weight':>12} "
          f"{'win_rate':>10} {'roi':>10} {'sharpe':>10} {'trades':>8}")
    for r in results[:5]:
        print(f"  {r['min_mispricing']:>12.3f} {r['min_whale_confidence']:>12.2f} "
              f"{r['fundamental_weight']:>12.2f} {r['win_rate']:>10.1%} "
              f"{r['roi']:>10.2%} {r['sharpe']:>10.3f} {r['trades']:>8}")

    print(f"\n→ Recommended: set .env to top row's values after reviewing the full sweep CSV")


# ── CLI ───────────────────────────────────────────────────────────────────────

def main() -> None:
    parser = argparse.ArgumentParser(description="Kalshi bot backtester")
    parser.add_argument("--from", dest="from_date", default=None,
                        help="Start date YYYY-MM-DD")
    parser.add_argument("--to", dest="to_date", default=None,
                        help="End date YYYY-MM-DD")
    parser.add_argument("--sweep", action="store_true",
                        help="Run parameter sensitivity sweep")
    parser.add_argument("--skips", action="store_true",
                        help="Show detailed skip reason breakdown")
    parser.add_argument("--whale-conf", type=float, default=0.50,
                        help="Default whale confidence for replay (0.0-1.0)")
    args = parser.parse_args()

    OUTPUT_DIR.mkdir(exist_ok=True)

    whales, settlements = load_logs(args.from_date, args.to_date)

    if not whales and not settlements:
        print("\nNo data to backtest. Generate logs by running the bot first.")
        print("Logs are written to ./trade_logs/trades_YYYY-MM-DD.jsonl")
        print("\nTo generate synthetic test data, run:")
        print("  python backtester.py --demo")
        return

    if args.sweep:
        print(f"\nRunning parameter sweep over {len(whales)} whale events...")
        run_param_sweep(whales, settlements)
        return

    # Single backtest with defaults
    params = BacktestParams()
    result = run_backtest(whales, settlements, params, default_whale_conf=args.whale_conf)
    print_summary(result)

    if result.trades:
        trades_path = OUTPUT_DIR / "trades.csv"
        save_trades_csv(result, trades_path)
        print(f"\nTrade-level results saved to {trades_path}")

    if args.skips:
        print("\nDetailed skip analysis:")
        skip_by_cat: dict[str, dict] = defaultdict(lambda: defaultdict(int))
        for whale in whales:
            trade = simulate_signal(whale, params, default_whale_conf=args.whale_conf)
            if trade.skip_reason:
                skip_by_cat[trade.category][trade.skip_reason] += 1
        for cat, reasons in sorted(skip_by_cat.items()):
            print(f"\n  {cat}:")
            for reason, count in sorted(reasons.items(), key=lambda x: -x[1]):
                print(f"    {reason:<50} {count:>4}")

    # Category breakdown
    if result.trades:
        by_cat: dict[str, list] = defaultdict(list)
        for t in result.trades:
            by_cat[t.category].append(t)
        print("\nResults by category:")
        print(f"  {'Category':<12} {'Trades':>7} {'Win%':>7} {'PnL':>10} {'ROI':>8}")
        for cat, trades in sorted(by_cat.items()):
            n     = len(trades)
            wins  = sum(1 for t in trades if t.realized_pnl > 0)
            pnl   = sum(t.realized_pnl for t in trades)
            wag   = sum(abs(t.entry_price * t.contracts) for t in trades)
            roi   = pnl / wag if wag else 0
            print(f"  {cat:<12} {n:>7} {wins/n:>7.1%} ${pnl:>9.4f} {roi:>8.2%}")


if __name__ == "__main__":
    main()
