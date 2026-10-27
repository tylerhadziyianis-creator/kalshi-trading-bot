"""
trade_logger.py — Immutable Audit Trail for Every Trade Decision

Writes every detected whale, every order placed, every fill, and every
market outcome to a structured JSONL file and a summary CSV.

This is the data you need to prove (or disprove) edge before scaling up.

Log files:
    trades_YYYY-MM-DD.jsonl   — one JSON object per line, one per event
    summary.csv               — one row per completed round-trip trade

Event types:
    WHALE_DETECTED   — a qualifying whale trade was seen on WS
    ORDER_PLACED     — a limit order was submitted to Kalshi
    ORDER_FILLED     — our order filled (from sync_status or fill WS)
    ORDER_CANCELLED  — our order was cancelled/expired
    MARKET_SETTLED   — a market we traded reached settlement
    SIGNAL_SKIPPED   — whale detected but no order placed (and why)
"""
from __future__ import annotations

import csv
import json
import logging
import os
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path
from typing import Optional

log = logging.getLogger(__name__)

LOG_DIR = Path(os.getenv("TRADE_LOG_DIR", "./trade_logs"))


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _today() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%d")


@dataclass
class TradeEvent:
    event_type:       str           # WHALE_DETECTED | ORDER_PLACED | ORDER_FILLED | etc.
    ts:               str           # ISO-8601 UTC timestamp
    ticker:           str
    side:             str           # yes | no
    whale_notional:   Optional[float] = None
    whale_contracts:  Optional[int]   = None
    whale_price:      Optional[float] = None
    order_id:         Optional[str]   = None
    entry_price:      Optional[float] = None
    entry_contracts:  Optional[int]   = None
    entry_cost:       Optional[float] = None
    confidence:       Optional[float] = None
    win_probability:  Optional[float] = None
    fill_price:       Optional[float] = None
    fill_contracts:   Optional[int]   = None
    realized_pnl:     Optional[float] = None
    settlement_value: Optional[float] = None   # 1.0 = YES won, 0.0 = NO won
    skip_reason:      Optional[str]   = None
    chase_cents:      Optional[int]   = None
    post_only:        Optional[bool]  = None
    notes:            Optional[str]   = None
    # Signal analysis fields — for edge measurement after 100+ signals
    notional_ratio:   Optional[float] = None   # whale size / threshold
    days_to_expiry:   Optional[int]   = None   # days until market settles
    category:         Optional[str]   = None   # market category (FED, WTI, RT, etc.)
    conviction_score: Optional[int]   = None   # 0-10 conviction score (bypass trades)
    fund_conf:        Optional[float] = None   # fundamental model confidence


class TradeLogger:
    """
    Thread-safe (append-only file writes are atomic on POSIX) audit logger.

    Every method is non-blocking — it writes synchronously but files are
    local so latency is negligible vs network calls.
    """

    def __init__(self) -> None:
        LOG_DIR.mkdir(parents=True, exist_ok=True)
        self._summary_path = LOG_DIR / "summary.csv"
        self._ensure_summary_header()
        log.info("TradeLogger: writing to %s", LOG_DIR.resolve())

    # ── public API ────────────────────────────────────────────────────────────

    def log_whale(
        self,
        ticker: str,
        side: str,
        notional: float,
        contracts: int,
        price: float,
    ) -> None:
        self._write(TradeEvent(
            event_type      = "WHALE_DETECTED",
            ts              = _now(),
            ticker          = ticker,
            side            = side,
            whale_notional  = notional,
            whale_contracts = contracts,
            whale_price     = price,
        ))

    def log_skip(
        self,
        ticker:           str,
        side:             str,
        reason:           str,
        confidence:       float = 0.0,
        notional_ratio:   Optional[float] = None,
        days_to_expiry:   Optional[int]   = None,
        category:         Optional[str]   = None,
        conviction_score: Optional[int]   = None,
        fund_conf:        Optional[float] = None,
    ) -> None:
        self._write(TradeEvent(
            event_type       = "SIGNAL_SKIPPED",
            ts               = _now(),
            ticker           = ticker,
            side             = side,
            skip_reason      = reason,
            confidence       = confidence,
            notional_ratio   = notional_ratio,
            days_to_expiry   = days_to_expiry,
            category         = category,
            conviction_score = conviction_score,
            fund_conf        = fund_conf,
        ))

    def log_order(
        self,
        ticker:           str,
        side:             str,
        order_id:         str,
        entry_price:      float,
        contracts:        int,
        cost:             float,
        confidence:       float,
        win_prob:         float,
        chase_cents:      int,
        post_only:        bool,
        notional_ratio:   Optional[float] = None,
        days_to_expiry:   Optional[int]   = None,
        category:         Optional[str]   = None,
        conviction_score: Optional[int]   = None,
        fund_conf:        Optional[float] = None,
    ) -> None:
        self._write(TradeEvent(
            event_type       = "ORDER_PLACED",
            ts               = _now(),
            ticker           = ticker,
            side             = side,
            order_id         = order_id,
            entry_price      = entry_price,
            entry_contracts  = contracts,
            entry_cost       = cost,
            confidence       = confidence,
            win_probability  = win_prob,
            chase_cents      = chase_cents,
            post_only        = post_only,
            notional_ratio   = notional_ratio,
            days_to_expiry   = days_to_expiry,
            category         = category,
            conviction_score = conviction_score,
            fund_conf        = fund_conf,
        ))

    def log_fill(
        self,
        ticker: str,
        side: str,
        order_id: str,
        entry_price: float,
        fill_price: float,
        contracts: int,
        cost: float,
        confidence: float,
    ) -> None:
        self._write(TradeEvent(
            event_type      = "ORDER_FILLED",
            ts              = _now(),
            ticker          = ticker,
            side            = side,
            order_id        = order_id,
            entry_price     = entry_price,
            fill_price      = fill_price,
            fill_contracts  = contracts,
            entry_cost      = cost,
            confidence      = confidence,
        ))

    def log_cancel(self, ticker: str, side: str, order_id: str, reason: str = "") -> None:
        self._write(TradeEvent(
            event_type = "ORDER_CANCELLED",
            ts         = _now(),
            ticker     = ticker,
            side       = side,
            order_id   = order_id,
            notes      = reason,
        ))

    def log_settlement(
        self,
        ticker: str,
        settlement_value: float,
        our_side: Optional[str],
        entry_price: Optional[float],
        contracts: Optional[int],
        realized_pnl: Optional[float],
    ) -> None:
        self._write(TradeEvent(
            event_type       = "MARKET_SETTLED",
            ts               = _now(),
            ticker           = ticker,
            side             = our_side or "",
            settlement_value = settlement_value,
            entry_price      = entry_price,
            fill_contracts   = contracts,
            realized_pnl     = realized_pnl,
        ))
        # Also append to summary CSV for easy analysis
        if realized_pnl is not None and entry_price is not None:
            self._append_summary(
                ticker         = ticker,
                side           = our_side or "",
                entry_price    = entry_price,
                contracts      = contracts or 0,
                settlement     = settlement_value,
                pnl            = realized_pnl,
            )

    # ── internals ─────────────────────────────────────────────────────────────

    def _write(self, event: TradeEvent) -> None:
        try:
            path = LOG_DIR / f"trades_{_today()}.jsonl"
            line = json.dumps({k: v for k, v in asdict(event).items() if v is not None})
            with open(path, "a") as f:
                f.write(line + "\n")
        except Exception as exc:
            log.error("TradeLogger write failed: %s", exc)

    def _ensure_summary_header(self) -> None:
        if not self._summary_path.exists():
            with open(self._summary_path, "w", newline="") as f:
                w = csv.writer(f)
                w.writerow([
                    "ts", "ticker", "side", "entry_price", "contracts",
                    "settlement", "pnl", "won",
                ])

    def _append_summary(
        self,
        ticker: str,
        side: str,
        entry_price: float,
        contracts: int,
        settlement: float,
        pnl: float,
    ) -> None:
        try:
            won = (side == "yes" and settlement >= 0.99) or (side == "no" and settlement <= 0.01)
            with open(self._summary_path, "a", newline="") as f:
                w = csv.writer(f)
                w.writerow([_now(), ticker, side, entry_price, contracts, settlement, round(pnl, 4), int(won)])
        except Exception as exc:
            log.error("TradeLogger summary write failed: %s", exc)
