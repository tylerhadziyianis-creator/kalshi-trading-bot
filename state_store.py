"""
state_store.py — Persistent Bot State (Positions + Orders)

Saves open positions and order metadata to a JSON file on every update.
On restart, BotEngine loads this file and reconnects in-flight positions
to the settlement watcher, preventing silent PnL loss after a crash.

The store is intentionally simple — it only tracks what's needed to resume:
  • open_positions: {kalshi_id → position dict}
  • open_orders:    {kalshi_id → {ticker, side, price, contracts, cost_basis}}
"""
from __future__ import annotations

import json
import logging
import os
from decimal import Decimal
from pathlib import Path
from typing import Any

log = logging.getLogger(__name__)


def _decimal_default(obj):
    if isinstance(obj, Decimal):
        return str(obj)
    raise TypeError(f"Not serializable: {type(obj)}")


def _revive(d: Any) -> Any:
    """Best-effort Decimal revival for known monetary fields."""
    if isinstance(d, dict):
        out = {}
        for k, v in d.items():
            if k in (
                "entry_price", "cost_basis", "price",
                "base_price", "yes_price", "no_price",
            ):
                try:
                    v = Decimal(str(v))
                except Exception:
                    pass
            else:
                v = _revive(v)
            out[k] = v
        return out
    if isinstance(d, list):
        return [_revive(i) for i in d]
    return d


class StateStore:
    """
    Thin read/write wrapper around a single JSON state file.

    Usage:
        store = StateStore("./bot_state.json")
        store.load()

        store.upsert_position(kalshi_id, pos_dict)
        store.remove_position(kalshi_id)
        store.upsert_order(kalshi_id, order_dict)
        store.remove_order(kalshi_id)

        # Recover on restart:
        positions = store.open_positions   # dict[str, dict]
        orders    = store.open_orders      # dict[str, dict]
    """

    def __init__(self, path: str = "./bot_state.json") -> None:
        self._path = Path(path)
        self.open_positions: dict[str, dict] = {}
        self.open_orders:    dict[str, dict] = {}

    def load(self) -> None:
        if not self._path.exists():
            log.info("StateStore: no file at %s — starting clean", self._path)
            return
        try:
            raw  = json.loads(self._path.read_text())
            self.open_positions = _revive(raw.get("open_positions", {}))
            self.open_orders    = _revive(raw.get("open_orders", {}))
            log.info(
                "StateStore loaded: %d open positions, %d open orders",
                len(self.open_positions), len(self.open_orders),
            )
            if self.open_positions:
                for oid, pos in self.open_positions.items():
                    log.info(
                        "  Recovered position: %s | ticker=%s side=%s contracts=%s",
                        oid[:16], pos.get("ticker"), pos.get("side"), pos.get("contracts"),
                    )
        except Exception as exc:
            log.error("StateStore load failed: %s — starting clean", exc)

    def _flush(self) -> None:
        try:
            self._path.write_text(
                json.dumps(
                    {
                        "open_positions": self.open_positions,
                        "open_orders":    self.open_orders,
                    },
                    default=_decimal_default,
                    indent=2,
                )
            )
        except Exception as exc:
            log.error("StateStore flush failed: %s", exc)

    def upsert_position(self, kalshi_id: str, pos: dict) -> None:
        self.open_positions[kalshi_id] = pos
        self._flush()

    def remove_position(self, kalshi_id: str) -> None:
        self.open_positions.pop(kalshi_id, None)
        self._flush()

    def upsert_order(self, kalshi_id: str, order: dict) -> None:
        self.open_orders[kalshi_id] = order
        self._flush()

    def remove_order(self, kalshi_id: str) -> None:
        self.open_orders.pop(kalshi_id, None)
        self._flush()

    def clear(self) -> None:
        self.open_positions.clear()
        self.open_orders.clear()
        self._flush()
