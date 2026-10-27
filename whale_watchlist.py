"""
whale_watchlist.py — Persistent Whale Tracking & Watchlist Management

Maintains a JSON-backed registry of whale IDs with performance stats.
Provides weight multipliers that override WhaleRegistry base weights.

NOTE on Kalshi IDs: The public trade stream uses `trade_id` (unique per
trade) unless Kalshi includes `member_id` (some API versions do). Run with
--debug and watch logs: if you see the same whale_id repeat across events,
persistent tracking is working. If every ID is unique, the system still
operates but per-ID history won't accumulate — use manage_whales.py to
manually add IDs you obtain from Kalshi support or a premium data feed.

On Polymarket (on-chain), wallet addresses are persistent — this system
works out-of-the-box there without any caveats.

Whale lifecycle:
  watching → (≥ MIN_RESOLVED_PROMOTE resolved, win_rate ≥ MIN_WINRATE_PROMOTE) → proven
  watching → (≥ MIN_RESOLVED_DEMOTE  resolved, win_rate < MAX_WINRATE_DEMOTE)  → demoted
  any       → manual ban                                                         → banned

Weight overrides applied on top of WhaleRegistry base weights:
  proven   → weight × PROVEN_BOOST   (default 1.5)
  watching → weight × 1.0            (no change)
  demoted  → weight × DEMOTED_WEIGHT (default 0.1)
  banned   → weight = 0.0            (always skip)
"""
from __future__ import annotations

import json
import logging
import os
from dataclasses import dataclass, field, asdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

log = logging.getLogger(__name__)

# ── Status constants ──────────────────────────────────────────────────────────
STATUS_WATCHING = "watching"
STATUS_PROVEN   = "proven"
STATUS_DEMOTED  = "demoted"
STATUS_BANNED   = "banned"


@dataclass
class WhaleEntry:
    whale_id:        str
    status:          str    = STATUS_WATCHING
    added_at:        str    = field(default_factory=lambda: datetime.now(timezone.utc).isoformat())
    last_seen:       Optional[str] = None
    notes:           str    = ""
    total_resolved:  int    = 0
    wins:            int    = 0
    # category → {"resolved": int, "wins": int}
    categories:      dict   = field(default_factory=dict)

    def win_rate(self) -> Optional[float]:
        if self.total_resolved < 1:
            return None
        return self.wins / self.total_resolved

    def category_win_rate(self, cat: str) -> Optional[float]:
        stats = self.categories.get(cat, {})
        r = stats.get("resolved", 0)
        if r < 1:
            return None
        return stats.get("wins", 0) / r

    def summary(self) -> str:
        wr = self.win_rate()
        wr_str = f"{wr:.0%}" if wr is not None else "n/a"
        cats = " | ".join(
            f"{c}:{v['wins']}/{v['resolved']}"
            for c, v in self.categories.items()
        )
        return (
            f"[{self.status.upper()}] {self.whale_id[:12]}… "
            f"win={wr_str} ({self.wins}/{self.total_resolved}) "
            f"{cats or 'no_history'}"
        )


class WatchlistManager:
    """
    Thread-safe (via external asyncio.Lock in WhaleRegistry) watchlist.

    Usage:
        wl = WatchlistManager("watchlist.json")
        wl.load()

        # Get weight override for a whale (None = use default registry weight)
        override = wl.get_weight_override(whale_id)

        # Record a resolved trade result
        wl.record_result(whale_id, category="CRYPTO", correct=True)

        # Re-evaluate all statuses and save
        wl.auto_update_status()
        wl.save()
    """

    def __init__(
        self,
        path: str = "./watchlist.json",
        min_resolved_promote: int   = 10,
        min_winrate_promote:  float = 0.60,
        max_winrate_demote:   float = 0.45,
        min_resolved_demote:  int   = 5,
        proven_boost:         float = 1.5,
        demoted_weight:       float = 0.10,
    ) -> None:
        self._path              = Path(path)
        self.min_resolved_promote = min_resolved_promote
        self.min_winrate_promote  = min_winrate_promote
        self.max_winrate_demote   = max_winrate_demote
        self.min_resolved_demote  = min_resolved_demote
        self.proven_boost         = proven_boost
        self.demoted_weight       = demoted_weight
        self._whales: dict[str, WhaleEntry] = {}

    # ── persistence ───────────────────────────────────────────────────────────

    def load(self) -> None:
        if not self._path.exists():
            log.info("Watchlist: no file at %s — starting empty", self._path)
            return
        try:
            raw = json.loads(self._path.read_text())
            for wid, data in raw.get("whales", {}).items():
                self._whales[wid] = WhaleEntry(**{
                    k: v for k, v in data.items()
                    if k in WhaleEntry.__dataclass_fields__
                })
            log.info(
                "Watchlist loaded: %d whales (%d proven, %d demoted, %d banned)",
                len(self._whales),
                sum(1 for w in self._whales.values() if w.status == STATUS_PROVEN),
                sum(1 for w in self._whales.values() if w.status == STATUS_DEMOTED),
                sum(1 for w in self._whales.values() if w.status == STATUS_BANNED),
            )
        except Exception as exc:
            log.error("Watchlist load failed: %s — starting empty", exc)

    def save(self) -> None:
        try:
            data = {
                "updated_at": datetime.now(timezone.utc).isoformat(),
                "whales": {wid: asdict(e) for wid, e in self._whales.items()},
            }
            self._path.write_text(json.dumps(data, indent=2))
            log.debug("Watchlist saved: %d entries", len(self._whales))
        except Exception as exc:
            log.error("Watchlist save failed: %s", exc)

    # ── core API ──────────────────────────────────────────────────────────────

    def get_weight_override(self, whale_id: str) -> Optional[float]:
        """
        Returns a weight override multiplier, or None to use the registry default.
          proven   → proven_boost (e.g. 1.5)
          watching → None (use default weight 1.0)
          demoted  → demoted_weight (e.g. 0.10)
          banned   → 0.0
        New whales (not in watchlist) → None (they get added as 'watching')
        """
        entry = self._whales.get(whale_id)
        if entry is None:
            return None   # not yet tracked — let registry use default weight
        if entry.status == STATUS_BANNED:
            return 0.0
        if entry.status == STATUS_DEMOTED:
            return self.demoted_weight
        if entry.status == STATUS_PROVEN:
            return self.proven_boost
        return None   # watching → use registry default

    def ensure_tracked(self, whale_id: str, notes: str = "") -> WhaleEntry:
        """Register a new whale if not already present. Returns the entry."""
        if whale_id not in self._whales:
            self._whales[whale_id] = WhaleEntry(whale_id=whale_id, notes=notes)
            log.debug("Watchlist: new whale added — %s", whale_id[:16])
        return self._whales[whale_id]

    def record_result(
        self,
        whale_id:  str,
        category:  str,
        correct:   bool,
    ) -> None:
        """Record a resolved trade outcome and update performance stats."""
        entry = self.ensure_tracked(whale_id)
        entry.total_resolved += 1
        if correct:
            entry.wins += 1
        entry.last_seen = datetime.now(timezone.utc).isoformat()
        if category not in entry.categories:
            entry.categories[category] = {"resolved": 0, "wins": 0}
        entry.categories[category]["resolved"] += 1
        if correct:
            entry.categories[category]["wins"] += 1
        log.debug(
            "Watchlist result: %s | cat=%s correct=%s | %s",
            whale_id[:12], category, correct, entry.summary(),
        )

    def auto_update_status(self) -> int:
        """
        Re-evaluate all watching whales and promote/demote based on thresholds.
        Returns number of status changes made.
        """
        changes = 0
        for entry in self._whales.values():
            if entry.status == STATUS_BANNED:
                continue   # manual bans are permanent until explicitly lifted

            old_status = entry.status
            wr = entry.win_rate()

            if wr is None:
                continue   # not enough data

            if (
                entry.status in (STATUS_WATCHING, STATUS_DEMOTED)
                and entry.total_resolved >= self.min_resolved_promote
                and wr >= self.min_winrate_promote
            ):
                entry.status = STATUS_PROVEN
                log.info(
                    "Watchlist PROMOTED: %s | win_rate=%.0f%% (%d resolved)",
                    entry.whale_id[:16], wr * 100, entry.total_resolved,
                )
                changes += 1

            elif (
                entry.status in (STATUS_WATCHING, STATUS_PROVEN)
                and entry.total_resolved >= self.min_resolved_demote
                and wr < self.max_winrate_demote
            ):
                entry.status = STATUS_DEMOTED
                log.info(
                    "Watchlist DEMOTED: %s | win_rate=%.0f%% (%d resolved)",
                    entry.whale_id[:16], wr * 100, entry.total_resolved,
                )
                changes += 1

        return changes

    # ── manual management ─────────────────────────────────────────────────────

    def add(self, whale_id: str, status: str = STATUS_WATCHING, notes: str = "") -> WhaleEntry:
        """Manually add or update a whale entry."""
        if whale_id in self._whales:
            self._whales[whale_id].status = status
            if notes:
                self._whales[whale_id].notes = notes
        else:
            self._whales[whale_id] = WhaleEntry(
                whale_id=whale_id, status=status, notes=notes,
            )
        self.save()
        log.info("Watchlist: manually added %s as [%s]", whale_id[:16], status)
        return self._whales[whale_id]

    def ban(self, whale_id: str, notes: str = "") -> None:
        """Permanently ban a whale from being followed."""
        entry = self.ensure_tracked(whale_id, notes)
        entry.status = STATUS_BANNED
        if notes:
            entry.notes = notes
        self.save()
        log.info("Watchlist: BANNED %s — %s", whale_id[:16], notes)

    def unban(self, whale_id: str) -> None:
        """Lift a ban and reset to watching."""
        if whale_id in self._whales:
            self._whales[whale_id].status = STATUS_WATCHING
            self.save()
            log.info("Watchlist: unbanned %s", whale_id[:16])

    def remove(self, whale_id: str) -> bool:
        """Remove a whale entirely from the watchlist."""
        if whale_id in self._whales:
            del self._whales[whale_id]
            self.save()
            return True
        return False

    # ── diagnostics ───────────────────────────────────────────────────────────

    def all_entries(self) -> list[WhaleEntry]:
        return sorted(
            self._whales.values(),
            key=lambda e: (e.status, -(e.win_rate() or 0)),
        )

    def proven_whales(self) -> list[WhaleEntry]:
        return [e for e in self._whales.values() if e.status == STATUS_PROVEN]

    def count(self) -> dict[str, int]:
        counts: dict[str, int] = {
            STATUS_WATCHING: 0, STATUS_PROVEN: 0,
            STATUS_DEMOTED: 0,  STATUS_BANNED:  0,
        }
        for e in self._whales.values():
            counts[e.status] = counts.get(e.status, 0) + 1
        return counts
