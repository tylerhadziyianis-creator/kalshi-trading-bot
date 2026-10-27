#!/usr/bin/env python3
"""
manage_whales.py — CLI tool for managing the whale watchlist.

Usage:
    python manage_whales.py list
    python manage_whales.py list --status proven
    python manage_whales.py add <whale_id> [--status watching|proven|demoted|banned] [--notes "..."]
    python manage_whales.py ban <whale_id> [--notes "reason"]
    python manage_whales.py unban <whale_id>
    python manage_whales.py remove <whale_id>
    python manage_whales.py stats

Environment:
    WATCHLIST_FILE=./watchlist.json  (default)

Notes on whale IDs on Kalshi:
    Kalshi's public trade stream uses 'trade_id' (unique per trade), not
    persistent user IDs. Check your bot logs for whale_id values — if you
    see the same ID appear across multiple events, you have a persistent ID
    and can track it here. If every ID is unique, the watchlist mainly
    serves as a manual quality override for confirmed smart-money accounts
    you obtain through other means (Kalshi API direct query, research, etc.).

    On Polymarket, wallet addresses in logs are always persistent and ready
    to add here directly.
"""
from __future__ import annotations

import argparse
import os
import sys

from whale_watchlist import WatchlistManager, STATUS_WATCHING, STATUS_PROVEN, STATUS_DEMOTED, STATUS_BANNED


def cmd_list(wl: WatchlistManager, args: argparse.Namespace) -> None:
    entries = wl.all_entries()
    if args.status:
        entries = [e for e in entries if e.status == args.status]

    if not entries:
        print("No whales in watchlist.")
        return

    counts = wl.count()
    print(f"\n{'─' * 70}")
    print(f"  Whale Watchlist  |  watching={counts['watching']}  proven={counts['proven']}  "
          f"demoted={counts['demoted']}  banned={counts['banned']}")
    print(f"{'─' * 70}")
    for e in entries:
        wr = e.win_rate()
        wr_str = f"{wr:.1%}" if wr is not None else "  n/a "
        cats = " | ".join(
            f"{c}:{v['wins']}/{v['resolved']}"
            for c, v in e.categories.items()
        )
        status_pad = e.status.upper().ljust(8)
        notes = f"  // {e.notes}" if e.notes else ""
        print(
            f"  [{status_pad}]  {e.whale_id[:32].ljust(32)}  "
            f"win={wr_str} ({e.wins:3d}/{e.total_resolved:3d})  "
            f"{cats or 'no history'}{notes}"
        )
    print(f"{'─' * 70}\n")


def cmd_add(wl: WatchlistManager, args: argparse.Namespace) -> None:
    status = args.status or STATUS_WATCHING
    if status not in (STATUS_WATCHING, STATUS_PROVEN, STATUS_DEMOTED, STATUS_BANNED):
        print(f"Invalid status: {status}")
        sys.exit(1)
    entry = wl.add(args.whale_id, status=status, notes=args.notes or "")
    print(f"Added: {entry.summary()}")


def cmd_ban(wl: WatchlistManager, args: argparse.Namespace) -> None:
    wl.ban(args.whale_id, notes=args.notes or "")
    print(f"Banned: {args.whale_id}")


def cmd_unban(wl: WatchlistManager, args: argparse.Namespace) -> None:
    wl.unban(args.whale_id)
    print(f"Unbanned: {args.whale_id} → watching")


def cmd_remove(wl: WatchlistManager, args: argparse.Namespace) -> None:
    ok = wl.remove(args.whale_id)
    print("Removed." if ok else f"Not found: {args.whale_id}")


def cmd_stats(wl: WatchlistManager, args: argparse.Namespace) -> None:
    entries = wl.all_entries()
    if not entries:
        print("No whales tracked yet.")
        return

    proven = [e for e in entries if e.status == STATUS_PROVEN]
    print(f"\nTotal tracked: {len(entries)}")
    counts = wl.count()
    for s, n in counts.items():
        print(f"  {s.ljust(10)}: {n}")

    if proven:
        print(f"\nTop proven whales:")
        for e in sorted(proven, key=lambda x: -(x.win_rate() or 0))[:10]:
            print(f"  {e.summary()}")

    # Category breakdown across all proven whales
    cat_totals: dict[str, dict] = {}
    for e in proven:
        for cat, stats in e.categories.items():
            if cat not in cat_totals:
                cat_totals[cat] = {"resolved": 0, "wins": 0}
            cat_totals[cat]["resolved"] += stats["resolved"]
            cat_totals[cat]["wins"]     += stats["wins"]

    if cat_totals:
        print(f"\nProven whale category breakdown:")
        for cat, stats in sorted(cat_totals.items()):
            wr = stats["wins"] / stats["resolved"] if stats["resolved"] else 0
            print(f"  {cat.ljust(12)}: {stats['wins']}/{stats['resolved']} ({wr:.0%})")
    print()


def main() -> None:
    parser = argparse.ArgumentParser(description="Manage the bot's whale watchlist")
    parser.add_argument("--file", default=os.getenv("WATCHLIST_FILE", "./watchlist.json"),
                        help="Path to watchlist JSON file")
    sub = parser.add_subparsers(dest="cmd")

    p_list = sub.add_parser("list", help="List all whales")
    p_list.add_argument("--status", choices=[STATUS_WATCHING, STATUS_PROVEN, STATUS_DEMOTED, STATUS_BANNED],
                        help="Filter by status")

    p_add = sub.add_parser("add", help="Add or update a whale")
    p_add.add_argument("whale_id")
    p_add.add_argument("--status", default=STATUS_WATCHING)
    p_add.add_argument("--notes", default="")

    p_ban = sub.add_parser("ban", help="Ban a whale (weight=0)")
    p_ban.add_argument("whale_id")
    p_ban.add_argument("--notes", default="")

    p_unban = sub.add_parser("unban", help="Lift ban, reset to watching")
    p_unban.add_argument("whale_id")

    p_rm = sub.add_parser("remove", help="Remove a whale from tracking")
    p_rm.add_argument("whale_id")

    sub.add_parser("stats", help="Show performance statistics")

    args = parser.parse_args()
    if not args.cmd:
        parser.print_help()
        sys.exit(0)

    wl = WatchlistManager(path=args.file)
    wl.load()

    dispatch = {
        "list":   cmd_list,
        "add":    cmd_add,
        "ban":    cmd_ban,
        "unban":  cmd_unban,
        "remove": cmd_remove,
        "stats":  cmd_stats,
    }
    dispatch[args.cmd](wl, args)


if __name__ == "__main__":
    main()
