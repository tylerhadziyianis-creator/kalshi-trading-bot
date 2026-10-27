"""
discover_series.py — Fast self-discovering Kalshi series scanner.

Queries all known series directly, then does a small catch-all pass
for anything outside the known list. Finishes in seconds.

Usage:
    python3 discover_series.py
"""
import asyncio
import os
import sys
from collections import defaultdict

from dotenv import load_dotenv
load_dotenv()

sys.path.insert(0, os.path.dirname(__file__))
from config import BotConfig
from session_manager import SessionManager

_SPORTS = (
    "KXNBA", "KXNFL", "KXMLB", "KXNHL", "KXWNBA",
    "KXEPL", "KXIPL", "KXPGA", "KXATP", "KXWTA",
    "KXNCAA", "KXUFC", "KXMMA", "KXNASCAR", "KXMVE",
    "KXNBAPTS", "KXNBAREB", "KXNBAAST", "KXNBA3PT",
    "KXNBASPREAD", "KXNBATOTAL", "KXNBAGAME",
    "KXMLBGAME", "KXMLBSPREAD", "KXMLBTOTAL",
    "KXNHLGAME", "KXEPLGAME",
)

CURRENT_SERIES = {
    "KXFED", "KXCPI", "KXGDP", "KXINX", "KXCPIYOY",
    "KXPRES", "KXELECT", "KXCONG", "KXPOL",
    "KXGEO", "KXSUPCT", "KXINTL",
    "KXFDA", "KXBIO", "KXSEC", "KXREG",
    "KXRT", "KXBOX", "KXMOV", "KXAWD", "KXOSCAR", "KXEMMY", "KXSPOT", "KXMUS",
    "KXEAR", "KXREV", "KXCEO", "KXSTK", "KXSPAC",
    "KXUNR", "KXJOBS", "KXHOU", "KXPCE", "KXPPI", "KXISM",
    "KXBRENTW", "KXBRENT", "KXWTI", "KXWTID",
    "KXNATGASW", "KXNATGAS", "KXOIL",
    "KXGOLDW", "KXGOLD", "KXSILVERW", "KXSILVER",
    "KXCOPPERW", "KXCOPPER",
    "KXAAAGASM", "KXAAAGASW",
    "KXPAYROLLS", "KXFEDDECISION", "KXBESSENTMTPMENTION",
    "KXBTCD", "KXETHD",
}

MIN_VOL = 50.0


def vol24(m: dict) -> float:
    for key in ("volume_24h_fp", "volume_24h", "volume24h"):
        v = m.get(key)
        if v is not None:
            try:
                return float(v)
            except (TypeError, ValueError):
                pass
    return 0.0


def is_sports(s: str) -> bool:
    return any(s.upper().startswith(p) for p in _SPORTS)


async def main():
    cfg = BotConfig()
    async with SessionManager(cfg) as sess:

        markets: list[dict] = []
        seen: set[str] = set()

        # ── Step 1: query every known series directly ─────────────────────────
        print("Step 1: querying all known series directly...")
        for series in sorted(CURRENT_SERIES):
            try:
                resp  = await sess.get("/markets", params={"series_ticker": series, "limit": "200"})
                batch = resp.get("markets") or []
                for m in batch:
                    t = m.get("ticker", "")
                    if t and t not in seen:
                        seen.add(t)
                        markets.append(m)
                if batch:
                    print(f"  {series:<28} {len(batch)} markets")
            except Exception as exc:
                print(f"  {series:<28} ERROR: {exc}")

        # ── Step 2: small catch-all pass for unknown series ───────────────────
        print(f"\nStep 2: catch-all pass (3 pages) for anything outside known series...")
        cursor = None
        for page in range(3):
            params: dict = {"limit": "200", "status": "open"}
            if cursor:
                params["cursor"] = cursor
            try:
                resp  = await sess.get("/markets", params=params)
                batch = resp.get("markets") or []
                if not batch:
                    break
                new = 0
                for m in batch:
                    t = m.get("ticker", "")
                    if t and t not in seen and not is_sports(t):
                        seen.add(t)
                        markets.append(m)
                        new += 1
                cursor = resp.get("cursor")
                print(f"  page {page+1}: {new} new non-sports markets added")
                if not cursor:
                    break
            except Exception as exc:
                print(f"  catch-all error: {exc}")
                break

        print(f"\nTotal: {len(markets)} markets scanned\n")

        # ── Group by series, rank by volume ───────────────────────────────────
        series_map: dict[str, list[dict]] = defaultdict(list)
        for m in markets:
            ticker = m.get("ticker", "")
            series = m.get("series_ticker") or ticker.split("-")[0]
            if is_sports(ticker) or is_sports(series):
                continue
            series_map[series].append(m)

        ranked = []
        for series, mkts in series_map.items():
            total = sum(vol24(m) for m in mkts)
            if total >= MIN_VOL:
                top = max(mkts, key=vol24)
                ranked.append((series, total, len(mkts), top.get("ticker", "")[:50]))
        ranked.sort(key=lambda x: x[1], reverse=True)

        print(f"{'Series':<25} {'vol_24h':>13}  {'Mkts':>5}  {'New?':<6}  Top ticker")
        print("─" * 110)
        for series, total, n, sample in ranked:
            flag = "[NEW]" if series not in CURRENT_SERIES else ""
            print(f"{series:<25} ${total:>12,.2f}  {n:>5}  {flag:<6}  {sample}")

        new = [r for r in ranked if r[0] not in CURRENT_SERIES]
        print(f"\n{'═'*110}")
        if new:
            print(f"  {len(new)} NEW series to add:")
            for series, total, n, _ in new:
                print(f'    "{series}",   # vol_24h=${total:,.0f}, {n} markets')
        else:
            print("  No new series — bot already targets everything with real volume.")
        print(f"{'═'*110}")


asyncio.run(main())
