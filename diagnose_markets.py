"""
diagnose_markets.py — Print raw Kalshi market API response so we can
see the exact field names the current API version is returning.

Usage:
    python diagnose_markets.py
"""
import asyncio, json, os, sys
from dotenv import load_dotenv
load_dotenv()

sys.path.insert(0, os.path.dirname(__file__))
from config import BotConfig
from session_manager import SessionManager

TARGET_SERIES = [
    "KXBTCD", "KXBTCW", "KXETHD", "KXETHW",
    "KXINX",  "KXSPX",  "KXNDX",
    "KXFED",  "KXCPI",  "KXPCE",  "KXGDP",
    "KXJOBS", "KXUNR",
    "KXOIL",  "KXGOLD",
]

async def main():
    cfg = BotConfig()
    cfg.rest_base = "https://api.elections.kalshi.com/trade-api/v2"

    async with SessionManager(cfg) as sess:
        print("\n── Fetching markets by series ──")
        all_markets = []

        for series in TARGET_SERIES:
            try:
                resp = await sess.get("/markets", params={"series_ticker": series, "limit": "200"})
                batch = resp.get("markets") or []
                print(f"  {series:10s}: {len(batch)} markets")
                all_markets.extend(batch)
            except Exception as e:
                print(f"  {series:10s}: ERROR — {e}")

        def vol24(m):
            for key in ("volume_24h_fp", "volume_24h", "volume24h"):
                v = m.get(key)
                if v is not None:
                    try: return float(v)
                    except: pass
            return 0.0

        print(f"\nTotal markets fetched: {len(all_markets)}")

        with_vol = [m for m in all_markets if vol24(m) > 0]
        print(f"Markets with 24h volume: {len(with_vol)}")

        # Print field names from first market to help debug volume key names
        if all_markets:
            sample = all_markets[0]
            vol_fields = {k: v for k, v in sample.items() if "vol" in k.lower()}
            print(f"Volume-related fields in API response: {vol_fields}")

        print("\n── Top markets by 24h volume ──")
        ranked = sorted(all_markets, key=vol24, reverse=True)
        for m in ranked[:20]:
            print(
                f"  {m.get('ticker','')[:50]:50s}  "
                f"vol_24h=${vol24(m):>10.2f}  "
                f"status={m.get('status')}"
            )

        if not ranked:
            print("  (no markets found for any target series)")

asyncio.run(main())
