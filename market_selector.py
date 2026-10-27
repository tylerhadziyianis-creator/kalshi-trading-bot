"""
market_selector.py — Dynamic Market Discovery & Auto-Subscription

Queries the Kalshi REST API for open markets, ranks them by activity score,
and returns the top N tickers for the bot to subscribe to via WebSocket.

Controlled entirely via .env — no code changes needed to adjust behaviour:

    AUTO_SELECT_MARKETS=true          # enable dynamic selection
    AUTO_MARKET_COUNT=20              # how many tickers to select
    AUTO_MARKET_REFRESH_SECONDS=900   # re-rank every 15 min
    AUTO_MARKET_MIN_VOLUME=100        # minimum 24h volume ($) to consider

Category preference weights prioritise high-liquidity crypto/macro markets
over sports/entertainment. Adjust CATEGORY_WEIGHTS to tune this.

Fallback: if discovery fails for any reason, reverts to KALSHI_TICKERS from .env.
"""
from __future__ import annotations

import asyncio
import logging
import os
from typing import Callable, Coroutine, Optional

from session_manager import SessionManager
from config import VERBOSE_LOGS

log = logging.getLogger(__name__)

# ── Category preference weights ───────────────────────────────────────────────
# Higher weight = preferred. The weight multiplies the raw activity score.
CATEGORY_WEIGHTS: list[tuple[str, float]] = [
    # Macro — confirmed dominant on Kalshi
    ("KXFED",      10.0),  # Fed rate decisions — by far most active
    ("KXFEDDECISION", 9.5),  # Fed meeting rate decision (cut/hold/hike)
    ("KXCPI",       9.0),  # CPI month-over-month
    ("KXCPIYOY",    9.0),  # CPI year-over-year
    ("KXINX",       8.5),  # S&P 500 index
    ("KXGDP",       8.0),  # GDP releases
    ("KXPAYROLLS",  8.5),  # Jobs / payrolls
    # Crypto — highly liquid binary above/below markets, strong directional flow
    ("KXBTCD",      9.0),  # Bitcoin daily — clean binary, heavy retail + informed flow
    ("KXETHD",      8.5),  # Ethereum daily
    ("KXBTCW",      7.5),  # Bitcoin weekly
    ("KXETHW",      7.0),  # Ethereum weekly
    ("KXXRPD",      7.5),  # XRP daily — institutional + SEC case watchers
    ("KXXRPW",      7.0),  # XRP weekly
    ("KXSOLD",      7.0),  # Solana daily
    ("KXSOLW",      6.5),  # Solana weekly
    ("KXDOGE",      6.0),  # Dogecoin — Musk-driven, volatile informed spikes
    ("KXGPT",       7.0),  # AI/ChatGPT — insiders have genuine edge on release dates
    ("KXAI",        6.5),  # General AI markets
    # IPO markets — investment bankers, company insiders, early investors
    ("KXIPO",       8.0),  # General IPO market — strong insider flow
    ("KXIPOSPACEX", 8.5),  # SpaceX IPO — executives, bankers know the timeline
    ("KXIPOANDURIL",7.5),  # Anduril IPO — defense tech insiders
    # Long-duration political / legal
    ("KXIMPEACH",   8.0),  # Impeachment proceedings — congressional insiders
    ("KXSHUTDOWN",  7.5),  # Government shutdown — appropriations staff
    ("KXDEBT",      7.5),  # Debt ceiling — Treasury / congressional leadership
    # Politics — strong insider flow signal
    ("KXPRES",   8.5),  # Presidential actions/approval
    ("KXELECT",  8.0),  # Elections
    ("KXCONG",   7.5),  # Congressional (legislation, votes)
    ("KXPOL",    7.5),  # General political events
    ("KXGEO",    7.0),  # Geopolitical events
    ("KXSUPCT",  7.0),  # Supreme Court decisions
    ("KXINTL",   6.5),  # International politics
    # FDA / Biotech — notorious insider trading around trial results
    ("KXFDA",    8.5),  # FDA approvals
    ("KXBIO",    8.0),  # Biotech trials
    # Crypto / Financial regulation — SEC/CFTC decisions
    ("KXSEC",    7.5),  # SEC regulatory decisions
    ("KXREG",    7.0),  # General regulatory events
    # Entertainment / media — strong insider flow (screeners, industry insiders)
    ("KXRT",     7.5),  # Rotten Tomatoes scores — screeners know early
    ("KXBOX",    7.0),  # Box office results
    ("KXMOV",    7.0),  # Movie markets
    ("KXAWD",    7.5),  # Awards shows — heavy industry insider flow
    ("KXOSCAR",  7.5),  # Oscars
    ("KXEMMY",   7.0),  # Emmys
    ("KXSPOT",   6.5),  # Spotify / streaming charts
    ("KXMUS",    6.5),  # Music markets
    # Companies / earnings
    ("KXEAR",    8.0),  # Earnings — analysts and insiders trade heavily
    ("KXREV",    7.5),  # Revenue
    ("KXCEO",    7.0),  # Executive changes
    ("KXSTK",    7.0),  # Stock events
    ("KXSPAC",   6.5),  # SPACs
    # Additional economic indicators
    ("KXUNR",    7.5),  # Unemployment
    ("KXJOBS",   7.5),  # Jobs report
    ("KXHOU",    6.5),  # Housing
    ("KXPCE",    7.0),  # PCE inflation
    ("KXPPI",    7.0),  # PPI
    ("KXISM",    6.5),  # ISM manufacturing
    # Commodities (confirmed Kalshi series names)
    ("KXBRENTW",  7.5),  # Brent crude weekly
    ("KXBRENT",   7.5),  # Brent crude
    ("KXWTI",     7.5),  # WTI crude
    ("KXWTID",    7.5),  # WTI crude daily
    ("KXNATGASW", 7.0),  # Natural gas weekly
    ("KXNATGAS",  7.0),  # Natural gas
    ("KXOIL",     7.0),  # Crude oil generic
    ("KXGOLDW",   7.0),  # Gold weekly
    ("KXGOLD",    7.0),  # Gold
    ("KXSILVERW", 6.5),  # Silver weekly
    ("KXSILVER",  6.5),  # Silver
    ("KXCOPPERW", 6.5),  # Copper weekly (confirmed)
    ("KXCOPPER",  6.0),  # Copper generic
    ("KXAAAGASM", 6.5),  # US gas price monthly (AAA)
    ("KXAAAGASW", 6.5),  # US gas price weekly direction (AAA)
    # Sports — excluded by default, low weight if re-enabled
    ("KXNBA",    4.0),  # NBA
    ("KXNFL",    4.0),  # NFL
    ("KXWNBA",   3.5),  # WNBA
    ("KXMLB",    3.0),  # MLB
    ("KXNHL",    3.0),  # NHL
    ("KXEPL",    3.0),  # Premier League
    ("KXIPL",    2.5),  # IPL cricket
    ("KXPGA",    2.0),  # PGA golf
    ("KXATP",    2.0),  # ATP tennis
]
_DEFAULT_WEIGHT = 1.0


def _category_weight(ticker: str) -> float:
    for prefix, weight in CATEGORY_WEIGHTS:
        if ticker.startswith(prefix):
            return weight
    return _DEFAULT_WEIGHT


# Sports prefixes to hard-exclude when EXCLUDE_SPORTS=true (default: true)
# These markets have no information signal — just retail gambling noise
_SPORTS_PREFIXES = (
    "KXNBA", "KXNFL", "KXMLB", "KXNHL", "KXWNBA",
    "KXEPL", "KXIPL", "KXPGA", "KXATP", "KXWTA",
    "KXNCAA", "KXUFC", "KXMMA", "KXNASCAR",
    "KXNBAPTS", "KXNBAREB", "KXNBAAST", "KXNBA3PT",
    "KXNBASPREAD", "KXNBATOTAL", "KXNBAGAME",
    "KXMLBGAME", "KXMLBSPREAD", "KXMLBTOTAL",
    "KXNHLGAME", "KXEPLGAME", "KXMVE",  # multivariate (usually sports parlays)
)

_EXCLUDE_SPORTS: bool  = os.getenv("EXCLUDE_SPORTS", "true").lower() == "true"
_EXCLUDE_OIL:    bool  = os.getenv("EXCLUDE_OIL", "true").lower() == "true"
_MIN_VOL_DEFAULT: float = float(os.getenv("AUTO_MARKET_MIN_VOLUME", "10"))

_OIL_PREFIXES = ("KXWTI", "KXBRENTW", "KXBRENT", "KXWTID", "KXOIL")

# Celebrity / pure-entertainment series with no informational signal.
# These may have real volume (fan betting) but no insider flow or fundamental model.
# Distinct from KXRT/KXAWD/KXOSCAR which have genuine insider trading.
_NOISE_PREFIXES = (
    "KXBBCHART",    # Billboard chart positions — fan betting, no insider angle
    "KXSOCIAL",     # Social media metrics — pure retail
    "KXFOLLOWER",   # Follower counts
    "KXSTREAMS",    # Streaming counts (distinct from KXSPOT which has Spotify chart insider flow)
    "KXCELEB",      # Celebrity markets
    "KXREALITY",    # Reality TV voting — fans, no advance info
    "KXTIKTOK",     # TikTok metrics
    "KXYOUTUBE",    # YouTube metrics
)


def _is_sports(ticker: str) -> bool:
    return any(ticker.startswith(p) for p in _SPORTS_PREFIXES)


def _is_noise(ticker: str) -> bool:
    """Returns True for celebrity/fan markets with no informational signal."""
    return any(ticker.startswith(p) for p in _NOISE_PREFIXES)


def _vol24_score(m: dict) -> float:
    for key in ("volume_24h_fp", "volume_24h", "volume24h"):
        v = m.get(key)
        if v is not None:
            try:
                return float(v)
            except (TypeError, ValueError):
                pass
    return 0.0


def _score_market(m: dict) -> float:
    """
    Composite activity score for market ranking.

    Factors (in rough priority order):
      1. 24h volume     — strongest real-time activity signal
      2. Liquidity $    — depth of book
      3. All-time volume — established market
      4. Open interest  — how many contracts are outstanding
      5. Spread tightness — proxy for market quality
      6. Category weight — strongly prefer macro/crypto over sports

    Sports markets are excluded entirely by default (EXCLUDE_SPORTS=true).
    Macro/crypto markets carry real informational signal from informed traders.
    """
    vol24   = _vol24_score(m)
    vol_all = float(m.get("volume_fp")         or m.get("volume")         or 0)
    liq     = float(m.get("liquidity_dollars") or m.get("liquidity")      or 0)
    oi      = float(m.get("open_interest_fp")  or m.get("open_interest")  or 0)

    yes_bid = float(m.get("yes_bid_dollars") or 0)
    yes_ask = float(m.get("yes_ask_dollars") or 0)
    spread  = (yes_ask - yes_bid) if (yes_ask > 0 and yes_bid > 0) else 1.0
    spread_score = max(0.0, 1.0 - spread * 10) * 500

    cat = _category_weight(m.get("ticker", ""))

    return (vol24 * 3.0 + vol_all * 0.5 + liq * 2.0 + oi * 1.0 + spread_score) * cat


class MarketSelector:
    """
    Fetches, ranks, and periodically refreshes the list of market tickers
    the bot should subscribe to.

    Usage (from bot_engine):
        selector = MarketSelector(cfg, sess)
        tickers  = await selector.get_tickers()   # initial fetch
        selector.set_refresh_callback(feed.resubscribe)
        asyncio.create_task(selector.refresh_loop())
    """

    # Dynamic threshold parameters
    #
    # Sliding scale: higher % for thin markets, lower % for liquid ones.
    # Log-linear interpolation between two anchors:
    #   ≤ $5k/day   → 12% (or $400 floor)
    #   $5k–$100k   → tapers 12% → 5%
    #   ≥ $100k/day → flat 5%
    #   hard cap    → $20,000
    #
    # Example thresholds:
    #   $5k/day   → $600  (12%)   $10k → $1,038 (10.4%)
    #   $20k/day  → $1,752 (8.8%) $50k → $3,310  (6.6%)
    #   $100k/day → $5,000 (5%)
    _NOTIONAL_FLOOR:     float = 400.0    # flat floor for ultra-thin markets
    _NOTIONAL_CAP:       float = 20_000.0 # hard cap
    _SCALE_LO_VOL:       float = 5_000.0  # anchor: low-volume end
    _SCALE_LO_PCT:       float = 0.12     # 12% at $5k/day
    _SCALE_HI_VOL:       float = 100_000.0# anchor: high-volume end
    _SCALE_HI_PCT:       float = 0.05     # 5% at $100k/day

    # Minimum series-level volume to auto-promote a newly discovered series
    _NEW_SERIES_MIN_VOL: float = 5000.0

    def __init__(self, cfg, sess: SessionManager) -> None:
        self._cfg  = cfg
        self._sess = sess

        self._enabled      = cfg.auto_select_markets
        self._count        = cfg.auto_market_count
        self._refresh_sec  = cfg.auto_market_refresh_seconds
        self._min_volume   = cfg.auto_market_min_volume

        self._static_tickers: list[str]  = self._load_static()
        self._current_tickers: list[str] = []
        self._market_volumes: dict[str, float] = {}    # ticker → 24h volume ($)
        self._market_close_times: dict[str, str] = {}  # ticker → close_time ISO string
        self._lock = asyncio.Lock()

        # Runtime series discovery — series found via catch-all that aren't in
        # _TARGET_SERIES are added here and queried directly on subsequent refreshes.
        # Survives within a session; logged prominently so operator can hard-code them.
        self._discovered_series: set[str] = set()
        self._known_series: set[str]      = set(self._TARGET_SERIES)

        # Registered by bot_engine so we can push new tickers to the WS feed
        self._refresh_cb: Optional[Callable[..., Coroutine]] = None

    # ── public API ────────────────────────────────────────────────────────────

    def is_enabled(self) -> bool:
        return self._enabled

    def set_refresh_callback(self, cb: Callable[..., Coroutine]) -> None:
        """Register an async callback(tickers: list[str]) called after each refresh."""
        self._refresh_cb = cb

    def get_threshold(self, ticker: str) -> "Decimal":
        """
        Dynamic whale notional threshold for a specific market.

        Base formula: sliding scale 12%→5% (log-linear), floor=$400, cap=$20k

        Duration multiplier (applied to the floor only, not to high-volume markets):
          Long-duration markets require a proportionally larger trade to qualify
          as a whale signal — a $1,500 casual bet on an event 6 months away is
          noise; a $6,000 bet on the same event is meaningful conviction.

          ≤7 days   → 1.0× floor  ($1,500)
          8–30 days → 1.5× floor  ($2,250)
          31–90 days→ 2.0× floor  ($3,000)
          91–365d   → 3.0× floor  ($4,500)
          >365 days → 4.0× floor  ($6,000)

          For liquid markets (dynamic threshold > floor), the duration multiplier
          is applied via max() so it never reduces an already-high threshold.
        """
        import time as _time
        from decimal import Decimal as _D

        vol24 = self._market_volumes.get(ticker)
        if vol24 is None:
            return self._cfg.whale_min_notional

        # Sliding scale: log-linear taper from 12% (thin) to 5% (liquid)
        if vol24 <= self._SCALE_LO_VOL:
            pct = self._SCALE_LO_PCT
        elif vol24 >= self._SCALE_HI_VOL:
            pct = self._SCALE_HI_PCT
        else:
            import math as _math
            t   = _math.log(vol24 / self._SCALE_LO_VOL) / _math.log(self._SCALE_HI_VOL / self._SCALE_LO_VOL)
            pct = self._SCALE_LO_PCT + (self._SCALE_HI_PCT - self._SCALE_LO_PCT) * t
        dynamic = vol24 * pct
        clamped = max(self._NOTIONAL_FLOOR, min(self._NOTIONAL_CAP, dynamic))

        # Commodity multiplier — WTI, Brent, Nat Gas attract heavy hedger flow
        # (airlines, refiners, energy companies buying contracts as operational
        # insurance regardless of direction). Require 3× larger trades before
        # treating as informed flow. Reduces false positives significantly.
        _COMMODITY_PREFIXES = ("KXWTI", "KXBRENT", "KXBRENTW", "KXNATGAS", "KXNATGASW", "KXOIL")
        if any(ticker.startswith(p) for p in _COMMODITY_PREFIXES):
            clamped = max(clamped * 3.0, 3000.0)   # minimum $3k for commodity whales

        # Duration multiplier — raise the floor for long-duration markets
        close_iso = self._market_close_times.get(ticker)
        if close_iso:
            try:
                from datetime import datetime, timezone
                close_dt = datetime.fromisoformat(close_iso.replace("Z", "+00:00"))
                days_out  = (close_dt - datetime.now(timezone.utc)).days
                if days_out > 365:
                    duration_floor = self._NOTIONAL_FLOOR * 4.0
                elif days_out > 90:
                    duration_floor = self._NOTIONAL_FLOOR * 3.0
                elif days_out > 30:
                    duration_floor = self._NOTIONAL_FLOOR * 2.0
                elif days_out > 7:
                    duration_floor = self._NOTIONAL_FLOOR * 1.5
                else:
                    duration_floor = self._NOTIONAL_FLOOR
                clamped = max(clamped, duration_floor)
            except Exception:
                pass  # malformed date → use base threshold

        return _D(str(round(clamped, 2)))

    def get_vol24(self, ticker: str) -> Optional[float]:
        """Return the cached 24h volume for a ticker, or None if not yet known."""
        return self._market_volumes.get(ticker)

    def get_days_to_close(self, ticker: str) -> Optional[int]:
        """Return days until this market closes, or None if unknown."""
        close_iso = self._market_close_times.get(ticker)
        if not close_iso:
            return None
        try:
            from datetime import datetime, timezone
            close_dt = datetime.fromisoformat(close_iso.replace("Z", "+00:00"))
            return max(0, (close_dt - datetime.now(timezone.utc)).days)
        except Exception:
            return None

    async def get_tickers(self) -> list[str]:
        """
        Return the current ticker list.
        On first call (if enabled), fetches from the API.
        Falls back to static KALSHI_TICKERS on any failure.
        """
        if not self._enabled:
            log.info("MarketSelector: auto-selection disabled — using static tickers (%d)", len(self._static_tickers))
            return list(self._static_tickers)

        async with self._lock:
            if not self._current_tickers:
                # Run full series discovery on first fetch to ensure complete coverage.
                # This catches any series Kalshi has that isn't in _TARGET_SERIES.
                await self._discover_all_series()
                await self._fetch_and_rank()
            return list(self._current_tickers)

    async def refresh_loop(self) -> None:
        """
        Background task: re-rank markets every AUTO_MARKET_REFRESH_SECONDS.
        After each refresh, calls the registered callback so the WS feed
        can subscribe to newly selected tickers.
        """
        if not self._enabled:
            return

        log.info("MarketSelector: refresh loop started (interval=%ds)", self._refresh_sec)
        while True:
            await asyncio.sleep(self._refresh_sec)
            log.info("MarketSelector: scheduled refresh triggered")
            async with self._lock:
                old = list(self._current_tickers)
                await self._fetch_and_rank()
                new = list(self._current_tickers)

            added   = [t for t in new if t not in old]
            removed = [t for t in old if t not in new]
            if added or removed:
                log.info("MarketSelector: refresh — +%d added, -%d dropped (%d total)",
                         len(added), len(removed), len(new))
                if VERBOSE_LOGS:
                    if added:   log.info("  added:   %s", added)
                    if removed: log.info("  dropped: %s", removed)

            if self._refresh_cb and (added or removed):
                try:
                    await self._refresh_cb(new)
                except Exception as exc:
                    log.error("MarketSelector: refresh callback failed: %s", exc)

    # ── internals ─────────────────────────────────────────────────────────────

    def _load_static(self) -> list[str]:
        raw = os.getenv("KALSHI_TICKERS", "")
        tickers = [t.strip() for t in raw.split(",") if t.strip()]
        return tickers

    # Series we want to trade — queried directly to avoid sports parlay flood.
    # Confirmed active (from diagnostic): KXFED, KXCPI, KXINX, KXGDP
    # Confirmed dead/zero volume: KXBTCD, KXBTCW, KXETHD, KXETHW, KXOIL, KXGOLD,
    #                              KXJOBS, KXUNR, KXSPX, KXNDX, KXPCE
    # Political/FDA series included speculatively — return 0 markets if not on Kalshi.
    _TARGET_SERIES = [
        # ── Confirmed active ──────────────────────────────────────────────────
        "KXFED",  "KXCPI",  "KXGDP",                    # macro (dominant)
        "KXINX",                                         # equity index
        # ── Politics / informed flow ──────────────────────────────────────────
        "KXPRES", "KXELECT", "KXCONG", "KXPOL",         # politics / elections
        "KXGEO",  "KXSUPCT", "KXINTL",                  # geopolitical / SCOTUS
        # ── FDA / regulation ─────────────────────────────────────────────────
        "KXFDA",  "KXBIO",                               # FDA approvals / biotech
        "KXSEC",  "KXREG",                               # regulatory decisions
        # ── Entertainment / media — strong insider flow ───────────────────────
        "KXRT",   "KXBOX",  "KXMOV",                    # Rotten Tomatoes, box office, movies
        "KXAWD",  "KXOSCAR", "KXEMMY",                  # awards shows
        "KXMUS",                                         # music charts
        # ── Companies / earnings ─────────────────────────────────────────────
        "KXEAR",  "KXREV",  "KXCEO",                    # earnings, revenue, executive changes
        "KXSTK",  "KXSPAC",                              # stock events, SPACs
        # ── Additional financials ─────────────────────────────────────────────
        "KXUNR",  "KXJOBS", "KXHOU",                    # unemployment, jobs, housing
        "KXPCE",  "KXPPI",  "KXISM",                    # PCE, PPI, ISM
        # ── Commodities (confirmed series names from Kalshi) ─────────────────
        "KXBRENTW",  "KXBRENT",  "KXWTI",  "KXWTID",    # crude oil
        "KXNATGASW", "KXNATGAS", "KXOIL",                # natural gas
        "KXGOLDW",   "KXGOLD",                           # gold
        "KXSILVERW", "KXSILVER", "KXCOPPER",             # silver / metals
        "KXAAAGASM", "KXAAAGASW",                        # US gas prices (AAA)
        # ── Economic releases (confirmed) ────────────────────────────────────
        "KXPAYROLLS",                                    # jobs / payrolls report
        "KXFEDDECISION",                                 # Fed meeting rate decision
        # ── Political / statement markets ────────────────────────────────────
        "KXBESSENTMTPMENTION",                           # Treasury Sec. Bessent statements
        # ── Additional confirmed series ───────────────────────────────────────
        "KXCPIYOY",                                      # CPI year-over-year (separate from KXCPI)
        "KXCOPPERW",                                     # Copper weekly
        "KXSPOT",                                        # Spotify/streaming charts
        # ── Crypto — daily/weekly above-below binary markets ─────────────────
        "KXBTCD", "KXETHD",                              # Bitcoin/Ethereum daily
        "KXBTCW", "KXETHW",                              # Bitcoin/Ethereum weekly
        "KXXRPD", "KXXRPW",                              # XRP daily/weekly — institutional flow, SEC case watchers
        "KXSOLD", "KXSOLW",                              # Solana daily/weekly — growing institutional presence
        "KXDOGE",                                        # Dogecoin — Musk-driven spikes, insider circle
        # ── AI / Tech — growing informed flow ────────────────────────────────
        "KXGPT",                                         # ChatGPT/AI capability markets — OpenAI/Anthropic insiders
        "KXAI",                                          # General AI markets
        # ── IPO markets — investment bankers and company insiders ─────────────
        "KXIPO",                                         # General IPO market — who IPOs in 2026
        "KXIPOSPACEX",                                   # SpaceX IPO timing — executives, bankers, early investors
        "KXIPOANDURIL",                                  # Anduril IPO — defense tech insiders
        # ── Tech product releases — supply chain / insider knowledge ──────────
        "KXIPHONERELEASE",                               # iPhone release — TSMC/Foxconn workers know months ahead
        "KXPS6",                                         # PlayStation 6 — Sony insiders
        "KXMETA",                                        # Meta earnings / product milestones
        # ── Long-duration political / legal events ────────────────────────────
        "KXIMPEACH",                                     # Impeachment — congressional staff, party leadership
        "KXSHUTDOWN",                                    # Government shutdown — appropriations insiders
        "KXDEBT",                                        # Debt ceiling — Treasury, congressional leadership
        # ── Weather — energy companies, agriculture, commodity desks ──────────
        "KXTEMP",                                        # Temperature markets — energy/ag insiders
        "KXWEAT",                                        # Weather events
        "KXRAIN",                                        # Precipitation
        "KXSNOW",                                        # Snowfall
        "KXHEAT",                                        # Heat index / heat wave markets
        "KXHURR",                                        # Hurricane markets — insurance/reinsurance insiders
        "KXTROP",                                        # Tropical storm markets
    ]

    async def _fetch_series(self, series_ticker: str) -> list[dict]:
        """
        Fetch all active markets for a single series ticker.
        Retries once on empty response to guard against transient API issues
        (rate limiting during concurrent batch fetches returns empty instead
        of an error, causing important series like KXFEDDECISION to vanish).
        """
        for attempt in range(2):   # 1 retry on empty
            markets: list[dict] = []
            cursor: Optional[str] = None
            for _ in range(3):   # max 3 pages per series
                params: dict = {"series_ticker": series_ticker, "limit": "200"}
                if cursor:
                    params["cursor"] = cursor
                try:
                    resp  = await self._sess.get("/markets", params=params)
                    batch = resp.get("markets") or []
                    markets.extend(batch)
                    cursor = resp.get("cursor")
                    if not cursor or not batch:
                        break
                except Exception as exc:
                    log.debug("Series fetch failed for %s: %s", series_ticker, exc)
                    break
            if markets:
                return markets
            if attempt == 0:
                # Empty response — wait briefly and retry once
                await asyncio.sleep(0.5)
                log.debug("Series %s returned empty — retrying", series_ticker)
        return []

    async def _fetch_catchall(self) -> list[dict]:
        """
        Supplementary: scan 5 pages of the general market endpoint for anything
        not covered by _TARGET_SERIES.  Sports are stripped immediately.

        Also performs new-series detection: if a non-sports series with meaningful
        per-market volume appears that isn't in our known list, it is logged as a
        WARNING (so it's visible in the operator log) and added to
        _discovered_series so it gets direct-queried on the next refresh cycle.
        """
        markets: list[dict] = []
        cursor: Optional[str] = None
        for page in range(5):
            params: dict = {"limit": "200"}
            if cursor:
                params["cursor"] = cursor
            try:
                resp  = await self._sess.get("/markets", params=params)
                batch = resp.get("markets") or []
                if not batch:
                    break
                for m in batch:
                    ticker = m.get("ticker", "")
                    if not ticker or _is_sports(ticker) or _is_noise(ticker):
                        continue
                    vol = _vol24_score(m)
                    if vol > 0:
                        markets.append(m)

                    # ── New-series detection ──────────────────────────────────
                    if vol >= self._NEW_SERIES_MIN_VOL:
                        series = m.get("series_ticker") or ticker.split("-")[0]
                        if series and series not in self._known_series and series not in self._discovered_series:
                            self._discovered_series.add(series)
                            self._known_series.add(series)
                            log.warning(
                                "NEW SERIES DISCOVERED: %s  (vol_24h=$%.0f, sample: %s) — "
                                "will query directly next refresh. Add to _TARGET_SERIES to make permanent.",
                                series, vol, ticker,
                            )

                cursor = resp.get("cursor")
                if not cursor:
                    break
            except Exception as exc:
                log.debug("Catch-all fetch error (page %d): %s", page + 1, exc)
                break

        if markets:
            log.debug("Catch-all fetch: %d non-sports markets with volume across %d pages",
                      len(markets), page + 1)
        return markets

    async def _discover_all_series(self) -> None:
        """
        Full series discovery pass — runs once on first fetch.

        Paginates through the entire Kalshi market feed (skipping sports),
        groups by series_ticker, and adds any non-sports series with meaningful
        total volume to _discovered_series for direct querying.

        This is the same logic as discover_series.py but integrated into the bot
        so it always starts with a complete, live series list rather than relying
        on a manually maintained _TARGET_SERIES.  Takes ~10-15 seconds on startup.
        """
        log.info("MarketSelector: running full series discovery scan...")
        from collections import defaultdict
        series_vol: dict[str, float] = defaultdict(float)
        cursor: Optional[str] = None
        pages = 0

        while True:
            params: dict = {"limit": "200", "status": "open"}
            if cursor:
                params["cursor"] = cursor
            try:
                resp  = await self._sess.get("/markets", params=params)
                batch = resp.get("markets") or []
                if not batch:
                    break
                for m in batch:
                    ticker = m.get("ticker", "")
                    if not ticker or _is_sports(ticker) or _is_noise(ticker):
                        continue
                    series = m.get("series_ticker") or ticker.split("-")[0]
                    if series:
                        series_vol[series] += _vol24_score(m)
                cursor = resp.get("cursor")
                pages += 1
                if not cursor:
                    break
                # Safety cap — Kalshi has 50k+ markets, stop after 100 pages (~20k markets)
                # Non-sports series with real volume always appear early in the feed
                if pages >= 100:
                    log.info("MarketSelector: discovery capped at %d pages", pages)
                    break
            except Exception as exc:
                log.warning("MarketSelector: discovery scan error (page %d): %s", pages, exc)
                break

        newly_found = []
        for series, total_vol in series_vol.items():
            if series not in self._known_series and total_vol >= self._NEW_SERIES_MIN_VOL:
                self._discovered_series.add(series)
                self._known_series.add(series)
                newly_found.append((series, total_vol))

        if newly_found:
            newly_found.sort(key=lambda x: x[1], reverse=True)
            log.warning(
                "MarketSelector: discovery found %d new series not in _TARGET_SERIES — "
                "add these permanently to _TARGET_SERIES:\n%s",
                len(newly_found),
                "\n".join(f'    "{s}",   # vol_24h=${v:,.0f}' for s, v in newly_found),
            )
        else:
            log.info("MarketSelector: discovery complete (%d pages) — no new series found", pages)

    async def _fetch_and_rank(self) -> None:
        """
        Fetch markets by querying each financial series directly.
        This avoids paginating through thousands of KXMVE sports parlay markets
        that Kalshi generates automatically and that dominate the default listing.
        """
        try:
            log.info("MarketSelector: fetching markets by series...")
            markets: list[dict] = []

            all_series = list(self._TARGET_SERIES) + sorted(self._discovered_series)
            if self._discovered_series:
                log.info("MarketSelector: also querying %d auto-discovered series: %s",
                         len(self._discovered_series), sorted(self._discovered_series))

            # Fetch all series concurrently in chunks of 5.
            # Chunk size reduced from 10 → 5 to avoid rate-limit silent failures
            # where the API returns an empty market list instead of an error,
            # causing important series (KXFEDDECISION) to silently disappear.
            _CHUNK = 5
            for i in range(0, len(all_series), _CHUNK):
                chunk = all_series[i : i + _CHUNK]
                results = await asyncio.gather(
                    *[self._fetch_series(s) for s in chunk],
                    return_exceptions=True,
                )
                for series, result in zip(chunk, results):
                    if isinstance(result, Exception):
                        log.debug("Series fetch error %s: %s", series, result)
                    elif result:
                        log.debug("  %s: %d markets", series, len(result))
                        markets.extend(result)

            # Supplement with catch-all to catch any series not explicitly listed
            catchall = await self._fetch_catchall()
            existing_tickers = {m.get("ticker") for m in markets}
            new_from_catchall = [m for m in catchall if m.get("ticker") not in existing_tickers]
            if new_from_catchall:
                log.info("Catch-all added %d new markets not in target series", len(new_from_catchall))
            markets.extend(new_from_catchall)

            log.info("MarketSelector: fetched %d markets across %d series + catch-all",
                     len(markets), len(self._TARGET_SERIES))

            if not markets:
                # API returned nothing (network outage during refresh).
                # Keep the previous ticker list if we have one — never downgrade
                # to 7 static tickers just because a single refresh failed.
                if self._current_tickers:
                    log.warning(
                        "MarketSelector: no markets returned — keeping previous "
                        "%d tickers (will retry next refresh cycle)",
                        len(self._current_tickers),
                    )
                else:
                    log.warning("MarketSelector: no markets returned — falling back to static tickers")
                    self._current_tickers = list(self._static_tickers)
                return

            from datetime import datetime, timezone, timedelta
            _OPEN_STATUSES = {"open", "active"}
            _now_utc = datetime.now(timezone.utc)

            def _is_expired(m: dict) -> bool:
                """
                Returns True if the market's close_time has already passed.
                Kalshi sometimes keeps markets as 'open' during the settlement
                processing window — these look tradeable but fills ghost entirely.
                """
                ct = m.get("close_time", "")
                if not ct:
                    return False
                try:
                    return datetime.fromisoformat(ct.replace("Z", "+00:00")) <= _now_utc
                except Exception:
                    return False

            filtered = [
                m for m in markets
                if m.get("ticker")
                and m.get("status", "").lower() in _OPEN_STATUSES
                and not _is_expired(m)
                and _vol24_score(m) >= self._min_volume
                and not (_EXCLUDE_SPORTS and _is_sports(m.get("ticker", "")))
                and not _is_noise(m.get("ticker", ""))
                and not (_EXCLUDE_OIL and any(m.get("ticker", "").startswith(p) for p in _OIL_PREFIXES))
            ]
            if VERBOSE_LOGS:
                log.info(
                    "MarketSelector: %d/%d pass filter (vol≥$%.0f)",
                    len(filtered), len(markets), self._min_volume,
                )

            if not filtered:
                log.warning("MarketSelector: no markets passed filter — falling back to static tickers")
                self._current_tickers = list(self._static_tickers)
                return

            # ── Duration filtering ────────────────────────────────────────────
            # Rules for long-duration markets (close_time > 1 month):
            #   1. Never watch markets closing > 6 months from now
            #   2. Only include 1-month+ markets if active ticker count < 100
            _now        = _now_utc
            _6mo        = _now + timedelta(days=180)
            _1mo        = _now + timedelta(days=30)

            def _close_dt(m):
                ct = m.get("close_time", "")
                if not ct:
                    return None
                try:
                    return datetime.fromisoformat(ct.replace("Z", "+00:00"))
                except Exception:
                    return None

            # Hard cap: exclude anything settling more than 6 months out
            filtered = [
                m for m in filtered
                if (_close_dt(m) or _now) <= _6mo
            ]

            # Split into short-term (≤1 month) and long-term (>1 month)
            short_term = [m for m in filtered if (_close_dt(m) or _now) <= _1mo]
            long_term  = [m for m in filtered if (_close_dt(m) or _now) >  _1mo]

            # Always rank and select short-term markets first
            ranked_short = sorted(short_term, key=_score_market, reverse=True)
            selected     = [m["ticker"] for m in ranked_short[: self._count]]

            # Fill with long-term when short-term count < 100
            if len(selected) < 100 and long_term:
                ranked_long   = sorted(long_term, key=_score_market, reverse=True)
                slots_left    = 100 - len(selected)
                long_selected = [
                    m["ticker"] for m in ranked_long[:slots_left]
                    if m["ticker"] not in selected
                ]
                selected.extend(long_selected)
                if long_selected:
                    log.info(
                        "MarketSelector: added %d long-duration markets (total<40 gate, ≤20¢)",
                        len(long_selected),
                    )

            # ── Forced series inclusion ───────────────────────────────────────
            # Some high-volume series have their total volume spread across many
            # contracts — individual markets may sit below the $5k gate even
            # though the series is genuinely one of the biggest on Kalshi.
            # We guarantee the top 5 markets per forced series (if vol ≥ $500).
            #   KXBTCD/KXETHD: daily crypto — resets each morning, builds intraday
            #   KXWTI: 2nd biggest series on Kalshi (~$547k/day total, ~$2-4k/market)
            #   KXNATGASW: similar volume profile to KXWTI
            _FORCED_SERIES = (
                # Crypto — daily contracts reset each morning, build volume intraday
                "KXBTCD", "KXETHD", "KXBTCW", "KXETHW",
                "KXXRPD", "KXXRPW", "KXSOLD", "KXSOLW", "KXDOGE",
                # Macro — most liquid markets on Kalshi, must never drop off on weekends
                "KXFEDDECISION",                           # Fed meeting rate — $130k/day peak, quiet weekends
                "KXPAYROLLS",                              # Jobs report — major macro event
                "KXINX",                                   # S&P 500 index
                # Commodities — volume spread across many strikes, each below $5k threshold
                "KXWTI", "KXWTID",
                "KXNATGASW", "KXNATGAS",
                # Entertainment / insider flow
                "KXRT",                                    # Rotten Tomatoes — screeners, critics
                # FDA/biotech — pharma insiders, trial investigators
                "KXFDA", "KXBIO",
                # Other high-signal series
                "KXSUPCT",                                 # Supreme Court — law clerks, legal journalists
                "KXEAR",                                   # earnings — corporate insiders
            )
            # Minimum volume to force-include a market from a forced series.
            # Low intentionally: $50 = just a few trades confirming the contract
            # is active.  Daily crypto contracts start at $0 each morning — this
            # ensures they're picked up within minutes of first activity.
            _FORCED_MIN_VOL = 50.0
            all_open = [
                m for m in markets
                if m.get("ticker")
                and m.get("status", "").lower() in _OPEN_STATUSES
                and not _is_sports(m.get("ticker", ""))
                and not _is_noise(m.get("ticker", ""))
            ]
            # KXRT gets more slots — 13 movies are open simultaneously on weekends,
            # and RT is the most weekend-active category (movies release Friday/Saturday).
            # All other series are capped at 5 to avoid over-subscribing thin strikes.
            _SERIES_SLOT_OVERRIDE = {"KXRT": 10}

            for series_prefix in _FORCED_SERIES:
                # Skip oil series if EXCLUDE_OIL is enabled
                if _EXCLUDE_OIL and any(series_prefix.startswith(p) for p in _OIL_PREFIXES):
                    continue
                n_slots = _SERIES_SLOT_OVERRIDE.get(series_prefix, 5)
                candidates = sorted(
                    [m for m in all_open
                     if m.get("ticker", "").startswith(series_prefix)
                     and _vol24_score(m) >= _FORCED_MIN_VOL],
                    key=_vol24_score, reverse=True,
                )
                forced = [m["ticker"] for m in candidates[:n_slots]]
                added  = [t for t in forced if t not in selected]
                if added:
                    selected.extend(added)
                    if VERBOSE_LOGS:
                        log.info("MarketSelector: force-included %d %s markets: %s",
                                 len(added), series_prefix, added)

            self._current_tickers = selected

            # Cache 24h volumes and close times for threshold calculation
            all_tracked = {m["ticker"]: _vol24_score(m) for m in all_open if m.get("ticker")}
            self._market_volumes    = all_tracked
            self._market_close_times = {
                m["ticker"]: m.get("close_time", "")
                for m in all_open if m.get("ticker") and m.get("close_time")
            }

            log.info("MarketSelector: watching %d markets", len(selected))
            if VERBOSE_LOGS:
                market_by_ticker = {m["ticker"]: m for m in markets}
                for t in selected:
                    vol = _vol24_score(market_by_ticker.get(t, {}))
                    threshold = self.get_threshold(t)
                    log.info("  → %s  vol_24h=$%.2f  whale_threshold=$%s", t, vol, threshold)

        except Exception as exc:
            log.error("MarketSelector: fetch failed (%s) — using %s tickers",
                      exc, "existing" if self._current_tickers else "static")
            if not self._current_tickers:
                self._current_tickers = list(self._static_tickers)
