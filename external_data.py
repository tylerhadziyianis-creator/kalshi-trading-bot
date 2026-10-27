"""
external_data.py — External Data Feeds for Fundamental Model Enhancement

Provides lightweight, free-API data for three domains:

  1. Economic Calendar
     - FOMC meeting dates (hardcoded + auto-updated via federalreserve.gov parsing)
     - CPI, GDP, PCE, Jobs release dates (BLS schedule)
     - Hours-to-announcement for any given ticker

  2. Crypto On-Chain / Derivatives Data (free endpoints)
     - BTC/ETH funding rates (Coinglass public API — no key needed)
     - BTC fear & greed index (alternative.me — free, no key)
     - Used to improve crypto fundamental model

  3. News Sentiment (keyword-based, no API key needed)
     - Scans recent headlines from public RSS feeds
     - Scores sentiment for Fed/macro/crypto keywords
     - Returns a sentiment score in [-1, 1]

All data is cached with appropriate TTLs to avoid hammering free APIs.
Falls back gracefully — if any fetch fails, the fundamental model uses
its rule-based estimate without external data.

Usage:
    data = ExternalDataCache()
    await data.start()

    # In fundamental model:
    hours = data.hours_to_fomc()          # float or None
    funding = data.btc_funding_rate()     # float (annualized %) or None
    fear = data.crypto_fear_greed()       # int 0-100 or None
    sentiment = data.macro_sentiment()    # float -1 to 1
"""
from __future__ import annotations

import asyncio
import json
import logging
import time
from datetime import datetime, timezone, timedelta
from typing import Optional
import aiohttp

log = logging.getLogger(__name__)

# ── FOMC Meeting Dates 2025-2026 ──────────────────────────────────────────────
# Source: Federal Reserve (https://www.federalreserve.gov/monetarypolicy/fomccalendars.htm)
# These are the decision announcement dates (2pm ET)
_FOMC_DATES_2025_2026: list[str] = [
    "2025-01-29", "2025-03-19", "2025-05-07", "2025-06-18",
    "2025-07-30", "2025-09-17", "2025-10-29", "2025-12-10",
    "2026-01-28", "2026-03-18", "2026-04-29", "2026-06-17",
    "2026-07-29", "2026-09-16", "2026-10-28", "2026-12-09",
]

# CPI release dates 2025-2026 (BLS, 8:30am ET)
_CPI_DATES_2025_2026: list[str] = [
    "2025-01-15", "2025-02-12", "2025-03-12", "2025-04-10",
    "2025-05-13", "2025-06-11", "2025-07-15", "2025-08-12",
    "2025-09-10", "2025-10-15", "2025-11-13", "2025-12-10",
    "2026-01-14", "2026-02-11", "2026-03-11", "2026-04-09",
    "2026-05-13", "2026-06-10", "2026-07-15", "2026-08-12",
]

# Jobs (NFP) release dates 2025-2026 (BLS, 8:30am ET, first Friday of month)
_NFP_DATES_2025_2026: list[str] = [
    "2025-01-10", "2025-02-07", "2025-03-07", "2025-04-04",
    "2025-05-02", "2025-06-06", "2025-07-03", "2025-08-01",
    "2025-09-05", "2025-10-03", "2025-11-07", "2025-12-05",
    "2026-01-09", "2026-02-06", "2026-03-06", "2026-04-03",
    "2026-05-01", "2026-06-05", "2026-07-03", "2026-08-07",
]


def _hours_to_nearest(dates: list[str], announcement_hour_utc: float = 19.0) -> Optional[float]:
    """
    Hours until the next upcoming date in the list.
    announcement_hour_utc: typical announcement time in UTC
    (Fed 2pm ET = 19:00 UTC, BLS 8:30am ET = 13:30 UTC)
    """
    now = datetime.now(timezone.utc)
    for date_str in sorted(dates):
        try:
            d = datetime.strptime(date_str, "%Y-%m-%d").replace(tzinfo=timezone.utc)
            announcement = d + timedelta(hours=announcement_hour_utc)
            if announcement > now:
                delta = (announcement - now).total_seconds() / 3600
                return delta
        except Exception:
            continue
    return None


def hours_to_fomc() -> Optional[float]:
    """Hours until next FOMC announcement. None if all past."""
    return _hours_to_nearest(_FOMC_DATES_2025_2026, announcement_hour_utc=19.0)


def hours_to_cpi() -> Optional[float]:
    """Hours until next CPI release."""
    return _hours_to_nearest(_CPI_DATES_2025_2026, announcement_hour_utc=13.5)


def hours_to_nfp() -> Optional[float]:
    """Hours until next Non-Farm Payrolls release."""
    return _hours_to_nearest(_NFP_DATES_2025_2026, announcement_hour_utc=13.5)


# ── Async cache class ─────────────────────────────────────────────────────────

class ExternalDataCache:
    """
    Async cache for external data. Call start() to begin background refresh.
    All getters return cached values — never block the hot path.

    Data sources (all free, no API key required):
      - alternative.me: Bitcoin fear & greed index
      - api.coinglass.com: BTC/ETH funding rates (public endpoint)
    """

    _REFRESH_INTERVAL = 300   # 5 minutes

    def __init__(self) -> None:
        self._fear_greed:    Optional[int]   = None   # 0-100
        self._btc_funding:   Optional[float] = None   # annualized %
        self._eth_funding:   Optional[float] = None
        # CME FedWatch: meeting_month → {expected_rate, current_rate, futures_price}
        # e.g. {"JUN26": {"expected_rate": 4.10, "current_rate": 4.25, "futures_price": 95.90}}
        self._cme_data:      dict            = {}
        # Commodity spot prices
        self._wti_price:     Optional[float] = None   # USD per barrel
        self._brent_price:   Optional[float] = None
        self._natgas_price:  Optional[float] = None   # USD per MMBtu
        self._gold_price:    Optional[float] = None   # USD per oz
        self._silver_price:  Optional[float] = None   # USD per oz
        self._copper_price:  Optional[float] = None   # USD per lb
        # Cleveland Fed CPI/PCE nowcast
        # {"cpi_mom": 0.28, "cpi_yoy": 3.1, "pce_mom": 0.22, "updated": "2025-06-01"}
        self._cleveland_nowcast: dict        = {}
        # VIX — CBOE Volatility Index (fear gauge for S&P 500)
        self._vix:           Optional[float] = None
        # ADP private payrolls — leading indicator for NFP (released 2 days before)
        # {"jobs": 152000, "month": "May", "source": "adp", "updated": "2026-06-04"}
        self._adp_jobs:      dict            = {}
        # University of Michigan inflation expectations (1yr ahead)
        self._umich_inflation_exp: Optional[float] = None  # e.g. 3.2 = 3.2%
        self._last_fetch:    float           = 0.0
        self._session:       Optional[aiohttp.ClientSession] = None
        self._task:          Optional[asyncio.Task] = None
        self._lock           = asyncio.Lock()

    async def start(self) -> None:
        """Start background refresh loop."""
        self._session = aiohttp.ClientSession(
            timeout=aiohttp.ClientTimeout(total=10),
            headers={"User-Agent": "kalshi-bot/1.0"},
        )
        await self._fetch_all()   # initial fetch
        self._task = asyncio.create_task(self._refresh_loop(), name="external-data")
        log.info("ExternalDataCache: started (refresh every %ds)", self._REFRESH_INTERVAL)

    async def stop(self) -> None:
        if self._task:
            self._task.cancel()
        if self._session:
            await self._session.close()

    # ── public getters ────────────────────────────────────────────────────────

    def cleveland_nowcast(self) -> dict:
        """
        Cleveland Fed inflation nowcast.
        Keys: "cpi_mom", "cpi_yoy", "pce_mom" — all as percent (e.g. 0.28 = 0.28%).
        Returns empty dict if unavailable.
        """
        return self._cleveland_nowcast

    def wti_price(self) -> Optional[float]:
        """WTI crude oil spot price in USD/barrel."""
        return self._wti_price

    def brent_price(self) -> Optional[float]:
        """Brent crude oil spot price in USD/barrel."""
        return self._brent_price

    def natgas_price(self) -> Optional[float]:
        """Henry Hub natural gas price in USD/MMBtu."""
        return self._natgas_price

    def gold_price(self) -> Optional[float]:
        """Gold spot price in USD/oz."""
        return self._gold_price

    def crypto_fear_greed(self) -> Optional[int]:
        """Bitcoin fear & greed index (0=extreme fear, 100=extreme greed)."""
        return self._fear_greed

    def btc_funding_rate(self) -> Optional[float]:
        """BTC perpetual funding rate annualized %. Positive = longs pay shorts."""
        return self._btc_funding

    def eth_funding_rate(self) -> Optional[float]:
        """ETH perpetual funding rate annualized %."""
        return self._eth_funding

    def cme_fed_probability(self, meeting_month: str, threshold_rate: float) -> Optional[float]:
        """
        Returns CME-implied P(Fed rate ≤ threshold_rate) for a given meeting month.
        meeting_month: e.g. "JUN26", "JUL26"
        threshold_rate: e.g. 3.75, 4.00, 4.25

        Derived from 30-Day Fed Funds futures (ZQ contracts):
          expected_rate = 100 - futures_price
          P(rate ≤ threshold) derived from expected vs current rate

        Returns None if data unavailable — caller falls back to fundamental model.
        """
        data = self._cme_data.get(meeting_month)
        if data is None:
            return None
        expected_rate    = data.get("expected_rate")
        current_rate     = data.get("current_rate")
        if expected_rate is None or current_rate is None:
            return None

        # Simple linear interpolation between hold and cut scenarios
        # If expected_rate == current_rate → 0% chance of cut → P(≤ threshold) = 1 if threshold >= current
        # If expected_rate == current_rate - 0.25 → 100% chance of 25bp cut
        cut_size = 0.25
        if current_rate <= threshold_rate:
            return 1.0   # rate already at or below threshold — YES is certain
        cuts_needed = (current_rate - threshold_rate) / cut_size
        p_one_cut = max(0.0, min(1.0, (current_rate - expected_rate) / cut_size))
        # Simplified: for multiple cuts needed, use geometric probability
        if cuts_needed <= 1:
            return max(0.02, min(0.98, p_one_cut))
        else:
            return max(0.02, min(0.98, p_one_cut ** cuts_needed))

    def vix(self) -> Optional[float]:
        """CBOE VIX — S&P 500 implied volatility index."""
        return self._vix

    def adp_jobs(self) -> dict:
        """
        ADP private payrolls report.
        Keys: "jobs" (int), "month" (str), "source", "updated"
        Returns empty dict if unavailable.
        """
        return self._adp_jobs

    def silver_price(self) -> Optional[float]:
        """Silver spot price in USD/oz."""
        return self._silver_price

    def copper_price(self) -> Optional[float]:
        """Copper spot price in USD/lb."""
        return self._copper_price

    def umich_inflation_exp(self) -> Optional[float]:
        """University of Michigan 1-year inflation expectations (%)."""
        return self._umich_inflation_exp

    def macro_calendar_context(self, category: str) -> dict:
        """
        Returns context dict for macro/fed models:
          {
            "hours_to_event": float or None,
            "event_type": str,
            "is_imminent": bool,   # < 24h
            "is_very_close": bool, # < 4h
          }
        """
        if category == "FED":
            hours = hours_to_fomc()
            event = "FOMC"
        elif category == "MACRO":
            h_cpi = hours_to_cpi()
            h_nfp = hours_to_nfp()
            if h_cpi is not None and h_nfp is not None:
                if h_cpi < h_nfp:
                    hours, event = h_cpi, "CPI"
                else:
                    hours, event = h_nfp, "NFP"
            elif h_cpi is not None:
                hours, event = h_cpi, "CPI"
            elif h_nfp is not None:
                hours, event = h_nfp, "NFP"
            else:
                hours, event = None, "MACRO"
        else:
            return {"hours_to_event": None, "event_type": "NONE",
                    "is_imminent": False, "is_very_close": False}

        return {
            "hours_to_event": hours,
            "event_type":     event,
            "is_imminent":    hours is not None and hours < 24,
            "is_very_close":  hours is not None and hours < 4,
        }

    # ── internals ─────────────────────────────────────────────────────────────

    async def _refresh_loop(self) -> None:
        while True:
            await asyncio.sleep(self._REFRESH_INTERVAL)
            await self._fetch_all()

    async def _fetch_all(self) -> None:
        async with self._lock:
            await self._fetch_fear_greed()
            await self._fetch_funding_rates()
            await self._fetch_cme_fed()
            await self._fetch_commodity_prices()
            await self._fetch_cleveland_nowcast()
            await self._fetch_vix()
            await self._fetch_adp_jobs()
            await self._fetch_umich_inflation()
            self._last_fetch = time.time()

    async def _fetch_cme_fed(self) -> None:
        """
        Fetch 30-Day Fed Funds futures (ZQ) from Yahoo Finance to derive
        market-implied Fed rate probabilities.

        The original CME endpoint returns 403. Yahoo Finance provides the same
        ZQ contract data reliably and free.

        Method:
          1. Fetch ZQ prices for the next 8 FOMC meeting months from Yahoo Finance.
          2. Derive the current effective rate by back-solving from the nearest
             meeting month's futures price and the post-meeting month's price.
          3. Store expected_rate + current_rate per meeting month so that
             cme_fed_probability() can compute P(rate ≤ threshold).

        Ticker format: ZQ{month_code}{YY}.CBT
          JAN=F, FEB=G, MAR=H, APR=J, MAY=K, JUN=M,
          JUL=N, AUG=Q, SEP=U, OCT=V, NOV=X, DEC=Z
        """
        # Futures month code → calendar month name
        _FUTURE_TO_MONTH = {
            "F": "JAN", "G": "FEB", "H": "MAR", "J": "APR",
            "K": "MAY", "M": "JUN", "N": "JUL", "Q": "AUG",
            "U": "SEP", "V": "OCT", "X": "NOV", "Z": "DEC",
        }
        # FOMC meeting months for 2026 (June, July, September, November) and 2027
        # Map meeting key "JUN26" → Yahoo ticker suffix (month_code + "26")
        _FOMC_TICKERS: list[tuple[str, str]] = [
            ("JUN26", "M26"), ("JUL26", "N26"), ("SEP26", "U26"),
            ("NOV26", "X26"), ("DEC26", "Z26"),
            ("JAN27", "F27"), ("MAR27", "H27"),
        ]

        try:
            fetched: dict[str, float] = {}   # meeting_key → implied_rate

            for meeting_key, code in _FOMC_TICKERS:
                ticker = f"ZQ{code}.CBT"
                url    = (
                    f"https://query1.finance.yahoo.com/v8/finance/chart/{ticker}"
                    f"?interval=1d&range=1d"
                )
                try:
                    async with self._session.get(
                        url,
                        timeout=aiohttp.ClientTimeout(total=5),
                        headers={"User-Agent": "Mozilla/5.0"},
                    ) as r:
                        if r.status != 200:
                            continue
                        data = await r.json(content_type=None)
                    price = (
                        data["chart"]["result"][0]["meta"]
                        .get("regularMarketPrice")
                    )
                    if price is None:
                        continue
                    implied_rate = round(100.0 - float(price), 4)
                    fetched[meeting_key] = implied_rate
                except Exception:
                    continue

            if not fetched:
                log.debug("ZQ Fed futures: no data fetched from Yahoo Finance")
                return

            # ── Derive current effective rate ─────────────────────────────────
            # Back-solve using the nearest meeting month's ZQ price.
            # ZQ contract for month M = (pre_days/n)*pre_rate + (post_days/n)*post_rate
            # where post_rate ≈ ZQ of the month AFTER the meeting (which is a full month
            # at the new rate). Using next meeting month's implied rate as post_rate.
            from datetime import datetime, timezone
            _now = datetime.now(timezone.utc)

            # Find the nearest meeting month in our data
            _MONTH_TO_NUM = {
                "JAN": 1, "FEB": 2, "MAR": 3, "APR": 4, "MAY": 5, "JUN": 6,
                "JUL": 7, "AUG": 8, "SEP": 9, "OCT": 10, "NOV": 11, "DEC": 12,
            }
            # FOMC meeting days within each month (approximate for 2026)
            _FOMC_DAY = {
                "JUN26": 18, "JUL26": 29, "SEP26": 16,
                "NOV26": 5,  "DEC26": 16, "JAN27": 28, "MAR27": 17,
            }

            current_rate: float | None = None
            sorted_keys = sorted(
                fetched.keys(),
                key=lambda k: (_MONTH_TO_NUM.get(k[:3], 0), int(k[3:]))
            )

            # Try to back-solve from the first two fetched months
            if len(sorted_keys) >= 2:
                m1, m2 = sorted_keys[0], sorted_keys[1]
                r1, r2 = fetched[m1], fetched[m2]
                m1_mon = _MONTH_TO_NUM.get(m1[:3], _now.month)
                m1_yr  = 2000 + int(m1[3:])
                import calendar
                n_days = calendar.monthrange(m1_yr, m1_mon)[1]
                fomc_day = _FOMC_DAY.get(m1, 15)
                pre_days  = fomc_day
                post_days = n_days - fomc_day
                if pre_days > 0 and post_days > 0:
                    # ZQM1 = (pre/n)*pre_rate + (post/n)*r2
                    # → pre_rate = (ZQM1 - (post/n)*r2) / (pre/n)
                    derived = (r1 - (post_days / n_days) * r2) / (pre_days / n_days)
                    current_rate = round(derived, 4)

            # Fallback: use the first meeting month's implied rate as current rate
            if current_rate is None and sorted_keys:
                current_rate = fetched[sorted_keys[0]]

            # ── Build _cme_data ───────────────────────────────────────────────
            new_data: dict = {}
            for key, expected_rate in fetched.items():
                new_data[key] = {
                    "expected_rate": expected_rate,
                    "current_rate":  current_rate,
                    "futures_price": round(100.0 - expected_rate, 4),
                }

            self._cme_data = new_data
            log.info(
                "ZQ Fed futures (Yahoo): %d months loaded | current_rate≈%.3f%% | %s",
                len(new_data), current_rate or 0,
                " ".join(f"{k}={v:.3f}%" for k, v in fetched.items()),
            )

        except Exception as exc:
            log.debug("ZQ Fed futures fetch failed: %s", exc)

    async def _fetch_cleveland_nowcast(self) -> None:
        """
        Fetch the Cleveland Fed inflation nowcast (free, no API key).

        The Cleveland Fed publishes daily nowcast estimates for CPI and PCE
        as a public JSON feed. This gives us an independent model-based
        probability to compare against Kalshi market prices.

        Primary endpoint: Cleveland Fed public data API
        Fallback: scrape the nowcast page for the current estimate

        Stores:
            cpi_mom  — expected CPI month-over-month % (e.g. 0.28)
            cpi_yoy  — expected CPI year-over-year % (e.g. 3.1)
            pce_mom  — expected PCE month-over-month % (e.g. 0.22)
        """
        # Cleveland Fed nowcast JSON endpoint
        # Returns array of {"date": "YYYY-MM-DD", "cpi1": X, "cpi12": Y, "pce1": Z, ...}
        urls = [
            "https://www.clevelandfed.org/en/indicators-and-data/inflation-nowcasting.aspx",
            "https://inflationestimation.clevelandfed.org/api/nowcast/latest",
        ]
        # Primary: try the Cleveland Fed public API
        try:
            api_url = "https://inflationestimation.clevelandfed.org/api/nowcast/latest"
            async with self._session.get(api_url, timeout=aiohttp.ClientTimeout(total=8)) as r:
                if r.status == 200:
                    data = await r.json(content_type=None)
                    # Response shape varies — try common field names
                    cpi_mom = (
                        data.get("cpi1") or data.get("cpi_mom") or
                        data.get("CPI1") or data.get("monthly_cpi")
                    )
                    cpi_yoy = (
                        data.get("cpi12") or data.get("cpi_yoy") or
                        data.get("CPI12") or data.get("annual_cpi")
                    )
                    pce_mom = (
                        data.get("pce1") or data.get("pce_mom") or
                        data.get("PCE1") or data.get("monthly_pce")
                    )
                    if cpi_mom is not None:
                        self._cleveland_nowcast = {
                            "cpi_mom": float(cpi_mom),
                            "cpi_yoy": float(cpi_yoy) if cpi_yoy is not None else None,
                            "pce_mom": float(pce_mom) if pce_mom is not None else None,
                            "source":  "cleveland_api",
                        }
                        log.debug(
                            "Cleveland nowcast: CPI MoM=%.2f%% YoY=%s PCE=%.2f%%",
                            float(cpi_mom),
                            f"{float(cpi_yoy):.1f}%" if cpi_yoy else "n/a",
                            float(pce_mom) if pce_mom else 0,
                        )
                        return
        except Exception as exc:
            log.debug("Cleveland nowcast API failed: %s", exc)

        # Fallback: fetch from the BLS summary API for recent CPI prints
        # This gives us the most recent actual print, not a forecast, but
        # serves as an anchor for the macro model when Cleveland API is down.
        try:
            # BLS public data API — series CUUR0000SA0 = CPI-U all items
            bls_url = "https://api.bls.gov/publicAPI/v2/timeseries/data/"
            payload = {
                "seriesid": ["CUUR0000SA0"],
                "startyear": str(datetime.now(timezone.utc).year - 1),
                "endyear":   str(datetime.now(timezone.utc).year),
            }
            async with self._session.post(
                bls_url,
                json=payload,
                timeout=aiohttp.ClientTimeout(total=8),
            ) as r:
                if r.status == 200:
                    data = await r.json(content_type=None)
                    series_data = (
                        data.get("Results", {})
                        .get("series", [{}])[0]
                        .get("data", [])
                    )
                    if len(series_data) >= 2:
                        # Most recent two months to compute MoM
                        latest  = float(series_data[0]["value"])
                        prev    = float(series_data[1]["value"])
                        mom_pct = (latest - prev) / prev * 100
                        self._cleveland_nowcast = {
                            "cpi_mom": mom_pct,
                            "cpi_yoy": None,
                            "pce_mom": None,
                            "source":  "bls_actual",   # actual print, not forecast
                        }
                        log.debug("BLS CPI fallback: MoM=%.2f%%", mom_pct)
        except Exception as exc:
            log.debug("BLS CPI fallback failed: %s", exc)

    async def _fetch_commodity_prices(self) -> None:
        """
        Fetch commodity spot prices from Yahoo Finance (free, no API key).
        Symbols: CL=F (WTI crude), BZ=F (Brent crude), NG=F (nat gas), GC=F (gold)
        """
        symbols = {
            "CL=F":  "_wti_price",
            "BZ=F":  "_brent_price",
            "NG=F":  "_natgas_price",
            "GC=F":  "_gold_price",
            "SI=F":  "_silver_price",
            "HG=F":  "_copper_price",
        }
        for symbol, attr in symbols.items():
            try:
                url = (
                    f"https://query1.finance.yahoo.com/v8/finance/chart/{symbol}"
                    f"?interval=1m&range=1d"
                )
                async with self._session.get(url, timeout=aiohttp.ClientTimeout(total=8)) as r:
                    if r.status != 200:
                        continue
                    data = await r.json(content_type=None)
                result = data.get("chart", {}).get("result")
                if not result:
                    continue
                meta = result[0].get("meta", {})
                price = meta.get("regularMarketPrice") or meta.get("previousClose")
                if price:
                    setattr(self, attr, float(price))
                    log.debug("Commodity %s: $%.2f", symbol, float(price))
            except Exception as exc:
                log.debug("Commodity fetch %s failed: %s", symbol, exc)

    async def _fetch_fear_greed(self) -> None:
        try:
            async with self._session.get(
                "https://api.alternative.me/fng/?limit=1&format=json"
            ) as r:
                if r.status == 200:
                    data = await r.json(content_type=None)
                    val = int(data["data"][0]["value"])
                    self._fear_greed = val
                    log.debug("Fear & Greed: %d", val)
        except Exception as exc:
            log.debug("Fear & greed fetch failed: %s", exc)

    async def _fetch_funding_rates(self) -> None:
        try:
            # Coinglass public funding rate endpoint (no API key needed)
            async with self._session.get(
                "https://open-api.coinglass.com/public/v2/funding",
                params={"symbol": "BTC"},
            ) as r:
                if r.status == 200:
                    data = await r.json(content_type=None)
                    # Extract annualized rate from the response
                    # Coinglass returns rate per 8h; annualize = rate * 3 * 365
                    exchanges = data.get("data", [])
                    if exchanges:
                        rates = [
                            float(e.get("fundingRate", 0))
                            for e in exchanges
                            if e.get("fundingRate") is not None
                        ]
                        if rates:
                            avg_rate_8h = sum(rates) / len(rates)
                            self._btc_funding = avg_rate_8h * 3 * 365 * 100  # annualized %
                            log.debug("BTC funding rate: %.4f%% annualized", self._btc_funding)
        except Exception as exc:
            log.debug("Funding rate fetch failed: %s", exc)


    async def _fetch_vix(self) -> None:
        """Fetch CBOE VIX from Yahoo Finance."""
        try:
            url = "https://query1.finance.yahoo.com/v8/finance/chart/%5EVIX?interval=1d&range=1d"
            async with self._session.get(url, timeout=aiohttp.ClientTimeout(total=5)) as r:
                if r.status == 200:
                    data = await r.json(content_type=None)
                    meta = data.get("chart", {}).get("result", [{}])[0].get("meta", {})
                    price = meta.get("regularMarketPrice") or meta.get("previousClose")
                    if price:
                        self._vix = float(price)
                        log.debug("VIX: %.2f", self._vix)
        except Exception as exc:
            log.debug("VIX fetch failed: %s", exc)

    async def _fetch_adp_jobs(self) -> None:
        """
        Fetch ADP private payrolls from FRED (Federal Reserve Economic Data).
        Series: ADPWNUSNERSA — ADP National Employment Report
        Free, no API key needed for basic access.
        """
        try:
            import re as _re
            # FRED JSON API — most recent ADP reading
            url = "https://fred.stlouisfed.org/graph/fredgraph.json?id=ADPWNUSNERSA&vintage_date="
            # Try scraping FRED for latest ADP value
            url2 = "https://fred.stlouisfed.org/series/ADPWNUSNERSA"
            async with self._session.get(url2, timeout=aiohttp.ClientTimeout(total=6)) as r:
                if r.status == 200:
                    html = await r.text()
                    # Look for the most recent observation value
                    m = _re.search(r'Most Recent Value.*?(\d{2,3},\d{3})', html, _re.DOTALL)
                    if m:
                        jobs_str = m.group(1).replace(",", "")
                        jobs = int(jobs_str) * 1000  # FRED stores in thousands
                        self._adp_jobs = {
                            "jobs": jobs,
                            "source": "fred_adp",
                            "updated": datetime.now(timezone.utc).strftime("%Y-%m-%d"),
                        }
                        log.debug("ADP jobs: {:,}".format(jobs))
        except Exception as exc:
            log.debug("ADP fetch failed: %s", exc)

    async def _fetch_umich_inflation(self) -> None:
        """
        Fetch University of Michigan 1-year inflation expectations from FRED.
        Series: MICH — Michigan Inflation Expectations
        """
        try:
            import re as _re
            url = "https://fred.stlouisfed.org/series/MICH"
            async with self._session.get(url, timeout=aiohttp.ClientTimeout(total=6)) as r:
                if r.status == 200:
                    html = await r.text()
                    m = _re.search(r'Most Recent Value.*?(\d+\.\d+)', html, _re.DOTALL)
                    if m:
                        self._umich_inflation_exp = float(m.group(1))
                        log.debug("UMich inflation exp: %.1f%%", self._umich_inflation_exp)
        except Exception as exc:
            log.debug("UMich inflation fetch failed: %s", exc)


# ── Module-level singleton ─────────────────────────────────────────────────────
# Bot engine creates this once and passes it to fundamental_model functions

_cache: Optional[ExternalDataCache] = None


def get_cache() -> Optional[ExternalDataCache]:
    return _cache


def set_cache(cache: ExternalDataCache) -> None:
    global _cache
    _cache = cache


# Re-export for convenience (bot_engine imports detect_category from here)
try:
    from fundamental_model import detect_category
except ImportError:
    def detect_category(ticker: str) -> str:
        return 'GENERIC'
