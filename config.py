"""
config.py — Single source of truth for every tunable parameter.

ALL monetary fields use the v2 string-dollar convention.
Zero references to deprecated integer cent fields anywhere in the codebase.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from decimal import Decimal
import os
from dotenv import load_dotenv

load_dotenv()

# ─── Credentials (override via env vars in main.py) ───────────────────────────
KALSHI_API_KEY_ID: str = os.getenv("KALSHI_API_KEY", "")
KALSHI_PRIVATE_KEY_PATH: str = os.getenv("KALSHI_PRIVATE_KEY_PATH", "./private_key.pem")

# ─── API Endpoints ────────────────────────────────────────────────────────────
# Kalshi migrated from trading-api.kalshi.com → api.elections.kalshi.com
REST_BASE_URL: str = "https://api.elections.kalshi.com/trade-api/v2"
WS_BASE_URL:   str = "wss://api.elections.kalshi.com/trade-api/ws/v2"

DEMO_REST_BASE_URL: str = "https://demo-api.kalshi.co/trade-api/v2"
DEMO_WS_BASE_URL:   str = "wss://demo-api.kalshi.co/trade-api/ws/v2"

# ─── Exchange Sharding ────────────────────────────────────────────────────────
SHARD_ROUTING: dict[str, str] = {
    "KXBTCD":  "https://api.elections.kalshi.com/trade-api/v2",
    "KXETHD":  "https://api.elections.kalshi.com/trade-api/v2",
    "KXINX":   "https://api.elections.kalshi.com/trade-api/v2",
    "KXFED":   "https://api.elections.kalshi.com/trade-api/v2",
    "KXOIL":   "https://api.elections.kalshi.com/trade-api/v2",
    "KXGEO":   "https://api.elections.kalshi.com/trade-api/v2",
    "DEFAULT": "https://api.elections.kalshi.com/trade-api/v2",
}

# ─── Whale Filter ─────────────────────────────────────────────────────────────
# !! CRITICAL !!
# The user_orders WebSocket stream is extremely high-frequency.
# We ONLY react to a trade if its dollar notional >= this threshold.
# Anything below is noise.  Set conservatively high.
WHALE_MIN_NOTIONAL_USD: Decimal = Decimal(os.getenv("WHALE_MIN_NOTIONAL_USD", "5000.00"))

# Tracking threshold — lower than trading threshold.
# Trades between TRACK and MIN are shadow-tracked for win/loss data only.
# No real or simulated orders are placed. Builds whale quality database over time.
WHALE_TRACK_NOTIONAL_USD: Decimal = Decimal(os.getenv("WHALE_TRACK_NOTIONAL_USD", "200.00"))

    # ─── Capital & Risk ──────────────────────────────────────────────────────
# !! UPDATE THIS to match your actual live account balance before starting !!
STARTING_BALANCE_USD: Decimal  = Decimal(os.getenv("STARTING_BALANCE_USD", "100.00"))

# Fractional Kelly: fraction of full-Kelly to apply.
# Quarter-Kelly is the right starting point for an unproven live system.
KELLY_FRACTION: float          = 0.25

# Hard cap: never risk more than 3% of balance on a single trade ($3 on $100).
# The percentage stays the same — absolute dollar cap scales with balance.
MAX_SINGLE_TRADE_PCT: float    = 0.03          # 3 %
MAX_SINGLE_TRADE_USD: Decimal  = Decimal(os.getenv("MAX_SINGLE_TRADE_USD", "3.00"))

# Portfolio-level cap: never have more than 20% of balance at risk simultaneously.
# Tighter than before ($20 max exposure on a $100 account).
MAX_TOTAL_EXPOSURE_PCT: float  = 0.29          # 29 % — ~$50 on $175 balance

# Premium exposure zone: high-quality arbs and large whales can use up to 30%.
# A trade qualifies as premium if:
#   - Arb: edge ≥ PREMIUM_ARB_MIN_EDGE (6¢) OR edge_per_day ≥ PREMIUM_ARB_MIN_EPD
#   - Whale: notional ≥ PREMIUM_WHALE_NOTIONAL_USD ($3K)
MAX_PREMIUM_EXPOSURE_PCT: float       = 0.30   # 30 % — premium ceiling
PREMIUM_ARB_MIN_EDGE: float           = 0.06   # 6¢ edge → premium arb
PREMIUM_ARB_MIN_EDGE_PER_DAY: float   = 0.005  # 0.5¢/day → fast-settling premium arb
PREMIUM_WHALE_NOTIONAL_USD: float     = 3000.0 # $3K+ notional → premium whale

# Dynamic minimum edge based on fill speed.
# When fills average < FAST_FILL_THRESHOLD_SEC, the market is liquid — raise the
# bar so we only take the best opportunities and don't dilute capital.
FAST_FILL_THRESHOLD_SEC: float = 15.0   # avg fill < 15s → hot market
FAST_FILL_MIN_EDGE: float      = 0.05   # 5¢ floor in hot markets (vs normal 3¢)
FILL_SPEED_WINDOW: int         = 10     # rolling window of recent fills to average

# Minimum implied edge (Kelly fraction) required before we enter a trade
MIN_KELLY_EDGE: float          = 0.003         # 0.3% positive edge floor

# Daily Loss Limit: stop trading if we lose more than 10% of starting capital in a day.
MAX_DAILY_LOSS_USD: Decimal    = Decimal(os.getenv("MAX_DAILY_LOSS_USD", "10.00"))

# Consecutive Loss Limit: stop after this many consecutive losses
MAX_CONSECUTIVE_LOSSES: int    = 4


# ─── Order Mechanics ──────────────────────────────────────────────────────────
TICK_SIZE: Decimal             = Decimal("0.0001")   # $0.0001 per contract
MAKER_IMPROVE_TICKS: int       = 1                   # improve by 1 tick vs whale price
POST_ONLY: bool                = True                # NEVER take liquidity

# Ghost-Order Cleaner: cancel & replace stale resting orders every N seconds
STALE_ORDER_TTL_SEC: int       = 30

# ─── Rate Limiter (Token Bucket — 2026 Kalshi cost system) ────────────────────
TOKEN_BUCKET_CAPACITY: int     = 200      # max burst
TOKEN_REFILL_RATE: float       = 10.0    # tokens / second

# Per-operation token costs
TOKEN_COSTS: dict[str, int]    = {
    "read_market":      1,
    "read_orderbook":   2,
    "write_order":      10,
    "cancel_order":     5,
    "get_balance":      1,
    "get_settlements":  1,
    "authenticate":     0,   # free
}

# ─── Session & Auth ───────────────────────────────────────────────────────────
# Kalshi tokens expire ~30 min.  Refresh proactively at 25 min.
TOKEN_REFRESH_INTERVAL_SEC: int = 25 * 60
HTTP_TIMEOUT_SEC: float         = 10.0

# ─── Logging verbosity ───────────────────────────────────────────────────────
# Set VERBOSE_LOGS=true to restore detailed ticker/arb/refresh logs for debugging.
VERBOSE_LOGS: bool = os.getenv("VERBOSE_LOGS", "false").lower() == "true"

# ─── WebSocket ────────────────────────────────────────────────────────────────
WS_PING_INTERVAL_SEC: int      = 20
WS_RECONNECT_BASE_SEC: float   = 2.0
WS_RECONNECT_MAX_SEC: float    = 30.0

# ─── Cache TTLs ───────────────────────────────────────────────────────────────
MARKET_CACHE_TTL_SEC: int      = 300    # static market metadata: 5 min
BALANCE_CACHE_TTL_SEC: int     = 60     # balance: 1 min
SETTLEMENT_CACHE_TTL_SEC: int  = 300    # settlements: 5 min

# ─── Background Task Intervals ────────────────────────────────────────────────
DECAY_INTERVAL_SECONDS: int             = 3600   # whale decay: every 60 min
BALANCE_SYNC_INTERVAL_SECONDS: int      = int(os.getenv("BALANCE_SYNC_INTERVAL_SECONDS", "900"))  # balance sync: every 15 min
SETTLEMENT_CHECK_INTERVAL_SECONDS: int  = 300    # settlement check: every 5 min

# ─── Whale Registry ───────────────────────────────────────────────────────────
WHALE_CONVERGENCE_BOOST: float  = 2.0   # multiply confidence when 2+ categories agree
WHALE_DECAY_WINRATE: float      = 0.55  # 7-day win-rate floor
WHALE_DECAY_MULTIPLIER: float   = 0.50  # cut weight by half below the floor
WHALE_LOOKBACK_DAYS: int        = 7
WHALE_MIN_RESOLVED_TRADES: int  = 5     # need at least 5 results to apply decay
WHALE_MIN_SAMPLE_SIZE: int      = 30    # min resolved trades before trusting a whale


# ─── Logging ──────────────────────────────────────────────────────────────────
LOG_LEVEL: str = "INFO"
LOG_FILE: str  = "kalshi_bot.log"

# ─── Dry-Run / Paper-Trade Mode ───────────────────────────────────────────────
# When true: bot connects to live WS, generates signals, and logs what it WOULD
# do — but never submits real orders.  Use this to validate production data
# before committing capital.
DRY_RUN: bool = os.getenv("DRY_RUN", "false").lower() == "true"
# How many seconds before a dry-run order is considered "filled".
# Set to 0 to disable simulated fills (orders only cancel via ghost cleaner).
DRY_RUN_FILL_DELAY_SEC: int = int(os.getenv("DRY_RUN_FILL_DELAY_SEC", "25"))

# ─── State Persistence ────────────────────────────────────────────────────────
STATE_FILE: str = os.getenv("STATE_FILE", "./bot_state.json")

# ─── Whale Watchlist ──────────────────────────────────────────────────────────
WATCHLIST_FILE: str = os.getenv("WATCHLIST_FILE", "./watchlist.json")
# Minimum resolved trades before auto-promoting a whale to "proven"
WATCHLIST_MIN_RESOLVED_PROMOTE: int   = int(os.getenv("WATCHLIST_MIN_RESOLVED_PROMOTE", "10"))
# Win rate required for promotion
WATCHLIST_MIN_WINRATE_PROMOTE:  float = float(os.getenv("WATCHLIST_MIN_WINRATE_PROMOTE", "0.60"))
# Win rate below which a whale is demoted
WATCHLIST_MAX_WINRATE_DEMOTE:   float = float(os.getenv("WATCHLIST_MAX_WINRATE_DEMOTE",  "0.45"))
# Minimum resolved trades before auto-demotion is applied
WATCHLIST_MIN_RESOLVED_DEMOTE:  int   = int(os.getenv("WATCHLIST_MIN_RESOLVED_DEMOTE",  "5"))
# Weight multiplier for proven whales (stacks on top of registry weight)
WATCHLIST_PROVEN_BOOST:         float = float(os.getenv("WATCHLIST_PROVEN_BOOST",        "1.5"))
# Fixed weight for demoted whales (replaces registry weight)
WATCHLIST_DEMOTED_WEIGHT:       float = float(os.getenv("WATCHLIST_DEMOTED_WEIGHT",      "0.10"))
# How many days of inactivity before a whale profile is evicted from memory
WHALE_EVICTION_DAYS: int = int(os.getenv("WHALE_EVICTION_DAYS", "30"))

# ─── Aggressive Execution ─────────────────────────────────────────────────────
ENABLE_AGGRESSIVE_EXECUTION: bool = os.getenv("ENABLE_AGGRESSIVE_EXECUTION", "true").lower() == "true"
MAX_CHASE_CENTS: int               = int(os.getenv("MAX_CHASE_CENTS", "3"))

# ─── Signal Engine (Hybrid Model) ────────────────────────────────────────────
# Minimum whale confidence to even consider a signal (pre-fundamental-filter)
MIN_WHALE_CONFIDENCE: float   = float(os.getenv("MIN_WHALE_CONFIDENCE",   "0.25"))
# Minimum settled trades a whale must have in THIS category before we follow them
MIN_CATEGORY_TRADES: int      = int(os.getenv("MIN_CATEGORY_TRADES",     "3"))
# Whale must beat this win rate in the specific category
MIN_CATEGORY_WIN_RATE: float  = float(os.getenv("MIN_CATEGORY_WIN_RATE", "0.50"))
# Minimum mispricing (fair_value - market_price) to justify entry
MIN_MISPRICING: float         = float(os.getenv("MIN_MISPRICING",        "0.015"))
# Minimum composite confidence (after blending all signals)
MIN_COMPOSITE_CONF: float     = float(os.getenv("MIN_COMPOSITE_CONF",    "0.30"))
# How much weight to give the fundamental model vs whale signal
FUNDAMENTAL_WEIGHT: float     = float(os.getenv("FUNDAMENTAL_WEIGHT",    "0.35"))
# Market price anchor weight
MARKET_WEIGHT: float          = float(os.getenv("MARKET_WEIGHT",         "0.15"))
# Bonus confidence when whale + fundamental + market all agree
AGREEMENT_BONUS: float        = float(os.getenv("AGREEMENT_BONUS",       "0.20"))
# Skip trade if bid-ask spread exceeds this (in dollars)
MAX_SPREAD_TO_TRADE: float    = float(os.getenv("MAX_SPREAD_TO_TRADE",   "0.12"))
# Only trade markets with genuine uncertainty — skip near-certain markets
MIN_MARKET_PRICE: float       = float(os.getenv("MIN_MARKET_PRICE",      "0.01"))
MAX_MARKET_PRICE: float       = float(os.getenv("MAX_MARKET_PRICE",      "0.95"))

# ─── Dynamic Market Selection ─────────────────────────────────────────────────
AUTO_SELECT_MARKETS: bool          = os.getenv("AUTO_SELECT_MARKETS", "false").lower() == "true"
ENABLE_SUM_ARB: bool               = os.getenv("ENABLE_SUM_ARB", "false").lower() == "true"
AUTO_MARKET_COUNT: int             = int(os.getenv("AUTO_MARKET_COUNT", "20"))
AUTO_MARKET_REFRESH_SECONDS: int   = int(os.getenv("AUTO_MARKET_REFRESH_SECONDS", "900"))
AUTO_MARKET_MIN_VOLUME: float      = float(os.getenv("AUTO_MARKET_MIN_VOLUME", "100"))


# ─── Typed Config Object ──────────────────────────────────────────────────────

@dataclass
class BotConfig:
    """Immutable-ish config object threaded through every subsystem."""

    api_key_id: str                 = KALSHI_API_KEY_ID
    private_key_path: str           = KALSHI_PRIVATE_KEY_PATH
    rest_base: str                  = REST_BASE_URL
    ws_base: str                    = WS_BASE_URL
    shard_routing: dict             = field(default_factory=lambda: dict(SHARD_ROUTING))

    starting_balance: Decimal       = STARTING_BALANCE_USD
    kelly_fraction: float           = KELLY_FRACTION
    max_single_trade_pct: float     = MAX_SINGLE_TRADE_PCT
    max_single_trade_usd: Decimal   = MAX_SINGLE_TRADE_USD
    max_exposure_pct: float         = MAX_TOTAL_EXPOSURE_PCT
    max_premium_exposure_pct: float = MAX_PREMIUM_EXPOSURE_PCT
    premium_arb_min_edge: float     = PREMIUM_ARB_MIN_EDGE
    premium_arb_min_epd: float      = PREMIUM_ARB_MIN_EDGE_PER_DAY
    premium_whale_notional: float   = PREMIUM_WHALE_NOTIONAL_USD
    fast_fill_threshold_sec: float  = FAST_FILL_THRESHOLD_SEC
    fast_fill_min_edge: float       = FAST_FILL_MIN_EDGE
    fill_speed_window: int          = FILL_SPEED_WINDOW
    min_kelly_edge: float           = MIN_KELLY_EDGE
    max_daily_loss_usd: Decimal     = MAX_DAILY_LOSS_USD
    max_consecutive_losses: int     = MAX_CONSECUTIVE_LOSSES

    whale_min_notional: Decimal       = WHALE_MIN_NOTIONAL_USD
    whale_track_notional: Decimal     = WHALE_TRACK_NOTIONAL_USD
    min_whale_sample_size: int        = WHALE_MIN_SAMPLE_SIZE

    tick_size: Decimal              = TICK_SIZE
    maker_improve_ticks: int        = MAKER_IMPROVE_TICKS
    post_only: bool                 = POST_ONLY
    stale_order_ttl: int            = STALE_ORDER_TTL_SEC

    token_bucket_capacity: int      = TOKEN_BUCKET_CAPACITY
    token_refill_rate: float        = TOKEN_REFILL_RATE
    token_costs: dict               = field(default_factory=lambda: dict(TOKEN_COSTS))

    token_refresh_sec: int          = TOKEN_REFRESH_INTERVAL_SEC
    http_timeout: float             = HTTP_TIMEOUT_SEC

    ws_ping_interval: int           = WS_PING_INTERVAL_SEC
    ws_reconnect_base: float        = WS_RECONNECT_BASE_SEC
    ws_reconnect_max: float         = WS_RECONNECT_MAX_SEC

    market_cache_ttl: int           = MARKET_CACHE_TTL_SEC
    balance_cache_ttl: int          = BALANCE_CACHE_TTL_SEC
    settlement_cache_ttl: int       = SETTLEMENT_CACHE_TTL_SEC

    decay_interval_seconds: int             = DECAY_INTERVAL_SECONDS
    balance_sync_interval_seconds: int      = BALANCE_SYNC_INTERVAL_SECONDS
    settlement_check_interval_seconds: int  = SETTLEMENT_CHECK_INTERVAL_SECONDS

    convergence_boost: float        = WHALE_CONVERGENCE_BOOST
    decay_winrate: float            = WHALE_DECAY_WINRATE
    decay_multiplier: float         = WHALE_DECAY_MULTIPLIER
    whale_lookback_days: int        = WHALE_LOOKBACK_DAYS
    whale_min_resolved: int         = WHALE_MIN_RESOLVED_TRADES

    dry_run: bool                     = DRY_RUN
    dry_run_fill_delay_sec: int       = DRY_RUN_FILL_DELAY_SEC
    state_file: str                   = STATE_FILE

    watchlist_file: str               = WATCHLIST_FILE
    watchlist_min_resolved_promote: int   = WATCHLIST_MIN_RESOLVED_PROMOTE
    watchlist_min_winrate_promote:  float = WATCHLIST_MIN_WINRATE_PROMOTE
    watchlist_max_winrate_demote:   float = WATCHLIST_MAX_WINRATE_DEMOTE
    watchlist_min_resolved_demote:  int   = WATCHLIST_MIN_RESOLVED_DEMOTE
    watchlist_proven_boost:         float = WATCHLIST_PROVEN_BOOST
    watchlist_demoted_weight:       float = WATCHLIST_DEMOTED_WEIGHT
    whale_eviction_days: int          = WHALE_EVICTION_DAYS

    enable_aggressive_execution: bool = ENABLE_AGGRESSIVE_EXECUTION
    max_chase_cents: int              = MAX_CHASE_CENTS

    # Signal engine
    min_whale_confidence:  float = MIN_WHALE_CONFIDENCE
    min_category_trades:   int   = MIN_CATEGORY_TRADES
    min_category_win_rate: float = MIN_CATEGORY_WIN_RATE
    min_mispricing:        float = MIN_MISPRICING
    min_composite_conf:    float = MIN_COMPOSITE_CONF
    fundamental_weight:    float = FUNDAMENTAL_WEIGHT
    market_weight:         float = MARKET_WEIGHT
    agreement_bonus:       float = AGREEMENT_BONUS
    max_spread_to_trade:   float = MAX_SPREAD_TO_TRADE
    min_market_price:      float = MIN_MARKET_PRICE
    max_market_price:      float = MAX_MARKET_PRICE
    auto_select_markets: bool         = AUTO_SELECT_MARKETS
    auto_market_count: int            = AUTO_MARKET_COUNT
    auto_market_refresh_seconds: int  = AUTO_MARKET_REFRESH_SECONDS
    auto_market_min_volume: float     = AUTO_MARKET_MIN_VOLUME

    log_level: str                  = LOG_LEVEL
    log_file: str                   = LOG_FILE

    # Sum-to-1 arb (off by default — enable after validating MVE market structure)
    enable_sum_arb: bool            = ENABLE_SUM_ARB
