"""
main.py — Entry Point

Usage
─────
    # Demo environment (always start here)
    KALSHI_ENV=demo python main.py   # credentials come from .env

    # Production
    KALSHI_ENV=prod \
    KALSHI_TICKERS=KXBTCD-25DEC-T100000,KXINX-25DEC-T5000 python main.py

Environment Variables
─────────────────────
  KALSHI_EMAIL        account email
  KALSHI_PASSWORD     account password
  KALSHI_ENV          "demo" | "prod"  (default: demo for safety)
  KALSHI_TICKERS      comma-separated ticker list
  KALSHI_LOG_LEVEL    DEBUG | INFO | WARNING  (default: INFO)
"""
from __future__ import annotations
from dotenv import load_dotenv
load_dotenv()

import asyncio
import logging
import os
import signal
import sys

from config import BotConfig
from bot_engine import BotEngine

def build_config() -> BotConfig:
    cfg = BotConfig()
    
    # Map your .env variables straight into your updated config parameters
    if v := os.getenv("KALSHI_API_KEY"):
        cfg.api_key_id = v
    if v := os.getenv("KALSHI_PRIVATE_KEY_PATH"):
        cfg.private_key_path = v
        
    if v := os.getenv("KALSHI_LOG_LEVEL"):
        cfg.log_level = v.upper()

    env = os.getenv("KALSHI_ENV", "demo").lower()
    if env == "prod":
        cfg.rest_base = "https://api.elections.kalshi.com/trade-api/v2"
        cfg.ws_base   = "wss://api.elections.kalshi.com/trade-api/ws/v2"
    else:
        cfg.rest_base = "https://demo-api.kalshi.co/trade-api/v2"
        cfg.ws_base   = "wss://demo-api.kalshi.co/trade-api/ws/v2"
        # CRITICAL: in demo mode, ALL shard routes must point to the demo endpoint.
        # Production shard URLs will return 401 when using a demo API key.
        demo_base = cfg.rest_base
        cfg.shard_routing = {k: demo_base for k in cfg.shard_routing}
        logging.getLogger("main").warning("Running in DEMO mode — all shard routes overridden to demo endpoint.")

    return cfg


def get_tickers() -> list[str]:
    env = os.getenv("KALSHI_TICKERS", "")
    if env:
        return [t.strip() for t in env.split(",") if t.strip()]
    # Defaults for development — replace before going live
    return [
        "KXBTCD-25DEC-T100000",
        "KXINX-25DEC-T5000",
        "KXFED-25DEC-425",
    ]


class ColoredFormatter(logging.Formatter):
    """
    Terminal-only formatter that highlights key bot events:

      GREEN  — whale detection   (ws_feed logs "WHALE: …")
      BLUE   — trade execution   (COMPOSITE SIGNAL, ORDER →, Order live, FILLED)
      YELLOW — warnings
      RED    — errors / critical

    Plain text fallback when the terminal does not support ANSI (e.g. when
    stdout is redirected to a file — use the plain FileHandler for that).
    """

    # ANSI escape codes
    _GREEN  = "\033[92m"
    _BLUE   = "\033[94m"
    _YELLOW = "\033[93m"
    _RED    = "\033[91m"
    _RESET  = "\033[0m"

    # Message substrings that trigger each colour (checked against the full
    # formatted line so logger-name and level are also coloured).
    _GREEN_NEEDLES  = ("WHALE:",)
    _BLUE_NEEDLES   = (
        "COMPOSITE SIGNAL:",
        "ORDER →",
        "Order live:",
        "Taker order live:",
        "FILLED:",
        "[DRY-RUN] Simulated order:",
        "[DRY-RUN] Simulated taker order:",
    )

    def format(self, record: logging.LogRecord) -> str:
        line = super().format(record)

        # Determine colour based on content first, then level.
        colour = None
        if any(n in line for n in self._GREEN_NEEDLES):
            colour = self._GREEN
        elif any(n in line for n in self._BLUE_NEEDLES):
            colour = self._BLUE
        elif record.levelno >= logging.ERROR:
            colour = self._RED
        elif record.levelno >= logging.WARNING:
            colour = self._YELLOW

        if colour:
            return f"{colour}{line}{self._RESET}"
        return line


def setup_logging(cfg: BotConfig) -> None:
    fmt = "%(asctime)s %(levelname)-8s [%(name)s] %(message)s"

    # Terminal handler — coloured output
    stream_handler = logging.StreamHandler(sys.stdout)
    stream_handler.setFormatter(ColoredFormatter(fmt))
    handlers: list[logging.Handler] = [stream_handler]

    # File handler — plain text (no ANSI codes cluttering log files)
    if cfg.log_file:
        file_handler = logging.FileHandler(cfg.log_file)
        file_handler.setFormatter(logging.Formatter(fmt))
        handlers.append(file_handler)

    logging.basicConfig(
        level    = getattr(logging, cfg.log_level, logging.INFO),
        handlers = handlers,
        force    = True,
    )
    for noisy in ("aiohttp", "asyncio", "urllib3"):
        logging.getLogger(noisy).setLevel(logging.WARNING)


async def async_main() -> None:
    cfg     = build_config()
    setup_logging(cfg)
    log     = logging.getLogger("main")
    tickers = get_tickers()

    engine  = BotEngine(cfg)
    loop    = asyncio.get_running_loop()

    # Guard: ignore SIGTERM during startup. The bot takes ~15s to complete
    # market discovery and WS connection. A stray SIGTERM from a previous
    # dying process or duplicate instance during this window would kill a
    # clean new start. Only honour signals once fully running.
    _startup_complete = False

    def _on_signal(signame: str) -> None:
        # SIGINT (Ctrl+C) always shuts down — user-initiated, always honour it.
        # SIGTERM during startup is ignored — it's likely from a dying previous
        # instance and should not kill a clean new start mid-discovery.
        if not _startup_complete and signame == "SIGTERM":
            log.warning(
                "SIGTERM received during startup — ignored "
                "(likely from dying previous instance, not user-initiated).",
            )
            return
        log.warning("Signal %s received — initiating graceful shutdown.", signame)
        asyncio.create_task(engine.stop())

    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, _on_signal, sig.name)
        except NotImplementedError:
            pass   # Windows

    try:
        await engine.start(tickers)
        _startup_complete = True
    except KeyboardInterrupt:
        log.info("Keyboard interrupt.")
    except Exception as exc:
        log.critical("Fatal: %s", exc, exc_info=True)
        sys.exit(1)


if __name__ == "__main__":
    asyncio.run(async_main())
