# config.py
"""Central, environment-driven configuration for the apartment bot.

Replaces the old ``shared_scrapers_config.py``. Everything that used to be a
hard-coded module constant is now overridable via environment variables so the
same image can be deployed with different behaviour.
"""
import logging
import os
from pathlib import Path

# --- Paths & logging ---
OUTPUT_DIR = Path(os.getenv("OUTPUT_DIR", "out"))
OUTPUT_DIR.mkdir(exist_ok=True)
LOG_FILE = str(OUTPUT_DIR / "bot.log")
LOG_LEVEL = getattr(logging, os.getenv("LOG_LEVEL", "INFO").upper(), logging.INFO)
# Rotating log in the shared ./out volume: cap each file and keep N backups so it
# never grows without bound (bot.log, bot.log.1, … bot.log.N).
LOG_MAX_BYTES = int(os.getenv("LOG_MAX_BYTES", str(2 * 1024 * 1024)))  # 2 MB / file
LOG_BACKUP_COUNT = int(os.getenv("LOG_BACKUP_COUNT", "5"))             # → ~12 MB max

# Bundled location catalog (harvested nationwide sweep + per-city hood lists).
DATA_DIR = Path(os.getenv("DATA_DIR", str(Path(__file__).resolve().parent / "data")))
IL_LOCATIONS_FILE = DATA_DIR / "il_locations.json"
HOODS_DIR = DATA_DIR / "hoods"

# --- Telegram ---
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "")

# --- Scrape cycle ---
CHECK_INTERVAL_SECONDS = int(os.getenv("CHECK_INTERVAL_SECONDS", str(60 * 15)))  # default 15 min

# --- Database ---
DB_PATH = os.getenv("DB_PATH", str(OUTPUT_DIR / "bot.db"))

# When a monitor is first primed (new search, or after edit/unmute/re-enable),
# send up to this many *current* matches (newest first) as a backfill, then only
# notify about genuinely-new listings afterward.
BACKFILL_CAP = int(os.getenv("BACKFILL_CAP", "10"))

# On-demand daily HTML report: minimum hours between reports per user (anti-spam).
REPORT_COOLDOWN_HOURS = int(os.getenv("REPORT_COOLDOWN_HOURS", "24"))

# --- Sources ---
ENABLE_RENTLYFLY = os.getenv("ENABLE_RENTLYFLY", "1") == "1"
# Tel Aviv-Yafo. rentlyfly.ai only covers this city, so it is only layered on
# top of a Yad2 search whose city matches this id.
TEL_AVIV_CITY_ID = int(os.getenv("TEL_AVIV_CITY_ID", "5000"))
RENTLYFLY_API_URL = os.getenv("RENTLYFLY_API_URL", "https://rentlyfly.ai/api/listings")
# rentlyfly returns newest-first; only fetch the most recent pages each cycle
# (the whole dataset is thousands of listings — we only need what is new).
RENTLYFLY_PAGE_LIMIT = int(os.getenv("RENTLYFLY_PAGE_LIMIT", "50"))
RENTLYFLY_MAX_PAGES = int(os.getenv("RENTLYFLY_MAX_PAGES", "4"))

# --- Yad2 gateway ---
YAD2_GATEWAY = os.getenv("YAD2_GATEWAY", "https://gw.yad2.co.il")
YAD2_MAP_URL = f"{YAD2_GATEWAY}/realestate-feed/rent/map"
YAD2_AUTOCOMPLETE_URL = f"{YAD2_GATEWAY}/address-autocomplete/realestate/v2"
# The /map endpoint returns at most this many markers with no pagination.
MAP_MARKER_CAP = int(os.getenv("MAP_MARKER_CAP", "200"))
# When bisecting the price interval to defeat the cap, stop once the band is
# this narrow (avoids infinite recursion on dense price points).
MAP_MIN_PRICE_BAND = int(os.getenv("MAP_MIN_PRICE_BAND", "250"))
# Hard cap on gateway requests per location scrape, so an unfiltered whole-city
# search can't fire hundreds of requests each cycle (bounds load / bot-detection).
MAP_MAX_REQUESTS = int(os.getenv("MAP_MAX_REQUESTS", "16"))
# How many location groups to scrape concurrently per cycle (bounds parallel load
# on the gateway while cutting wall-clock time vs. sequential scraping).
SCRAPE_CONCURRENCY = int(os.getenv("SCRAPE_CONCURRENCY", "5"))

# --- HTTP ---
CONNECT_TIMEOUT = int(os.getenv("CONNECT_TIMEOUT", "20"))
SOCK_READ_TIMEOUT = int(os.getenv("SOCK_READ_TIMEOUT", "120"))
REQUEST_TIMEOUT = int(os.getenv("REQUEST_TIMEOUT", "30"))
TOTAL_TIMEOUT = CONNECT_TIMEOUT + SOCK_READ_TIMEOUT + 100

MIN_DELAY_BETWEEN_REQUESTS = float(os.getenv("MIN_DELAY_BETWEEN_REQUESTS", "1.0"))
MAX_DELAY_BETWEEN_REQUESTS = float(os.getenv("MAX_DELAY_BETWEEN_REQUESTS", "3.0"))

# --- Message pacing (Telegram anti-flood) ---
MIN_MESSAGE_DELAY_SECONDS = float(os.getenv("MIN_MESSAGE_DELAY_SECONDS", "0.3"))
MAX_MESSAGE_DELAY_SECONDS = float(os.getenv("MAX_MESSAGE_DELAY_SECONDS", "1.0"))
MAX_DESCRIPTION_CHARS = int(os.getenv("MAX_DESCRIPTION_CHARS", "700"))

# --- Default filter seeds for the add-search wizard ---
# ``None`` means "no constraint". These are just the values a brand-new search
# card starts with; the user edits them before saving.
DEFAULT_MIN_PRICE = None
DEFAULT_MAX_PRICE = None
DEFAULT_MIN_ROOMS = None
DEFAULT_MAX_ROOMS = None
DEFAULT_MIN_SQM = None
DEFAULT_MIN_FLOOR = None
DEFAULT_MAX_FLOOR = None
DEFAULT_PROPERTY_TYPES = None       # None = all property types
DEFAULT_PROPERTY_CONDITION = None
DEFAULT_FEATURES: list = []         # no required features by default

USER_AGENT = os.getenv(
    "USER_AGENT",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/141.0.0.0 Safari/537.36",
)


def setup_logging() -> logging.Logger:
    """Configure the root logger to write to a rotating ``bot.log`` (in the shared
    ./out volume) plus stdout."""
    from logging.handlers import RotatingFileHandler

    file_handler = RotatingFileHandler(
        LOG_FILE, maxBytes=LOG_MAX_BYTES, backupCount=LOG_BACKUP_COUNT, encoding="utf-8",
    )
    console_handler = logging.StreamHandler()

    formatter = logging.Formatter(
        "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    )
    file_handler.setFormatter(formatter)
    console_handler.setFormatter(formatter)

    root_logger = logging.getLogger()
    root_logger.setLevel(LOG_LEVEL)
    for handler in root_logger.handlers[:]:
        root_logger.removeHandler(handler)
    root_logger.addHandler(file_handler)
    root_logger.addHandler(console_handler)

    # Quiet down noisy libraries.
    logging.getLogger("telegram").setLevel(logging.WARNING)
    logging.getLogger("httpx").setLevel(logging.WARNING)
    logging.getLogger("aiosqlite").setLevel(logging.WARNING)

    return logging.getLogger(__name__)


logger = logging.getLogger(__name__)
