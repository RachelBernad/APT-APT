"""Central, environment-driven configuration for the apartment bot.

Every setting is overridable via environment variables so the same image can be
deployed with different behaviour.
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

# Bundled Yad2 location catalog (harvested nationwide sweep + per-city hood lists).
DATA_DIR = Path(os.getenv(
    "DATA_DIR", str(Path(__file__).resolve().parent / "scrapers" / "yad2" / "data")))
IL_LOCATIONS_FILE = DATA_DIR / "il_locations.json"
HOODS_DIR = DATA_DIR / "hoods"

# --- Telegram ---
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "")


def _id_list(raw: str) -> list:
    return [p.strip() for p in raw.split(",") if p.strip()]


# Only these chats may talk to the bot or receive anything from it (comma-separated
# Telegram chat ids; group chats are negative). Set it in .env, never in the repo.
# Empty = nobody (the bot logs every refused chat id, so you can find yours).
ALLOWED_CHAT_IDS = frozenset(int(x) for x in _id_list(os.getenv("ALLOWED_CHAT_IDS", "")))

# Only these PEOPLE (Telegram user ids, positive) may give commands, even inside an
# allowed group: a new group member, an anonymous admin or a linked channel is refused.
# Empty = any member of an allowed chat (logged as a warning at startup). The gate logs
# every refused user id once, so you can find yours.
ALLOWED_USER_IDS = frozenset(int(x) for x in _id_list(os.getenv("ALLOWED_USER_IDS", "")))

# --- Facebook groups (aptbot/scrapers/facebook_generic_browser) ---
FB_ENABLED = os.getenv("FB_ENABLED", "1").strip().lower() not in ("0", "false", "no", "")
# Groups to watch (the account must be a MEMBER). Default: the Tel Aviv rental groups.
FB_GROUP_IDS = _id_list(os.getenv("FB_GROUP_IDS", ",".join([
    "157839564799734",   # גינדי תל אביב דירות להשכרה ומכירה
    "474384046336013",   # דירות להשכרה לב תל אביב והצפון הישן
    "240748264581079",   # תל אביב דירות למכירה/השכרה
    "457465901082882",   # דירות בתל אביב ללא תיווך
    "101875683484689",   # דירות מפה לאוזן בתל אביב
    "305724686290054",   # דירות להשכרה בתל אביב
])))
FB_MAX_TABS = int(os.getenv("FB_MAX_TABS", "2"))
# Per-request limit (seconds) for one Facebook job, e.g. one group feed. Slow hosts (a Pi 3) need ~600.
FB_REQUEST_TIMEOUT = float(os.getenv("FB_REQUEST_TIMEOUT", "240"))
# Browser engine for Facebook: "chrome" (Patchright + Chromium under Xvfb) or "oxide" (BrowserOxide worker
# subprocess, ~450-560 MB; aptbot/browsers/oxide/kb/aptbot-integration.md).
FB_BROWSER = os.getenv("FB_BROWSER", "chrome").strip().lower()
# oxide's cookie jar (the logged-in session; chmod 600, in the fbgb-home volume). Empty = <FBGB_HOME>/oxide_jar.json.
FB_OXIDE_JAR = os.getenv("FB_OXIDE_JAR", "").strip() or None
# Poll every 8-12 min, jittered; a cycle passes over every watched group and diffs with the DB.
FB_POLL_MIN_SECONDS = int(os.getenv("FB_POLL_MIN_SECONDS", str(8 * 60)))
FB_POLL_MAX_SECONDS = int(os.getenv("FB_POLL_MAX_SECONDS", str(12 * 60)))
# No polling between these local (Israel) hours, "HH-HH"; empty = poll around the clock.
FB_QUIET_HOURS = os.getenv("FB_QUIET_HOURS", "01-07").strip()
# Newest posts sent as a taster when a monitor starts; the rest are recorded as seen.
FB_BACKFILL_CAP = int(os.getenv("FB_BACKFILL_CAP", "5"))
# Photos per post album (Telegram allows 2-10 per media group).
FB_MAX_PHOTOS = int(os.getenv("FB_MAX_PHOTOS", "10"))

# --- AI analysis of Facebook posts (aptbot/listing_ai, ChatGPT-subscription OAuth) ---
# Off by default: needs langchain-openai and a one-time `python -m aptbot.listing_ai login`.
# The token store lives OUTSIDE the repo (AI_AUTH_PATH, default ~/.aptbot/chatgpt-auth.json).
AI_ENABLED = os.getenv("AI_ENABLED", "0").strip().lower() not in ("0", "false", "no", "")
AI_MODEL = os.getenv("AI_MODEL", "gpt-6-luna")
AI_EFFORT = os.getenv("AI_EFFORT", "low")            # reasoning effort
AI_BATCH = int(os.getenv("AI_BATCH", "15"))          # posts per model call
AI_CONCURRENCY = int(os.getenv("AI_CONCURRENCY", "3"))
AI_TIMEOUT = float(os.getenv("AI_TIMEOUT", "120"))   # seconds per batch (15 posts take ~45 s)

# Which AI Post kinds reach the chat (comma-separated; empty = every kind):
# rent, roommate, sublet, sale, seeking, not_listing. Posts the AI couldn't analyze always go out.
FB_POST_KINDS = frozenset(k.lower() for k in _id_list(os.getenv("FB_POST_KINDS", "rent")))

# --- Scrape cycle ---
CHECK_INTERVAL_SECONDS = int(os.getenv("CHECK_INTERVAL_SECONDS", str(60 * 15)))  # default 15 min

# --- Database ---
DB_PATH = os.getenv("DB_PATH", str(OUTPUT_DIR / "bot.db"))

# When a monitor is first primed (new search, or after edit/unmute/re-enable),
# send up to this many *current* matches (newest first) as a backfill, then only
# notify about genuinely-new listings afterward.
BACKFILL_CAP = int(os.getenv("BACKFILL_CAP", "10"))


# --- Locations ---
# Tel Aviv-Yafo: the default city whose hoods harvest_locations bundles. Yad2 city
# ids are 4-char, zero-padded strings
# (e.g. "0070" = Ashdod), so this is kept as a string.
TEL_AVIV_CITY_ID = os.getenv("TEL_AVIV_CITY_ID", "5000")
# Yad2 neighborhoods the /monitor watches (exact names from data/hoods/5000.json, ';'-
# separated in the env because some names contain commas). Empty = the whole city.
# Default: לב העיר (the whole center quarter), הצפון הישן, נווה צדק.
YAD2_HOODS = [h.strip() for h in os.getenv("YAD2_HOODS", ";".join([
    "לב תל אביב, לב העיר צפון",
    "כרם התימנים",
    "מונטיפיורי, הרכבת",
    "גני שרונה, קרית הממשלה",
    "מכללת תל אביב יפו, דקר",
    "הצפון הישן - החלק הצפוני",
    "הצפון הישן - החלק הדרומי",
    "נווה צדק",
])).split(";") if h.strip()]

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
# Unfiltered Tel Aviv needed 31 requests for ~2,390 listings (2026-11-24); 16
# silently dropped almost half of them.
MAP_MAX_REQUESTS = int(os.getenv("MAP_MAX_REQUESTS", "40"))
# How many location groups to scrape concurrently per cycle (bounds parallel load
# on the gateway while cutting wall-clock time vs. sequential scraping).
SCRAPE_CONCURRENCY = int(os.getenv("SCRAPE_CONCURRENCY", "5"))

# --- Tel Aviv-Yafo municipal GIS (aptbot/scrapers/tel_aviv_gis) ---
TLV_GIS_MAPSERVER = os.getenv(
    "TLV_GIS_MAPSERVER",
    "https://gisn.tel-aviv.gov.il/arcgis/rest/services/WM/IView2WM/MapServer")
# Service-wide maxRecordCount is 2000; page size can't exceed it.
TLV_GIS_PAGE_SIZE = int(os.getenv("TLV_GIS_PAGE_SIZE", "2000"))
# Parallel in-flight requests to the GIS per client (no rate limit observed; stay polite).
TLV_GIS_CONCURRENCY = int(os.getenv("TLV_GIS_CONCURRENCY", "4"))
TLV_GIS_TIMEOUT = int(os.getenv("TLV_GIS_TIMEOUT", "60"))
TLV_GIS_RETRIES = int(os.getenv("TLV_GIS_RETRIES", "3"))
# Safety cap on pages per query() so a careless where=1=1 can't page forever.
TLV_GIS_MAX_PAGES = int(os.getenv("TLV_GIS_MAX_PAGES", "50"))

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

    # Quiet down noisy libraries. apscheduler logs an "Added job"/"Removed job"
    # pair for every conversation-timeout reschedule — i.e. two lines per button
    # tap — which would drown the activity log it sits next to.
    logging.getLogger("telegram").setLevel(logging.WARNING)
    logging.getLogger("httpx").setLevel(logging.WARNING)
    logging.getLogger("aiosqlite").setLevel(logging.WARNING)
    logging.getLogger("apscheduler").setLevel(logging.WARNING)

    return logging.getLogger(__name__)


logger = logging.getLogger(__name__)
