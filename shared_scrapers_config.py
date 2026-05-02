# shared_scrapers_config.py
import logging
from pathlib import Path
from typing import List

# --- Configuration ---
LOG_LEVEL = logging.INFO
OUTPUT_DIR = Path("out")
OUTPUT_DIR.mkdir(exist_ok=True)
LOG_FILE = str(OUTPUT_DIR / "bot.log")  # Centralized log file

# --- HTTP Configuration ---
CONNECT_TIMEOUT = 20
SOCK_READ_TIMEOUT = 120
REQUEST_TIMEOUT = 30
TOTAL_TIMEOUT = CONNECT_TIMEOUT + SOCK_READ_TIMEOUT + 100

# --- Delay Configuration ---
MIN_DELAY_BETWEEN_REQUESTS = 2.0
MAX_DELAY_BETWEEN_REQUESTS = 8.0

# --- Default Apartment Filters ---
DEFAULT_MAX_PRICE = 5800
DEFAULT_MIN_ROOMS = 3.5
DEFAULT_MAX_ROOMS = 4.0

# --- Known cities / areas ---
KNOWN_CITIES = {
    "תל אביב": {"city_id": 5000, "area_id": 1, "route_slug": "tel-aviv-area"},
    "חיפה": {"city_id": 4000, "area_id": 5, "route_slug": "coastal-north"},
    "קריית ים": {"city_id": 9600, "area_id": 6, "route_slug": "coastal-north"},
    "קריית מוצקין": {"city_id": 8200, "area_id": 6, "route_slug": "coastal-north"},
    "קריית ביאליק": {"city_id": 9500, "area_id": 6, "route_slug": "coastal-north"},
}

KNOWN_HOODS = {
    "קריית חיים מערבית": {"city_id": 4000, "area_id": 5, "neighborhood_id": 648, "route_slug": "coastal-north"},
    "קריית חיים מזרחית": {"city_id": 4000, "area_id": 5, "neighborhood_id": 650, "route_slug": "coastal-north"},
}

# --- Default Yad2 / API location filters ---
DEFAULT_SEARCH_LOCATIONS = [
    "קריית ים",
    "קריית חיים מערבית",
    "קריית חיים מזרחית",
    "קריית מוצקין",
    "קריית ביאליק",
]

DEFAULT_YAD2_BBOX = "32.728229,34.863183,32.970433,35.320162"
DEFAULT_YAD2_ZOOM = 10


DEFAULT_YAD2_LOCATION_FILTERS = [
    {
        "name": location_name,
        "city": KNOWN_CITIES["קריית ביאליק"]["city_id"],
        "area": KNOWN_CITIES["קריית ביאליק"]["area_id"],
        "route_slug": KNOWN_CITIES["קריית ביאליק"]["route_slug"],
        "bBox": DEFAULT_YAD2_BBOX,
        "zoom": DEFAULT_YAD2_ZOOM,
        "match_field": "hood" if location_name in KNOWN_HOODS else "city",
    }
    for location_name in DEFAULT_SEARCH_LOCATIONS
]
DEFAULT_YAD2_CITY_IDS: List[int] = [city["city_id"] for city in KNOWN_CITIES.values()]
DEFAULT_TARGET_CITIES = ["קריית ים", "חיפה", "קריית מוצקין", "קריית ביאליק"]
DEFAULT_STRUCTURED_LOCATIONS = [
    {"city": "קריית ים"},
    {"city": "חיפה", "hood": "קריית חיים מערבית"},
    {"city": "חיפה", "hood": "קריית חיים מזרחית"},
    {"city": "קריית מוצקין"},
    {"city": "קריית ביאליק"},
]

def setup_logging():
    """Set up the root logger to log everything to bot.log."""
    # Create a file handler
    file_handler = logging.FileHandler(LOG_FILE)
    # Create a console handler
    console_handler = logging.StreamHandler()

    # Create a formatter
    formatter = logging.Formatter(
        '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    file_handler.setFormatter(formatter)
    console_handler.setFormatter(formatter)

    # Configure the root logger
    root_logger = logging.getLogger()
    root_logger.setLevel(LOG_LEVEL)
    
    # Remove any existing handlers to avoid duplicates if called multiple times
    for handler in root_logger.handlers[:]:
        root_logger.removeHandler(handler)
    
    # Add the new handlers
    root_logger.addHandler(file_handler)
    root_logger.addHandler(console_handler)

    # Return the specific logger for the bot module
    return logging.getLogger(__name__)

# Placeholder logger, will be replaced by setup_logging in telegram_bot.py
logger = logging.getLogger(__name__)
