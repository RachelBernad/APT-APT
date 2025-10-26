# shared_scrapers_config.py
import logging
from pathlib import Path

# --- Shared Logging Configuration ---
LOG_FILE = "scraper.log"
LOG_LEVEL = logging.DEBUG

# Custom formatter to add scraper name prefix
class ScraperLogFormatter(logging.Formatter):
    def __init__(self, fmt, scraper_name):
        super().__init__(fmt)
        self.scraper_name = scraper_name

    def format(self, record):
        record.msg = f"[{self.scraper_name}] {record.msg}"
        return super().format(record)

# Set up the root logger
logging.basicConfig(
    level=LOG_LEVEL,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(LOG_FILE),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# --- Shared Directories ---
OUTPUT_DIR = Path("out")
OUTPUT_DIR.mkdir(exist_ok=True)

# --- Shared HTTP Configuration ---
CONNECT_TIMEOUT = 20
SOCK_READ_TIMEOUT = 60
REQUEST_TIMEOUT = 30
TOTAL_TIMEOUT = CONNECT_TIMEOUT + SOCK_READ_TIMEOUT + 100

# --- Shared Delay Configuration ---
MIN_DELAY_BETWEEN_REQUESTS = 2.0
MAX_DELAY_BETWEEN_REQUESTS = 5.0