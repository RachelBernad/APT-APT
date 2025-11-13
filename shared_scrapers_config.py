# shared_scrapers_config.py
import logging
from pathlib import Path

# --- Configuration ---
LOG_FILE = "bot.log"  # Centralized log file
LOG_LEVEL = logging.INFO
OUTPUT_DIR = Path("out")
OUTPUT_DIR.mkdir(exist_ok=True)

# --- HTTP Configuration ---
CONNECT_TIMEOUT = 20
SOCK_READ_TIMEOUT = 120
REQUEST_TIMEOUT = 30
TOTAL_TIMEOUT = CONNECT_TIMEOUT + SOCK_READ_TIMEOUT + 100

# --- Delay Configuration ---
MIN_DELAY_BETWEEN_REQUESTS = 2.0
MAX_DELAY_BETWEEN_REQUESTS = 8.0

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