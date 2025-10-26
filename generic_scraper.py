# generic_scraper.py
import asyncio
import datetime
import hashlib
import json
import logging
from pathlib import Path
from typing import Any, Dict, List

import facebook
import yad2
from shared_scrapers_config import OUTPUT_DIR, ScraperLogFormatter
from shared_scrapers_config import logger as shared_logger

# --- Configure Generic Scraper-specific logger with prefix ---
generic_logger = logging.getLogger(__name__)
# Apply the custom formatter with scraper name
# Example handler, apply to all handlers if needed
handler = logging.StreamHandler()
handler.setFormatter(ScraperLogFormatter(
    '%(asctime)s - %(levelname)s - %(message)s', 'GENERIC'))
generic_logger.addHandler(handler)
# Set level for Generic logger (inherits from root if not set)
# Keep at DEBUG for detailed merging logs
generic_logger.setLevel(logging.DEBUG)

# --- Configuration ---
MERGED_OUTPUT_FILE = OUTPUT_DIR / 'merged_apartments.json'

# --- Scraper Registry (For easy extension) ---
# Define a type alias for scraper functions
ScraperFunction = callable  # This is a placeholder, use proper typing if needed
SCRAPER_REGISTRY: Dict[str, Dict[str, Any]] = {
    'yad2': {
        'scraper_class': yad2.ApartmentScraper,
        'type_name': 'yad2',
        'logger': logging.getLogger(yad2.__name__),  # Use the module's logger
    },
    'facebook': {
        'scraper_class': facebook.FacebookMarketplaceScraper,
        'type_name': 'facebook marketplace',  # Updated type name
        # Use the module's logger
        'logger': logging.getLogger(facebook.__name__),
    }
    # Add more scrapers here as needed
    # 'new_scraper_name': {
    #     'scraper_class': new_scraper_module.NewScraperClass,
    #     'type_name': 'new_scraper_type',
    #     'logger': logging.getLogger(new_scraper_module.__name__),
    # },
}


def _get_md5(thing: Any) -> str:
    """Calculate MD5 hash for an item. This duplicates the logic from yad2 for consistency."""
    return hashlib.md5(str(thing).encode()).hexdigest()


async def run_generic_scraper():
    generic_logger.info(
        "Starting Generic Scraper to merge data from registered scrapers...")

    # --- Run All Registered Scrapers Concurrently ---
    tasks = []
    scrapers_to_run = {}
    for name, config in SCRAPER_REGISTRY.items():
        scraper_instance = config['scraper_class']()
        # Store the instance and its config for later use
        scrapers_to_run[name] = {
            'instance': scraper_instance,
            'config': config
        }
        # Create the task to run the scraper
        task = asyncio.create_task(scraper_instance.run())
        tasks.append(task)

    # Gather results from all tasks
    results = await asyncio.gather(*tasks)

    # Combine results with scraper names
    all_apartments = []
    scraper_stats = {}  # To store counts per scraper
    for i, (name, config_info) in enumerate(scrapers_to_run.items()):
        scraper_results = results[i]
        config = config_info['config']
        scraper_type = config['type_name']
        logger = config['logger']

        logger.info(f"Scraper '{name}' returned {len(scraper_results)} items.")
        scraper_stats[name] = {'new': 0, 'updated': 0}

        for apt in scraper_results:
            # Ensure 'type' field is set correctly
            apt['type'] = scraper_type
            # Calculate MD5 if not present (should be done by scraper, but just in case)
            if 'md5' not in apt:
                apt['md5'] = _get_md5(apt)
        all_apartments.extend(scraper_results)

    # --- Merge Logic (Similar to Yad2 but across types) ---
    old_data = {}
    if MERGED_OUTPUT_FILE.exists():
        with MERGED_OUTPUT_FILE.open('r', encoding='utf-8') as f:
            old_data = json.load(f)
        generic_logger.info(
            f"Loaded {len(old_data)} old items from {MERGED_OUTPUT_FILE}")
    else:
        generic_logger.info(
            f"No existing merged data file found at {MERGED_OUTPUT_FILE}, starting fresh.")

    old_by_md5 = {item['md5']: item for item in old_data.values()}

    # Process current items (from all scrapers)
    new_items = {}
    updated_items = {}

    for current_item in all_apartments:
        current_md5 = current_item['md5']
        current_id = current_item['id']
        # Get the type to find the associated logger/stats
        current_type = current_item['type']

        if current_md5 in old_by_md5:
            existing_item = old_by_md5[current_md5]
            if current_id != existing_item['id']:
                # Update the ID and URL in the existing item
                existing_item['id'] = current_id
                existing_item['apartment_page_url'] = current_item['apartment_page_url']
                updated_items[current_md5] = existing_item

                # Find the logger for the item's type to log the update
                for name, config in SCRAPER_REGISTRY.items():
                    if config['type_name'] == current_type:
                        config['logger'].debug(
                            f"Updated ID for item with MD5 {current_md5}")
                        scraper_stats[name]['updated'] += 1
                        break
        else:
            new_items[current_md5] = current_item

            # Find the logger for the item's type to log the new item
            for name, config in SCRAPER_REGISTRY.items():
                if config['type_name'] == current_type:
                    config['logger'].debug(
                        f"Found new item with MD5 {current_md5}")
                    scraper_stats[name]['new'] += 1
                    break

    # Reconstruct the final dictionary
    final_all_md5_based = {}
    for md5_key, stored_item in old_by_md5.items():
        if md5_key not in new_items and md5_key not in updated_items:
            final_all_md5_based[md5_key] = stored_item

    final_all_md5_based.update(new_items)
    final_all_md5_based.update(updated_items)

    # --- Save Merged Data ---
    with MERGED_OUTPUT_FILE.open('w', encoding='utf-8') as out:
        json.dump(final_all_md5_based, out, ensure_ascii=False, indent=2)
    generic_logger.info(
        f"Merged data saved to {MERGED_OUTPUT_FILE} with {len(final_all_md5_based)} items.")

    now = str(datetime.datetime.now()).split('.')[0]
    generic_logger.info(
        f'{now}: Generic Scraper - New: {len(new_items)} | Updated: {len(updated_items)}')

    # Print per-scraper stats
    for name, stats in scraper_stats.items():
        print(f'[{name.upper()}] New: {stats["new"]} | Updated: {stats["updated"]}')

    # Keep print for console feedback for the main summary
    print(f'{now}: Generic Scraper - New: {len(new_items)} | Updated: {len(updated_items)}')


async def main():
    await run_generic_scraper()


if __name__ == '__main__':
    asyncio.run(main())
