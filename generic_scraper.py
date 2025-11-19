# generic_scraper.py
import asyncio
import datetime
import hashlib
import json
import logging
from pathlib import Path
from typing import Any, Dict, List, Tuple

# Import the scrapers
import facebook
# Import the new Facebook Groups scraper
import facebook_groups_scraper
import yad2
from shared_scrapers_config import OUTPUT_DIR
from shared_scrapers_config import logger as shared_logger

# --- Configuration ---
# Note: MERGED_OUTPUT_FILE is no longer used by the generic scraper itself
# MERGED_OUTPUT_FILE = OUTPUT_DIR / 'merged_apartments.json'

# --- Scraper Registry (For easy extension and filtering) ---
ScraperFunction = callable
SCRAPER_REGISTRY: Dict[str, Dict[str, Any]] = {
    'yad2': {
        'scraper_class': yad2.ApartmentScraper,
        'type_name': 'yad2',
        'logger': logging.getLogger(yad2.__name__),
        # Common filter parameters
        'min_price': 3,
        'max_price': 10000,
        'min_rooms': 2.5,
        'min_squaremeter': 65,
    },
    'facebook_groups': {
        'scraper_class': facebook_groups_scraper.FacebookGroupsScraper,
        'type_name': 'facebook groups',
        'logger': logging.getLogger(facebook_groups_scraper.__name__),
        # Common filter parameters
        'min_price': 3,
        'max_price': 10000,
        'min_rooms': 3,  # the facebook api doesn't support float room counts
        'max_rooms': None,
        'is_shared_apartment': False,
        'is_sublet': False,
        # Max number of items to fetch per request (shouldn't really be changed...)
        'limit': 50,
    }
    # 'facebook': {
    #     'scraper_class': facebook.FacebookMarketplaceScraper,
    #     'type_name': 'facebook marketplace',
    #     'logger': logging.getLogger(facebook.__name__),
    #     # Common filter parameters
    #     'min_price': 3,
    #     'max_price': 10000,
    #     'min_bedrooms': 2.5,
    #     # Location-based parameters
    #     'lat': 32.08214,  # Tel Aviv
    #     # Can be calculated using this site: https://www.calcmaps.com/map-radius/
    #     'lng': 34.77842,
    #     'radius': 2,  # 2km
    # },
}


def _get_md5(thing: Any) -> str:
    """Calculate MD5 hash for an item. This duplicates the logic from yad2 for consistency."""
    return hashlib.md5(str(thing).encode()).hexdigest()


# Return the combined list of apartments from all scrapers
async def run_generic_scraper() -> List[Dict[str, Any]]:
    """
    Runs all registered scrapers concurrently and returns the combined list of apartments.
    Does NOT load, merge, or save data to merged_apartments.json.
    """
    shared_logger.info(
        "Starting Generic Scraper to fetch data from registered scrapers...")

    # --- Run All Registered Scrapers Concurrently ---
    tasks = []
    scrapers_to_run = {}
    for name, config in SCRAPER_REGISTRY.items():
        # Pass filter parameters to the scraper instance
        if name == 'yad2':
            scraper_instance = config['scraper_class'](
                min_price=config.get('min_price', None),
                max_price=config.get('max_price', None),
                min_rooms=config.get('min_rooms', None),
                max_rooms=config.get('max_rooms', None),
                min_squaremeter=config.get('min_squaremeter', None)
            )
        # elif name == 'facebook':
        #     scraper_instance = config['scraper_class'](
        #         min_price=config['min_price'],
        #         max_price=config['max_price'],
        #         min_bedrooms=config['min_bedrooms'],
        #         lat=config['lat'],
        #         lng=config['lng'],
        #         radius=config['radius']
        #     )
        elif name == 'facebook_groups':
            # Pass the parameters for the Facebook Groups scraper
            scraper_instance = config['scraper_class'](
                min_price=config['min_price'],
                max_price=config['max_price'],
                min_rooms=config['min_rooms'],
                max_rooms=config.get('max_rooms', None),
                is_shared_apartment=config['is_shared_apartment'],
                is_sublet=config['is_sublet'],
                limit=config['limit'],
                structured_locations=config.get('structured_locations', None)
            )
        else:
            raise ValueError(f"Unknown scraper name: {name}")

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
    all_scraped_apartments = []
    scraper_stats = {}  # To store counts per scraper
    for i, (name, config_info) in enumerate(scrapers_to_run.items()):
        scraper_results = results[i]
        config = config_info['config']
        scraper_type = config['type_name']
        logger = config['logger']

        logger.info(f"Scraper '{name}' returned {len(scraper_results)} items.")
        scraper_stats[name] = {'scraped': len(scraper_results)}

        for apt in scraper_results:
            # Ensure 'type' field is set correctly
            apt['type'] = scraper_type
            # Calculate MD5 if not present (should be done by scraper, but just in case)
            if 'md5' not in apt:
                logger.warning(f"MD5 not found for apartment: {apt}")
                apt['md5'] = _get_md5(apt)

            all_scraped_apartments.append(apt)

    now = str(datetime.datetime.now()).split('.')[0]

    # Calculate total scraped across all scrapers
    total_scraped = sum(stats.get('scraped', 0)
                        for stats in scraper_stats.values())

    shared_logger.info(
        f'{now}: Generic Scraper - Total Scraped: {total_scraped}')

    # Print per-scraper stats
    for name, stats in scraper_stats.items():
        shared_logger.debug(
            f'[{name.upper()}] Scraped: {stats["scraped"]}')

    # Return the combined list of all apartments fetched
    return all_scraped_apartments


async def main():
    all_apartments = await run_generic_scraper()
    print(
        f"Scraping completed. Fetched {len(all_apartments)} apartments from all sources.")


if __name__ == '__main__':
    asyncio.run(main())
