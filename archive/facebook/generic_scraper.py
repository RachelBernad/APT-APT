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
# RentlyFly/Facebook Groups is disabled for the Krayot search because it does
# not currently return listings for all requested Krayot locations.
# import facebook_groups_scraper
import yad2
from shared_scrapers_config import (DEFAULT_MAX_PRICE, DEFAULT_MAX_ROOMS,
                                    DEFAULT_MIN_ROOMS,
                                    DEFAULT_STRUCTURED_LOCATIONS,
                                    DEFAULT_TARGET_CITIES,
                                    OUTPUT_DIR)
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
        'max_price': DEFAULT_MAX_PRICE,
        'min_rooms': DEFAULT_MIN_ROOMS,
        'max_rooms': DEFAULT_MAX_ROOMS,
        'require_mamad': True,
        'require_elevator': True,
        'min_floor': None,
        'max_floor': None,
        'min_squaremeter': 65,
    },
    # 'facebook_groups': {
    #     'scraper_class': facebook_groups_scraper.FacebookGroupsScraper,
    #     'type_name': 'facebook groups',
    #     'logger': logging.getLogger(facebook_groups_scraper.__name__),
    #     # Common filter parameters
    #     'min_price': 3,
    #     'max_price': DEFAULT_MAX_PRICE,
    #     'min_rooms': DEFAULT_MIN_ROOMS,
    #     'max_rooms': DEFAULT_MAX_ROOMS,
    #     'require_mamad': None,
    #     'require_elevator': None,
    #     'min_floor': None,
    #     'max_floor': None,
    #     'is_shared_apartment': False,
    #     'is_sublet': False,
    #     # Max number of items to fetch per request (shouldn't really be changed...)
    #     'limit': 50,
    #     'target_cities': DEFAULT_TARGET_CITIES,
    #     'structured_locations': DEFAULT_STRUCTURED_LOCATIONS,
    # }
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

    # --- Run Registered Scrapers Sequentially to reduce anti-bot pressure ---
    scrapers_to_run = []
    for name, config in SCRAPER_REGISTRY.items():
        # Pass filter parameters to the scraper instance
        if name == 'yad2':
            scraper_instance = config['scraper_class'](
                min_price=config.get('min_price', None),
                max_price=config.get('max_price', None),
                min_rooms=config.get('min_rooms', None),
                max_rooms=config.get('max_rooms', None),
                require_mamad=config.get('require_mamad', None),
                require_elevator=config.get('require_elevator', None),
                min_floor=config.get('min_floor', None),
                max_floor=config.get('max_floor', None),
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
                target_cities=config.get('target_cities', None),
                structured_locations=config.get('structured_locations', None),
                require_mamad=config.get('require_mamad', None),
                require_elevator=config.get('require_elevator', None),
                min_floor=config.get('min_floor', None),
                max_floor=config.get('max_floor', None)
            )
        else:
            raise ValueError(f"Unknown scraper name: {name}")

        # Store the instance and its config for later use
        scrapers_to_run.append({
            'name': name,
            'instance': scraper_instance,
            'config': config
        })

    # Combine results with scraper names
    all_scraped_apartments = []
    seen_apartments = set()
    scraper_stats = {}  # To store counts per scraper
    for config_info in scrapers_to_run:
        name = config_info['name']
        config = config_info['config']
        scraper_type = config['type_name']
        logger = config['logger']

        try:
            scraper_results = await config_info['instance'].run()
        except Exception as scraper_error:
            logger.error(
                f"Scraper '{name}' failed: {scraper_error}"
            )
            scraper_stats[name] = {'scraped': 0, 'failed': True}
            continue

        logger.info(f"Scraper '{name}' returned {len(scraper_results)} items.")
        scraper_stats[name] = {'scraped': len(scraper_results)}

        for apt in scraper_results:
            # Ensure 'type' field is set correctly
            apt['type'] = scraper_type
            # Calculate MD5 if not present (should be done by scraper, but just in case)
            if 'md5' not in apt:
                logger.warning(f"MD5 not found for apartment: {apt}")
                apt['md5'] = _get_md5(apt)

            apartment_key = (apt.get('type'), apt.get('id'))
            if apartment_key in seen_apartments:
                continue
            seen_apartments.add(apartment_key)
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
