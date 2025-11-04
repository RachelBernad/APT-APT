# yad2.py
import asyncio
import datetime
import hashlib
import json
import logging
import random
from pathlib import Path
from typing import Any, Dict, List

import aiohttp
from bs4 import BeautifulSoup

# Import shared configuration
from shared_scrapers_config import (LOG_FILE, LOG_LEVEL,
                                    MAX_DELAY_BETWEEN_REQUESTS,
                                    MIN_DELAY_BETWEEN_REQUESTS, OUTPUT_DIR,
                                    REQUEST_TIMEOUT, ScraperLogFormatter)
from shared_scrapers_config import logger as shared_logger

# --- Configuration ---
# URL Templates
BASE_URL_TEMPLATE = 'https://www.yad2.co.il/realestate/_next/data/{build_id}/rent.json?minPrice={min_price}&maxPrice={max_price}&minRooms={min_rooms}&maxRooms={max_rooms}&topArea=2&area=1&city={ct}&page={pg}'
RENT_PAGE_URL = 'https://www.yad2.co.il/realestate/rent?topArea=2&area=1&city=5000'

CITIES = [5000]  # Tel Aviv
APARTMENT_PAGE_URL_TEMPLATE = 'https://www.yad2.co.il/realestate/item/{token}'

USER_AGENT = 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/141.0.0.0 Safari/537.36'
DEFAULT_HEADERS = {
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
    'Accept-Encoding': 'gzip, deflate, br, zstd',
    'Accept-Language': 'en-US,en;q=0.9',
    'Cache-Control': 'no-cache',
    'Pragma': 'no-cache',
    'Priority': 'u=0, i',
    'Sec-Ch-Ua': '"Google Chrome";v="141", "Not?A_Brand";v="8", "Chromium";v="141"',
    'Sec-Ch-Ua-Mobile': '?0',
    'Sec-Ch-Ua-Platform': '"Windows"',
    'Sec-Fetch-Dest': 'document',
    'Sec-Fetch-Mode': 'navigate',
    'Sec-Fetch-Site': 'none',
    'Sec-Fetch-User': '?1',
    'Upgrade-Insecure-Requests': '1',
    'User-Agent': USER_AGENT,
}


class ApartmentScraper:
    def __init__(self, min_price=3, max_price=10000, min_rooms=2.5, max_rooms=4):
        self.build_id = None
        self.min_price = min_price
        self.max_price = max_price
        self.min_rooms = min_rooms
        self.max_rooms = max_rooms
        
    async def _fetch_build_id(self) -> str:
        """
        Fetch the build ID from the main rent page.
        This ID is required for constructing the API URLs.
        """
        timeout = aiohttp.ClientTimeout(total=REQUEST_TIMEOUT)
        async with aiohttp.ClientSession(timeout=timeout) as session:
            async with session.get(RENT_PAGE_URL, headers=DEFAULT_HEADERS) as response:
                shared_logger.info(f"Fetching build ID from {RENT_PAGE_URL}")
                response.raise_for_status()
                content = await response.text()
                
                # Parse the HTML to find the build ID
                soup = BeautifulSoup(content, 'html.parser')
                
                # Look for the script with id="__NEXT_DATA__"
                data_script = soup.find('script', {'id': '__NEXT_DATA__'})
                if data_script and data_script.string:
                    try:
                        data = json.loads(data_script.string)
                        build_id = data.get('buildId')
                        if build_id:
                            shared_logger.info(f"Found build ID from NEXT_DATA: {build_id}")
                            return build_id
                    except (json.JSONDecodeError, AttributeError):
                        shared_logger.error("Could not parse NEXT_DATA script content")
                
                # If we still can't find it, raise an exception
                shared_logger.error("Could not find build ID in the page content")
                raise ValueError("Could not extract build ID from rent page")
    
    async def _ensure_build_id(self):
        """Ensure we have a valid build ID, fetch it if needed."""
        if not self.build_id:
            self.build_id = await self._fetch_build_id()

    def _process_item(self, item: Dict[str, Any]) -> Dict[str, Any]:
        processed_item = {}

        # Extract address details - accessing 'text' field from nested objects
        address = item.get('address', {})
        city = address.get('city', {}).get('text', '')
        street = address.get('street', {}).get('text', '')
        
        # Create unified location field
        if city and street:
            location = f"{street}, {city}"
        elif city:
            location = city
        elif street:
            location = street
        else:
            location = ""
        
        processed_item['city'] = city
        processed_item['street'] = street
        processed_item['location'] = location
        # Extract coordinates
        coords = address.get('coords', {})
        processed_item['latitude'] = coords.get('lat')
        processed_item['longitude'] = coords.get('lon')

        # Extract price
        processed_item['price'] = item.get('price')

        # Extract ID (token is used as a unique identifier)
        processed_item['id'] = item.get('token')

        # Extract the actual apartment page URL
        processed_item['apartment_page_url'] = APARTMENT_PAGE_URL_TEMPLATE.format(
            token=item.get('token', ''))

        # Extract additional details if available
        additional_details = item.get('additionalDetails', {})
        processed_item['rooms'] = str(additional_details.get('roomsCount', ''))
        processed_item['size'] = str(additional_details.get('squareMeter', ''))

        # Extract metadata
        metadata = item.get('metaData', {})
        processed_item['images'] = metadata.get('images', [])

        # Extract tags if available
        tags = item.get('tags', [])
        processed_item['tags'] = [tag.get('name', '') for tag in tags]

        # Extract floor if available in address.house
        house_details = address.get('house', {})
        processed_item['floor'] = str(house_details.get('floor', ''))

        # Calculate and add MD5 hash
        processed_item['md5'] = self._get_md5(processed_item)
        processed_item['type'] = 'yad2'  # Add type field
        return processed_item

    async def _get_page_data(self, page_number: int, city: int) -> Dict[str, Any]:
        await asyncio.sleep(random.uniform(MIN_DELAY_BETWEEN_REQUESTS, MAX_DELAY_BETWEEN_REQUESTS))
        
        # Ensure we have the build ID before making requests
        await self._ensure_build_id()
        
        url = BASE_URL_TEMPLATE.format(
            build_id=self.build_id, 
            min_price=self.min_price,
            max_price=self.max_price,
            min_rooms=self.min_rooms,
            max_rooms=self.max_rooms,
            ct=city, 
            pg=page_number
        )

        timeout = aiohttp.ClientTimeout(total=REQUEST_TIMEOUT)
        async with aiohttp.ClientSession(timeout=timeout) as session:
            async with session.get(url, headers=DEFAULT_HEADERS) as response:
                shared_logger.info(
                    f"Fetching page {page_number} for city {city}, URL: {url}")
                response.raise_for_status()
                page_data = await response.json()

                # Log the number of apartments found on this page
                feed_data = page_data.get('pageProps', {}).get('feed', {})
                private_ads = feed_data.get('private', [])
                commercial_ads = feed_data.get('commercial', [])
                total_ads_on_page = len(private_ads) + len(commercial_ads)

                shared_logger.info(
                    f"Fetched page {page_number} for city {city}: Found {total_ads_on_page} apartments")

                return page_data

    def _get_md5(self, thing: Any) -> str:
        return hashlib.md5(str(thing).encode()).hexdigest()

    async def _process_page(self, page: Dict[str, Any]) -> List[Dict[str, Any]]:
        processed_items = []
        feed_data = page.get('pageProps', {}).get('feed', {})

        for item in feed_data.get('private', []):
            processed_items.append(self._process_item(item))

        return processed_items

    async def get_current(self) -> List[Dict[str, Any]]:
        current = []
        total_expected = 0

        for city in CITIES:
            first_page = await self._get_page_data(1, city)

            # Extract total pages and total items from the new JSON structure
            pagination_data = first_page.get('pageProps', {}).get(
                'feed', {}).get('pagination', {})
            page_count = pagination_data.get('totalPages', 0)
            total_expected += pagination_data.get('total', 0)

            current.extend(await self._process_page(first_page))

            # Handle the case where there is only one page
            if page_count > 1:
                shared_logger.info(
                    f"City {city} has {page_count} pages. Fetching remaining pages...")
                tasks = []
                for page_number in range(2, page_count + 1):
                    tasks.append(self._get_page_data(page_number, city))

                # Fetch remaining pages concurrently
                pages = await asyncio.gather(*tasks)
                for page in pages:
                    current.extend(await self._process_page(page))

        # Check if the number of fetched items matches the expected total
        if len(current) != total_expected:
            shared_logger.warning(
                f"Fetched {len(current)} items, but expected {total_expected} according to pagination.")

        return current

    async def run(self) -> List[Dict[str, Any]]:
        shared_logger.info("Starting Yad2 scraper run...")
        current = await self.get_current()
        shared_logger.info(
            f"Yad2 scraper finished, returning {len(current)} items.")
        return current


async def main() -> None:
    scraper = ApartmentScraper()
    apartments = await scraper.run()
    print(f"Yad2 scraper returned {len(apartments)} apartments.")


if __name__ == '__main__':
    asyncio.run(main())