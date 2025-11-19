# yad2.py
import asyncio
import datetime
import hashlib
import json
import logging
import random
from pathlib import Path
from typing import Any, Dict, List, Optional

import aiohttp
from bs4 import BeautifulSoup

# Import shared configuration
from shared_scrapers_config import (MAX_DELAY_BETWEEN_REQUESTS,
                                    MIN_DELAY_BETWEEN_REQUESTS, OUTPUT_DIR,
                                    REQUEST_TIMEOUT)
from shared_scrapers_config import logger as shared_logger

# --- Configure Yad2-specific logger ---
# This will be configured by the telegram bot's setup_logging
yad2_logger = logging.getLogger(__name__)

# --- Configuration ---
# URL Templates - Base without optional filters
# The base URL now includes only the non-optional parts and placeholders for build_id, city, neighborhoods, and page
BASE_URL = 'https://www.yad2.co.il/realestate/_next/data/{build_id}/rent.json'
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
    def __init__(self, min_price: Optional[int] = None, max_price: Optional[int] = None, min_rooms: Optional[float] = None, max_rooms: Optional[float] = None, multi_neighborhoods: List[int] = None, min_squaremeter: Optional[int] = None, image_only: Optional[bool] = None, price_only: Optional[bool] = None):
        if multi_neighborhoods is None:
            # Default to central Tel Aviv neighborhood IDs
            multi_neighborhoods = [1519, 1483, 1461, 1520, 1462]
        
        self.build_id = None
        self.min_price = min_price
        self.max_price = max_price
        self.min_rooms = min_rooms
        self.max_rooms = max_rooms
        self.min_squaremeter = min_squaremeter
        self.image_only = image_only
        self.price_only = price_only
        # Store the list of neighborhoods
        self.multi_neighborhoods = multi_neighborhoods
        # Create a comma-separated string for the URL
        self.multi_neighborhoods_str = ','.join(map(str, self.multi_neighborhoods))
        
        # Log the filters being used
        yad2_logger.info(
            f"Yad2 scraper initialized with filters - "
            f"Min Price: {self.min_price}, Max Price: {self.max_price}, "
            f"Min Rooms: {self.min_rooms}, Max Rooms: {self.max_rooms}, "
            f"Min SquareMeter: {self.min_squaremeter}, "
            f"Image Only: {self.image_only}, Price Only: {self.price_only}, "
            f"Multi Neighborhoods: {self.multi_neighborhoods_str}"
        )

    async def _fetch_build_id(self) -> str:
        """
        Fetch the build ID from the main rent page.
        This ID is required for constructing the API URLs.
        """
        timeout = aiohttp.ClientTimeout(total=REQUEST_TIMEOUT)
        async with aiohttp.ClientSession(timeout=timeout) as session:
            async with session.get(RENT_PAGE_URL, headers=DEFAULT_HEADERS) as response:
                yad2_logger.info(f"Fetching build ID from {RENT_PAGE_URL}")
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
                            yad2_logger.info(f"Found build ID from NEXT_DATA: {build_id}")
                            return build_id
                    except (json.JSONDecodeError, AttributeError):
                        yad2_logger.error("Could not parse NEXT_DATA script content")

                # If we still can't find it, raise an exception
                yad2_logger.error("Could not find build ID in the page content")
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
        
        md5_item_part = {
            'location': processed_item['location'],
            'price': processed_item['price'],
        }

        # Calculate and add MD5 hash
        processed_item['md5'] = self._get_md5(md5_item_part)
        processed_item['type'] = 'yad2'  # Add type field
        return processed_item

    async def _get_page_data(self, page_number: int, city: int) -> Dict[str, Any]: 
        await asyncio.sleep(random.uniform(MIN_DELAY_BETWEEN_REQUESTS, MAX_DELAY_BETWEEN_REQUESTS))
        # Ensure we have the build ID before making requests
        await self._ensure_build_id()
        
        # Prepare the base URL with the build ID
        url = BASE_URL.format(build_id=self.build_id)
        
        # Prepare query parameters dictionary
        # Start with the required parameters that are always present
        params = {
            "topArea": "2",
            "area": "1",
            "city": city,
            "multiNeighborhood": self.multi_neighborhoods_str,
            "page": page_number
        }
        
        # Add optional filters to the params dictionary if they are not None
        if self.min_price is not None:
            params["minPrice"] = self.min_price
        if self.max_price is not None:
            params["maxPrice"] = self.max_price
        if self.min_rooms is not None:
            params["minRooms"] = self.min_rooms
        if self.max_rooms is not None:
            params["maxRooms"] = self.max_rooms
        if self.min_squaremeter is not None:
            params["minSquareMeter"] = self.min_squaremeter
        if self.image_only is not None:
            # Convert boolean to 0 or 1 for the API
            params["imageOnly"] = int(self.image_only)
        if self.price_only is not None:
            # Convert boolean to 0 or 1 for the API
            params["priceOnly"] = int(self.price_only)

        timeout = aiohttp.ClientTimeout(total=REQUEST_TIMEOUT)
        async with aiohttp.ClientSession(timeout=timeout) as session:
            # Pass the params dictionary to aiohttp
            async with session.get(url, headers=DEFAULT_HEADERS, params=params) as response:
                if page_number == 1:
                    yad2_logger.info(
                        f"Fetching first page for city {city} with neighborhoods {self.multi_neighborhoods_str}, URL: {url}, Params: {params}")
                else:
                    yad2_logger.debug(
                        f"Fetching page {page_number} for city {city} with neighborhoods {self.multi_neighborhoods_str}, URL: {url}, Params: {params}")
                response.raise_for_status()
                page_data = await response.json()

                # Log the number of apartments found on this page
                feed_data = page_data.get('pageProps', {}).get('feed', {})
                private_ads = feed_data.get('private', [])
                agency_ads = feed_data.get('agency', [])
                total_ads_on_page = len(private_ads) + len(agency_ads)

                yad2_logger.debug(
                    f"Fetched page {page_number} for city {city} with neighborhoods {self.multi_neighborhoods_str}: Found {total_ads_on_page} apartments")

                return page_data

    def _get_md5(self, thing: Any) -> str:
        return hashlib.md5(str(thing).encode()).hexdigest()

    async def _process_page(self, page: Dict[str, Any]) -> List[Dict[str, Any]]:
        processed_items = []
        feed_data = page.get('pageProps', {}).get('feed', {})

        for item in feed_data.get('private', []):
            processed_items.append(self._process_item(item))
            
        for item in feed_data.get('agency', []):
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
                yad2_logger.info(
                    f"City {city} with neighborhoods {self.multi_neighborhoods_str} has {page_count} pages. Fetching remaining pages...")
                tasks = []
                for page_number in range(2, page_count + 1):
                    tasks.append(self._get_page_data(page_number, city))

                # Fetch remaining pages concurrently
                pages = await asyncio.gather(*tasks)
                for page in pages:
                    current.extend(await self._process_page(page))

        # # Check if the number of fetched items matches the expected total
        # if len(current) != total_expected:
        #     yad2_logger.warning(
        #         f"Fetched {len(current)} items, but expected {total_expected} according to pagination.")

        return current

    async def run(self) -> List[Dict[str, Any]]:
        yad2_logger.info("Starting Yad2 scraper run...")
        current = await self.get_current()
        yad2_logger.info(
            f"Yad2 scraper finished, returning {len(current)} items.")
        return current


async def main() -> None:
    # Example usage with default neighborhoods and some filters
    scraper = ApartmentScraper(
        min_price=3000,
        max_price=10000,
        min_rooms=2.5,
        # max_rooms=4.0, # This filter is None, so it won't be added to the URL
        min_squaremeter=65,
        # image_only=True,
        # price_only=None, # This filter is None, so it won't be added to the URL
    )
    apartments = await scraper.run()
    yad2_logger.debug(f"Yad2 scraper returned {len(apartments)} apartments.")

    # Example usage with no filters (all optional params are None by default)
    # scraper_no_filters = ApartmentScraper()
    # apartments_no_filters = await scraper_no_filters.run()
    # yad2_logger.debug(f"Yad2 scraper (no filters) returned {len(apartments_no_filters)} apartments.")


if __name__ == '__main__':
    asyncio.run(main())