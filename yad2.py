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
from shared_scrapers_config import (DEFAULT_MAX_PRICE, DEFAULT_MAX_ROOMS,
                                    DEFAULT_MIN_ROOMS,
                                    DEFAULT_YAD2_LOCATION_FILTERS,
                                    MAX_DELAY_BETWEEN_REQUESTS,
                                    MIN_DELAY_BETWEEN_REQUESTS, OUTPUT_DIR,
                                    REQUEST_TIMEOUT)
from shared_scrapers_config import logger as shared_logger

# --- Configure Yad2-specific logger ---
# This will be configured by the telegram bot's setup_logging
yad2_logger = logging.getLogger(__name__)

# --- Configuration ---
# URL Templates - Base without optional filters
RENT_PAGE_URL = f"https://www.yad2.co.il/realestate/rent/{DEFAULT_YAD2_LOCATION_FILTERS[0]['route_slug']}?area={DEFAULT_YAD2_LOCATION_FILTERS[0]['area']}&city={DEFAULT_YAD2_LOCATION_FILTERS[0]['city']}"
RENT_DATA_URL = 'https://www.yad2.co.il/realestate/_next/data/{build_id}/rent/{route_slug}.json'
ITEM_DATA_URL = 'https://www.yad2.co.il/realestate/_next/data/{build_id}/item/{route_slug}/{token}.json'
CITIES = DEFAULT_YAD2_LOCATION_FILTERS

APARTMENT_PAGE_URL_TEMPLATE = 'https://www.yad2.co.il/realestate/item/{token}'

USER_AGENT = 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/141.0.0.0 Safari/537.36'
DEFAULT_HEADERS = {
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
    'Accept-Encoding': 'gzip, deflate',
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
    def __init__(self, min_price: Optional[int] = None, max_price: Optional[int] = None, min_rooms: Optional[float] = None, max_rooms: Optional[float] = None, multi_neighborhoods: List[int] | None = None, min_squaremeter: Optional[int] = None, image_only: Optional[bool] = None, price_only: Optional[bool] = None, require_mamad: Optional[bool] = None, require_elevator: Optional[bool] = None, min_floor: Optional[int] = None, max_floor: Optional[int] = None):
        if multi_neighborhoods is None:
            multi_neighborhoods = []

        self.build_id = None
        self.min_price = min_price
        self.max_price = max_price
        self.min_rooms = min_rooms
        self.max_rooms = max_rooms
        self.min_squaremeter = min_squaremeter
        self.image_only = image_only
        self.price_only = price_only
        self.require_mamad = require_mamad
        self.require_elevator = require_elevator
        self.min_floor = min_floor
        self.max_floor = max_floor
        # Store the list of neighborhoods, if any.
        self.multi_neighborhoods = multi_neighborhoods
        # Create a comma-separated string for the URL when neighborhood filtering is used.
        self.multi_neighborhoods_str = ','.join(map(str, self.multi_neighborhoods))
        
        # Log the filters being used
        yad2_logger.info(
            f"Yad2 scraper initialized with filters - "
            f"Min Price: {self.min_price}, Max Price: {self.max_price}, "
            f"Min Rooms: {self.min_rooms}, Max Rooms: {self.max_rooms}, "
            f"Min SquareMeter: {self.min_squaremeter}, "
            f"Image Only: {self.image_only}, Price Only: {self.price_only}, "
            f"Mamad: {self.require_mamad}, Elevator: {self.require_elevator}, "
            f"Min Floor: {self.min_floor}, Max Floor: {self.max_floor}, "
            f"Multi Neighborhoods: {self.multi_neighborhoods_str or 'none'}"
        )

    @staticmethod
    def _normalize_text(value: str) -> str:
        value = (value or '').replace('קריית', 'קרית')
        return ''.join(value.split()).casefold()

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

    def _extract_floor(self, item: Dict[str, Any]) -> str:
        address = item.get('address', {})
        house_details = address.get('house', {})
        floor = house_details.get('floor', '')
        return str(floor) if floor is not None else ''

    def _extract_room_tags(self, item: Dict[str, Any]) -> List[str]:
        tags = item.get('tags', [])
        return [tag.get('name', '') for tag in tags if isinstance(tag, dict)]

    def _process_item(self, item: Dict[str, Any]) -> Dict[str, Any]:
        processed_item = {}

        # Extract address details - accessing 'text' field from nested objects
        address = item.get('address', {})
        city = address.get('city', {}).get('text', '')
        street = address.get('street', {}).get('text', '')
        neighborhood = address.get('neighborhood', {}).get('text', '')
        region = address.get('region', {}).get('text', '')

        # Create unified location field
        parts = []
        for part in [street, neighborhood, city]:
            if part and part not in parts:
                parts.append(part)
        location = ", ".join(parts)

        processed_item['city'] = city
        processed_item['area'] = region
        processed_item['hood'] = neighborhood
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
        processed_item['floor'] = self._extract_floor(item)

        # Extract metadata
        metadata = item.get('metaData', {})
        processed_item['images'] = metadata.get('images', [])

        # Extract tags if available
        tags = item.get('tags', [])
        processed_item['tags'] = [tag.get('name', '') for tag in tags]
        tag_text = " ".join(processed_item['tags'])
        processed_item['is_mamad'] = 'ממ"ד' in tag_text or 'ממד' in tag_text
        processed_item['is_elevator'] = 'מעלית' in tag_text
        
        md5_item_part = {
            'location': processed_item['location'],
            'price': processed_item['price'],
        }

        # Calculate and add MD5 hash
        processed_item['md5'] = self._get_md5(md5_item_part)
        processed_item['type'] = 'yad2'  # Add type field
        return processed_item

    async def _get_page_data(self, page_number: int, location: Dict[str, int | str]) -> Dict[str, Any]: 
        # Ensure we have the build ID before making requests
        await self._ensure_build_id()
        
        # Prepare the base URL with the build ID
        url = RENT_DATA_URL.format(build_id=self.build_id, route_slug=location['route_slug'])
        city = location['city']
        area = location['area']
        route_slug = location['route_slug']
        
        # Prepare query parameters dictionary
        # Start with the required parameters that are always present
        params = {
            "area": area,
            "city": city,
            "page": page_number
        }
        if location.get('neighborhood') is not None:
            params["neighborhood"] = location['neighborhood']
        if location.get('bBox') is not None:
            params["bBox"] = location['bBox']
        if location.get('zoom') is not None:
            params["zoom"] = location['zoom']
        if self.multi_neighborhoods_str:
            params["multiNeighborhood"] = self.multi_neighborhoods_str
        
        timeout = aiohttp.ClientTimeout(total=REQUEST_TIMEOUT)
        for attempt in range(3):
            async with aiohttp.ClientSession() as session:
                # Pass the params dictionary to aiohttp
                async with session.get(url, headers=DEFAULT_HEADERS, params=params) as response:
                    if page_number == 1:
                        yad2_logger.info(
                            f"Fetching first page for city {city} / area {area} / slug {route_slug} with neighborhoods {self.multi_neighborhoods_str}, URL: {url}, Params: {params}")
                    else:
                        yad2_logger.debug(
                            f"Fetching page {page_number} for city {city} / area {area} / slug {route_slug} with neighborhoods {self.multi_neighborhoods_str}, URL: {url}, Params: {params}")

                    if response.status in {500, 502, 503, 504}:
                        if attempt < 2:
                            wait_seconds = 2 ** attempt
                            yad2_logger.warning(
                                f"Transient Yad2 error {response.status} for city {city} / area {area}, retrying in {wait_seconds}s (attempt {attempt + 1}/3)")
                            await asyncio.sleep(wait_seconds)
                            continue

                    response.raise_for_status()
                    page_data = await response.json()
                    return page_data

        raise RuntimeError(
            f"Failed to fetch Yad2 page {page_number} for city {city} / area {area} after retries"
        )

    async def _get_item_detail_data(self, token: str, route_slug: str) -> Dict[str, Any]:
        await self._ensure_build_id()
        url = ITEM_DATA_URL.format(
            build_id=self.build_id,
            route_slug=route_slug,
            token=token,
        )
        async with aiohttp.ClientSession() as session:
            async with session.get(url, headers=DEFAULT_HEADERS) as response:
                response.raise_for_status()
                return await response.json()

    def _get_md5(self, thing: Any) -> str:
        return hashlib.md5(str(thing).encode()).hexdigest()

    def _extract_feed(self, page: Dict[str, Any]) -> Dict[str, Any]:
        dehydrated_state = page.get('pageProps', {}).get('dehydratedState', {})
        queries = dehydrated_state.get('queries', [])
        for query in queries:
            query_key = query.get('queryKey', [])
            if isinstance(query_key, list) and query_key and query_key[0] == 'realestate-rent-feed':
                data = query.get('state', {}).get('data', {})
                if isinstance(data, dict):
                    return data
        return {}

    async def _process_page(self, feed_data: Dict[str, Any]) -> List[Dict[str, Any]]:
        processed_items = []
        for bucket_name, bucket_items in feed_data.items():
            if bucket_name in {'pagination', 'lookalike'}:
                continue
            if isinstance(bucket_items, list):
                for item in bucket_items:
                    processed_items.append(self._process_item(item))

        return processed_items

    def _extract_item_detail(self, detail_data: Dict[str, Any]) -> Dict[str, Any]:
        queries = detail_data.get('pageProps', {}).get('dehydratedState', {}).get('queries', [])
        for query in queries:
            query_key = query.get('queryKey', [])
            if isinstance(query_key, list) and query_key and query_key[0] == 'item':
                data = query.get('state', {}).get('data', {})
                if isinstance(data, dict):
                    return data
        return {}

    def _apply_item_detail(self, item: Dict[str, Any], detail_item: Dict[str, Any]) -> Dict[str, Any]:
        in_property = detail_item.get('inProperty', {})
        if 'includeSecurityRoom' in in_property:
            item['is_mamad'] = bool(in_property.get('includeSecurityRoom'))
        if 'includeElevator' in in_property:
            item['is_elevator'] = bool(in_property.get('includeElevator'))

        metadata = detail_item.get('metaData', {})
        if metadata.get('description'):
            item['description'] = metadata['description']

        additional_details = detail_item.get('additionalDetails', {})
        if additional_details.get('squareMeter') is not None:
            item['size'] = str(additional_details.get('squareMeter'))
        if additional_details.get('roomsCount') is not None:
            item['rooms'] = str(additional_details.get('roomsCount'))

        return item

    def _matches_filters(self, item: Dict[str, Any]) -> bool:
        price_value = item.get('price')
        if self.min_price is not None and (price_value is None or price_value < self.min_price):
            return False
        if self.max_price is not None and (price_value is None or price_value > self.max_price):
            return False

        rooms_value = item.get('rooms')
        try:
            rooms_number = float(rooms_value) if rooms_value not in ('', None) else None
        except (TypeError, ValueError):
            rooms_number = None
        if self.min_rooms is not None and (rooms_number is None or rooms_number < self.min_rooms):
            return False
        if self.max_rooms is not None and (rooms_number is None or rooms_number > self.max_rooms):
            return False

        size_value = item.get('size')
        try:
            size_number = float(size_value) if size_value not in ('', None) else None
        except (TypeError, ValueError):
            size_number = None
        if self.min_squaremeter is not None and (size_number is None or size_number < self.min_squaremeter):
            return False

        floor_value = item.get('floor')
        floor_number = int(floor_value) if str(floor_value).isdigit() else None
        if self.min_floor is not None and (floor_number is None or floor_number < self.min_floor):
            return False
        if self.max_floor is not None and (floor_number is None or floor_number > self.max_floor):
            return False

        return True

    def _matches_amenity_filters(self, item: Dict[str, Any]) -> bool:
        if self.require_mamad is not None and bool(item.get('is_mamad')) != self.require_mamad:
            return False
        if self.require_elevator is not None and bool(item.get('is_elevator')) != self.require_elevator:
            return False
        return True

    async def _enrich_and_filter_amenities(self, items: List[Dict[str, Any]], location: Dict[str, Any]) -> List[Dict[str, Any]]:
        if self.require_mamad is None and self.require_elevator is None:
            return items

        filtered_items = []
        for item in items:
            token = item.get('id')
            if token:
                try:
                    detail_data = await self._get_item_detail_data(token, str(location['route_slug']))
                    detail_item = self._extract_item_detail(detail_data)
                    if detail_item:
                        item = self._apply_item_detail(item, detail_item)
                except Exception as detail_error:
                    yad2_logger.warning(
                        f"Could not fetch Yad2 item details for {token}: {detail_error}")

            if self._matches_amenity_filters(item):
                filtered_items.append(item)

        return filtered_items

    def _matches_location(self, item: Dict[str, Any], location: Dict[str, Any]) -> bool:
        expected_name = self._normalize_text(str(location.get('name', '')))
        if location.get('match_field') == 'hood':
            return self._normalize_text(item.get('hood', '')) == expected_name

        return self._normalize_text(item.get('city', '')) == expected_name

    async def get_current(self) -> List[Dict[str, Any]]:
        current = []
        total_expected = 0
        page_cache: Dict[tuple, Dict[str, Any]] = {}

        async def get_page_cached(page_number: int, location: Dict[str, Any]) -> Dict[str, Any]:
            cache_key = (
                location.get('route_slug'),
                location.get('area'),
                location.get('city'),
                location.get('bBox'),
                location.get('zoom'),
                location.get('neighborhood'),
                page_number,
            )
            if cache_key not in page_cache:
                page_cache[cache_key] = await self._get_page_data(page_number, location)
            return page_cache[cache_key]

        for location in CITIES:
            first_page = await get_page_cached(1, location)

            feed_data = self._extract_feed(first_page)
            # Extract total pages and total items from the new JSON structure
            pagination_data = feed_data.get('pagination', {})
            page_count = pagination_data.get('totalPages', 0)
            total_expected += pagination_data.get('total', 0)

            city_items = await self._process_page(feed_data)
            base_matches = [
                item for item in city_items
                if self._matches_location(item, location) and self._matches_filters(item)
            ]
            current.extend(await self._enrich_and_filter_amenities(base_matches, location))

            # Handle the case where there is only one page
            if page_count > 1:
                yad2_logger.info(
                    f"City {location['city']} / area {location['area']} with neighborhoods {self.multi_neighborhoods_str} has {page_count} pages. Fetching remaining pages...")
                for page_number in range(2, page_count + 1):
                    page = await get_page_cached(page_number, location)
                    page_feed = self._extract_feed(page)
                    page_items = await self._process_page(page_feed)
                    base_matches = [
                        item for item in page_items
                        if self._matches_location(item, location) and self._matches_filters(item)
                    ]
                    current.extend(await self._enrich_and_filter_amenities(base_matches, location))

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
        max_price=DEFAULT_MAX_PRICE,
        min_rooms=DEFAULT_MIN_ROOMS,
        max_rooms=DEFAULT_MAX_ROOMS,
        min_squaremeter=65,
        require_mamad=None,
        require_elevator=None,
        min_floor=None,
        max_floor=None,
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
