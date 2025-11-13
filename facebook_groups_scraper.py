# facebook_groups_scraper.py
import asyncio
import hashlib
import json
import logging
import random
from pathlib import Path
from typing import Any, Dict, List

import aiohttp

from shared_scrapers_config import (CONNECT_TIMEOUT,
                                    MAX_DELAY_BETWEEN_REQUESTS,
                                    MIN_DELAY_BETWEEN_REQUESTS, OUTPUT_DIR,
                                    REQUEST_TIMEOUT, SOCK_READ_TIMEOUT,
                                    TOTAL_TIMEOUT)

# --- Configure Facebook Groups Scraper-specific logger ---
# This will be configured by the telegram bot's setup_logging
facebook_groups_logger = logging.getLogger(__name__)

# --- Constants ---
# Use the correct API endpoint
API_BASE_URL = "https://rentlyfly.ai/api/listings"
DEFAULT_USER_AGENT = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36"
DEFAULT_HEADERS = {
    "User-Agent": DEFAULT_USER_AGENT,
    "Accept": "application/json, text/plain, */*",
    "Accept-Language": "en-US,en;q=0.5",
    "Accept-Encoding": "gzip, deflate, br, zstd",
    "Connection": "keep-alive",
    "Referer": "https://rentlyfly.ai/",  # Referrer might be needed
    "Sec-Fetch-Dest": "empty",
    "Sec-Fetch-Mode": "cors",
    "Sec-Fetch-Site": "same-origin",
}

# --- File Names ---
JSON_OUTPUT_FILE_WITH_DETAILS = OUTPUT_DIR / "facebook_groups_apartments.json"


def _get_md5_for_comparison(item: Dict[str, Any]) -> str:
    """Calculate MD5 hash based on specific fields for comparison."""
    comparison_data = {
        'price': item.get('price'),
        # Assuming 'id' from the API response maps to a unique identifier
        'id': item.get('id'),
        'full_address': item.get('full_address', {})
    }
    # Create a consistent string representation for hashing
    # Sorting keys ensures the same order regardless of dict order
    comparison_string = json.dumps(
        comparison_data, sort_keys=True, default=str, ensure_ascii=False)
    return hashlib.md5(comparison_string.encode()).hexdigest()


class FacebookGroupsScraper:
    def __init__(
        self,
        min_price: int | None = None,
        max_price: int | None = None,
        min_rooms: int | None = None,
        max_rooms: int | None = None,
        is_shared_apartment: bool | None = None,
        is_sublet: bool | None = None,
        limit: int | None = None,
        structured_locations: List[Dict[str, str]] | None = None,
        output_dir: Path = OUTPUT_DIR,

    ):
        if structured_locations is None:
            # Default to central Tel Aviv areas as per the example
            structured_locations = [
                {"hood": "הצפון הישן - החלק הדרומי", "area": "מרכז"},
                {"hood": "הצפון הישן - החלק הצפוני", "area": "מרכז"},
                {"hood": "הצפון החדש - החלק הדרומי", "area": "מרכז"},
                {"hood": "לב תל אביב", "area": "מרכז"},
            ]

        self.limit = limit
        self.min_price = min_price
        self.max_price = max_price
        self.is_shared_apartment = is_shared_apartment
        self.is_sublet = is_sublet
        self.min_rooms = min_rooms
        self.max_rooms = max_rooms
        self.structured_locations = structured_locations
        self.output_dir = output_dir
        self.output_dir.mkdir(exist_ok=True)

        # Log the filters being used
        facebook_groups_logger.info(
            f"Facebook Groups scraper initialized with filters - "
            f"Limit: {self.limit}, "
            f"Min Price: {self.min_price}, Max Price: {self.max_price}, "
            f"Is Shared: {self.is_shared_apartment}, Is Sublet: {self.is_sublet}, "
            f"Min Rooms: {self.min_rooms}, Max Rooms: {self.max_rooms}, "
            f"Locations: {self.structured_locations}"
        )

    def build_query_params(self, page: int) -> Dict[str, Any]:
        """Build the query parameters for the API request."""
        # Create a copy of the base parameters to avoid modifying the instance variables
        params = {
            "page": page,
            "minPrice": self.min_price,
            "maxPrice": self.max_price,
            # Convert booleans to lowercase strings for the API
            "isSharedApartment": str(self.is_shared_apartment).lower() if self.is_shared_apartment is not None else None,
            "isSublet": str(self.is_sublet).lower() if self.is_sublet is not None else None,
            "minRooms": self.min_rooms,
            "maxRooms": self.max_rooms,
            # Use json.dumps for the structured locations list, ensuring unicode
            "structuredLocations": json.dumps(self.structured_locations, ensure_ascii=False),
        }
        if self.limit is not None:
            params["limit"] = self.limit
        # Remove any None values from the params dictionary before sending
        filtered_params = {k: v for k, v in params.items() if v is not None}
        return filtered_params

    async def fetch_apartments_page(self, page: int) -> Dict[str, Any]:
        """Fetch a single page of apartments from the API using a new session."""
        params = self.build_query_params(page)
        facebook_groups_logger.info(
            f"Fetching apartments from API for page: {page}")

        timeout = aiohttp.ClientTimeout(
            total=TOTAL_TIMEOUT,
            connect=CONNECT_TIMEOUT,
            sock_read=SOCK_READ_TIMEOUT
        )
        # Create a new session for this specific request
        async with aiohttp.ClientSession(timeout=timeout) as session:
            # Prepare the URL with query parameters
            url = API_BASE_URL
            # aiohttp handles URL encoding automatically when passing params
            async with session.get(url, headers=DEFAULT_HEADERS, params=params) as response:
                facebook_groups_logger.info(
                    f"API Response Status for page {page}: {response.status}")

                if response.status == 200:
                    response_json = await response.json()
                    # Log potential errors in the response
                    if 'error' in response_json:
                        facebook_groups_logger.error(
                            f"API Error on page {page}: {response_json['error']}")
                        raise aiohttp.ClientResponseError(
                            request_info=response.request_info,
                            history=response.history,
                            status=response.status,
                            message=f"API Error: {response_json['error']}"
                        )
                    return response_json
                elif response.status in (403, 401):
                    facebook_groups_logger.error(
                        f"Access denied (Status {response.status}) for page {page}. Likely requires specific headers or blocked by anti-bot.")
                    error_content = await response.text()
                    facebook_groups_logger.error(
                        f"Error page content for page {page} (truncated to 2000 chars):\n{error_content[:2000]}")
                    error_file = self.output_dir / \
                        f"error_page_login_required_page_{page}.html"
                    with open(error_file, 'w', encoding='utf-8') as f:
                        f.write(error_content)
                    raise aiohttp.ClientResponseError(
                        request_info=response.request_info,
                        history=response.history,
                        status=response.status,
                        message=f"Access denied: {response.status}"
                    )
                elif response.status == 429:
                    facebook_groups_logger.error(
                        f"Rate limited (Status 429) for page {page}.")
                    raise aiohttp.ClientResponseError(
                        request_info=response.request_info,
                        history=response.history,
                        status=response.status,
                        message="Rate limited"
                    )
                else:
                    facebook_groups_logger.warning(
                        f"Non-200 status {response.status} for page {page}")
                    content = await response.text()
                    facebook_groups_logger.error(
                        f"Non-200 Response for page {page} (Status {response.status}, truncated):\n{content[:2000]}")
                    error_file = self.output_dir / \
                        f"error_page_{response.status}_page_{page}.html"
                    with open(error_file, 'w', encoding='utf-8') as f:
                        f.write(content)
                    raise aiohttp.ClientResponseError(
                        request_info=response.request_info,
                        history=response.history,
                        status=response.status,
                        message=f"Unexpected status: {response.status}"
                    )

    def normalize_apartment_data(self, raw_item: Dict[str, Any]) -> Dict[str, Any]:
        """Normalize raw API data into a standard format."""
        # Assuming 'id' from the API response is the unique identifier
        item_id = raw_item.get(
            'id', f"unknown_id_{hash(json.dumps(raw_item, sort_keys=True, ensure_ascii=False))}")

        # Extract core details
        price = raw_item.get('price', 'N/A')
        location_details = raw_item.get('location', {})
        url = raw_item.get('url', 'N/A')
        description = raw_item.get('description', 'No description provided')
        time_posted = raw_item.get('time', 'N/A')
        photos = raw_item.get('photos', [])
        thumbnail_url = raw_item.get('thumbnailUrl', 'N/A')
        phones = raw_item.get('phones', [])
        is_shared = raw_item.get('isSharedApartment', 'N/A')
        is_brokered = raw_item.get('isBrokeredApartment', 'N/A')
        is_sublet = raw_item.get('isSublet', 'N/A')
        rooms_available = raw_item.get('roomsAvailable', 'N/A')
        user_username = raw_item.get('user_username_raw', 'N/A')
        group_name = raw_item.get('group_name', 'N/A')
        group_url = raw_item.get('group_url', 'N/A')

        # Construct unified location string
        city = location_details.get('city', 'N/A')
        area = location_details.get('area', 'N/A')
        hood = location_details.get('hood', 'N/A')
        street = location_details.get('street', 'N/A')
        full_address = f"{street}, {hood}, {area}, {city}" if street != 'N/A' else f"{hood}, {area}, {city}"
        
        # Construct a title if not present
        title = raw_item.get(
            'title', f"Apartment at {full_address} for {price}")

        # Calculate numeric price from string price if possible
        import re
        numeric_price = None
        if isinstance(price, str):
            # Remove currency symbols and commas, extract the number
            numeric_price_str = re.sub(r'[^\d,]', '', price)
            numeric_price_str = numeric_price_str.replace(',', '')
            try:
                numeric_price = int(numeric_price_str)
            except ValueError:
                numeric_price = None
        else:
            # If price is already a number, use it directly
            numeric_price = price

        # Create the normalized apartment object
        normalized_item = {
            "id": item_id,
            "title": title,
            "description": description,
            "apartment_page_url": url,
            "price": numeric_price,  # Use the extracted numeric price
            "formatted_price": price,  # Keep the original formatted price
            # May be N/A initially
            "latitude": location_details.get('latitude', 'N/A'),
            # May be N/A initially
            "longitude": location_details.get('longitude', 'N/A'),
            "location": full_address,  # Use the constructed full address as the main location
            "full_address": full_address,  # Also store it in full_address
            "city": city,
            "area": area,
            "hood": hood,
            "street": street,
            "rooms": rooms_available,  # Map roomsAvailable to rooms
            "images": photos,  # Use the photos list
            "thumbnail_url": thumbnail_url,
            "time_posted": time_posted,
            "phones": phones,
            "is_shared_apartment": is_shared,
            "is_brokered_apartment": is_brokered,
            "is_sublet": is_sublet,
            "user_username_raw": user_username,
            "group_name": group_name,
            "group_url": group_url,
            "type": "facebook groups",  # Add type field
            # Calculate MD5 based on specific fields (latitude/longitude might be N/A initially)
            "md5": _get_md5_for_comparison({
                "price": numeric_price,
                "id": item_id,
                "full_address": full_address,
            })
        }
        return normalized_item

    async def run(self) -> List[Dict[str, Any]]:
        all_apartments = []
        current_page = 1  # Start from page 1

        while True:  # Loop until hasMore is False
            # Fetch the current page
            api_response = await self.fetch_apartments_page(current_page)
            # Assuming the list of apartments is under 'data'
            data_list = api_response.get('data', [])
            pagination_info = api_response.get('pagination', {})
            # Check if there are more pages
            has_more = pagination_info.get('hasMore', False)

            # Normalize the data from this page
            if data_list:
                normalized_page_data = [
                    self.normalize_apartment_data(item) for item in data_list]
                all_apartments.extend(normalized_page_data)
                facebook_groups_logger.debug(
                    f"Normalized {len(normalized_page_data)} items from page {current_page}.")
            else:
                facebook_groups_logger.warning(
                    f"No data found on page {current_page}, stopping.")
                break  # Stop if no data is returned unexpectedly

            # Check if there are more pages to fetch based on the response
            if not has_more:
                facebook_groups_logger.info(
                    f"No more pages to fetch after page {current_page}.")
                break

            current_page += 1

            delay = random.uniform(
                MIN_DELAY_BETWEEN_REQUESTS, MAX_DELAY_BETWEEN_REQUESTS)
            facebook_groups_logger.debug(
                f"Waiting {delay:.2f}s before fetching page {current_page}...")
            await asyncio.sleep(delay)

        facebook_groups_logger.info(
            f"Final data: {len(all_apartments)} Apartments fetched and normalized from Facebook Groups.")
        return all_apartments


async def main():
    # Example usage with default parameters
    scraper = FacebookGroupsScraper(
        min_rooms=3,
        max_price=10000,
        limit=50,
        is_shared_apartment=False,
        is_sublet=False
    )
    
    # --- Conditional Logging Setup for Standalone Execution ---
    # Check if the logger (or its parent root logger) already has handlers
    # This prevents adding handlers when run as part of the bot which sets up logging globally
    if not facebook_groups_logger.handlers and not logging.getLogger().handlers:
        # Create a file handler
        file_handler = logging.FileHandler("facebook_groups_scraper.log")
        # Create a console handler
        console_handler = logging.StreamHandler()

        # Create a formatter
        formatter = logging.Formatter(
            '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
        )
        file_handler.setFormatter(formatter)
        console_handler.setFormatter(formatter)

        # Add the new handlers to the specific logger for this module
        facebook_groups_logger.addHandler(file_handler)
        facebook_groups_logger.addHandler(console_handler)
        
        # Set the logging level for this specific logger
        facebook_groups_logger.setLevel(logging.INFO)
    
    # Log that scraping is starting
    facebook_groups_logger.info("Starting Facebook Groups scraping...")
    
    apartments = await scraper.run()
    facebook_groups_logger.info(
        f"Scraping completed. Total apartments fetched: {len(apartments)}")


if __name__ == "__main__":
    asyncio.run(main())