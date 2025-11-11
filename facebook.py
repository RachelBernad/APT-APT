# facebook.py
import asyncio
import datetime
import json
import logging
import random
from pathlib import Path
from typing import Any, Dict, List, Optional, Union

import aiohttp
from bs4 import BeautifulSoup

# Import shared configuration
from shared_scrapers_config import (CONNECT_TIMEOUT,
                                    MAX_DELAY_BETWEEN_REQUESTS,
                                    MIN_DELAY_BETWEEN_REQUESTS, OUTPUT_DIR,
                                    REQUEST_TIMEOUT, SOCK_READ_TIMEOUT,
                                    TOTAL_TIMEOUT)
from shared_scrapers_config import logger as shared_logger # This is now a placeholder

# --- Configure Facebook-specific logger ---
# This will be configured by the telegram bot's setup_logging
facebook_logger = logging.getLogger(__name__)

# --- URLs and Endpoints ---
MARKETPLACE_BASE_URL = "https://www.facebook.com/marketplace/"
# --- File Names ---
HTML_OUTPUT_FILE = OUTPUT_DIR / "fetched_page.html"
JSON_OUTPUT_FILE_WITH_DETAILS = OUTPUT_DIR / "apartments_with_details.json"
# New internal directory for debug JSONs
DEBUG_JSON_DIR = OUTPUT_DIR / "debug_jsons"
# --- Batch Configuration ---
BATCH_SIZE = 50

# --- JSON Script Tag Attributes ---
JSON_SCRIPT_TAG_ATTR_NAME = 'data-sjs'

# --- JSON Key Prefixes ---
JSON_KEY_PREFIX_LISTINGS = "adp_CometMarketplaceRealEstateMapStoryQueryRelayPreloader_"
JSON_KEY_PREFIX_DETAILS = "adp_MarketplacePDPContainerQueryRelayPreloader_"

# --- HTTP Headers ---
HEADERS = {
    'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8',
    'accept-encoding': 'gzip, deflate, br',
    'accept-language': 'en-US,en;q=0.9,he;q=0.8',
    'cache-control': 'no-cache',
    'pragma': 'no-cache',
    'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/129.0.0.0 Safari/537.36',
    'sec-ch-ua': '"Chromium";v="129", "Not=A?Brand";v="24"',
    'sec-ch-ua-mobile': '?0',
    'sec-ch-ua-platform': '"Windows"',
    'sec-fetch-dest': 'document',
    'sec-fetch-mode': 'navigate',
    'sec-fetch-site': 'none',
    'sec-fetch-user': '?1',
    'upgrade-insecure-requests': '1',
}


class FacebookMarketplaceScraper:
    def __init__(self, min_price, max_price, min_bedrooms, lat, lng, radius, output_dir: Path = OUTPUT_DIR):
        self.min_price = min_price
        self.max_price = max_price
        self.min_bedrooms = min_bedrooms
        self.lat = lat
        self.lng = lng
        self.radius = radius
        # Log the filters being used
        facebook_logger.info(
            f"Facebook scraper initialized with filters - "
            f"Min Price: {self.min_price}, Max Price: {self.max_price}, "
            f"Min Bedrooms: {self.min_bedrooms}, "
            f"Location: ({self.lat}, {self.lng}), Radius: {self.radius}m"
        )
        self.listings_url = f"{MARKETPLACE_BASE_URL}telaviv/propertyrentals?minPrice={min_price}&maxPrice={max_price}&minBedrooms={min_bedrooms}&exact=false&latitude={lat}&longitude={lng}&radius={radius}"
        self.output_dir = output_dir
        self.output_dir.mkdir(exist_ok=True)

    def save_html_to_file(self, html_content: str, filename: Path = HTML_OUTPUT_FILE):
        with open(filename, 'w', encoding='utf-8') as f:
            f.write(html_content)
        facebook_logger.debug(f"Fetched HTML saved to {filename}")

    def save_apartments_to_json(self, apartments: List[Dict[str, Any]], filename: Path = JSON_OUTPUT_FILE_WITH_DETAILS):
        with open(filename, 'w', encoding='utf-8') as f:
            json.dump(apartments, f, ensure_ascii=False, indent=2)
        facebook_logger.info(f"Extracted apartments saved to {filename}")

    def extract_json_script_content(self, html_content: str) -> List[str]:
        soup = BeautifulSoup(html_content, 'html.parser')
        script_tags = soup.find_all(
            'script', attrs={JSON_SCRIPT_TAG_ATTR_NAME: True})

        json_strings = []
        for tag in script_tags:
            content = tag.string
            if content:
                json_strings.append(content.strip())

        if json_strings:
            facebook_logger.debug(
                f"Found {len(json_strings)} potential JSON script tags using BeautifulSoup.")
            return json_strings
        else:
            facebook_logger.error(
                f"No script tags with {JSON_SCRIPT_TAG_ATTR_NAME} attribute found.")
            raise ValueError(
                f"No script tags with {JSON_SCRIPT_TAG_ATTR_NAME} attribute found.")

    def find_rental_data_in_blob(self, parsed_json: Dict[str, Any]) -> List[Dict[str, Any]]:
        facebook_logger.debug(
            "Starting to navigate JSON blob for rental listings...")
        for item in parsed_json.get('require', []):
            if isinstance(item, list) and len(item) >= 4 and isinstance(item[3], list) and len(item[3]) > 0:
                data_block = item[3][0]
                if (isinstance(data_block, dict) and '__bbox' in data_block and
                        isinstance(data_block['__bbox'], dict) and 'require' in data_block['__bbox']):
                    inner_require_list = data_block['__bbox']['require']
                    for inner_item in inner_require_list:
                        if (isinstance(inner_item, list) and len(inner_item) >= 4 and
                                isinstance(inner_item[3], list) and len(inner_item[3]) >= 2):
                            potential_key = inner_item[3][0]
                            potential_data_obj = inner_item[3][1]
                            if isinstance(potential_key, str) and potential_key.startswith(JSON_KEY_PREFIX_LISTINGS):
                                nested_bbox = potential_data_obj.get(
                                    "__bbox", {})
                                result = nested_bbox.get("result", {})
                                data_viewer = result.get(
                                    "data", {}).get("viewer", {})
                                stories_data = data_viewer.get(
                                    "marketplace_rentals_map_view_stories", {})
                                if stories_data:
                                    facebook_logger.debug(
                                        f"Found 'marketplace_rentals_map_view_stories' under key {potential_key}.")
                                    edges = stories_data.get("edges", [])
                                    return edges
        return []

    def parse_rental_info(self, edge: Dict[str, Any]) -> Dict[str, Any]:
        node = edge.get('node', {})
        for_sale_item = node.get('for_sale_item', {})

        id = for_sale_item.get('id', 'N/A')
        location = for_sale_item.get('location', {})
        # Use 'latitude' and 'longitude' for consistency with Yad2
        latitude = location.get('latitude', 'N/A')
        longitude = location.get('longitude', 'N/A')
        formatted_price_text = for_sale_item.get(
            'formatted_price', {}).get('text', 'N/A')
        share_uri = for_sale_item.get('share_uri', 'N/A')

        listing_photos_urls = []
        for photo_obj in for_sale_item.get('listing_photos', []):
            uri = photo_obj.get('image', {}).get('uri')
            if uri:
                listing_photos_urls.append(uri)

        # Extract numeric price from formatted price
        import re
        numeric_price = None
        if formatted_price_text != 'N/A':
            # Remove currency symbols and commas, extract the number
            numeric_price_str = re.sub(r'[^\d,]', '', formatted_price_text)
            numeric_price_str = numeric_price_str.replace(',', '')
            try:
                numeric_price = int(numeric_price_str)
            except ValueError:
                numeric_price = None

        # Removed 'title' field as requested
        # title = for_sale_item.get('name', 'N/A')

        return {
            "id": id,
            "latitude": latitude,
            "longitude": longitude,
            "price": numeric_price,  # Use the extracted numeric price
            "formatted_price": formatted_price_text,
            "share_uri": share_uri,
            "images": listing_photos_urls,  # Rename to match generic field
            # "title": title, # Removed
        }

    async def fetch_html(self, session: aiohttp.ClientSession, url: str) -> str:
        facebook_logger.info(f"Fetching URL: {url}")
        async with session.get(url, headers=HEADERS) as response:
            facebook_logger.info(f"Response Status: {response.status}")

            if response.status == 200:
                html_content = await response.text()
                return html_content
            elif response.status in (403, 401):
                facebook_logger.error(
                    f"Access denied (Status {response.status}). Likely requires login or blocked by anti-bot.")
                error_content = await response.text()
                facebook_logger.error(
                    f"Error page content (truncated to 2000 chars):\n{error_content[:2000]}")
                error_file = self.output_dir / "error_page_login_required.html"
                self.save_html_to_file(error_content, error_file)
                raise aiohttp.ClientResponseError(
                    request_info=response.request_info,
                    history=response.history,
                    status=response.status,
                    message=f"Access denied: {response.status}"
                )
            elif response.status == 429:
                facebook_logger.error(f"Rate limited (Status 429).")
                raise aiohttp.ClientResponseError(
                    request_info=response.request_info,
                    history=response.history,
                    status=response.status,
                    message="Rate limited"
                )
            else:
                facebook_logger.warning(f"Non-200 status: {response.status}")
                content = await response.text()
                facebook_logger.error(
                    f"Non-200 Response (Status {response.status}, truncated):\n{content[:2000]}")
                error_file = self.output_dir / \
                    f"error_page_{response.status}.html"
                self.save_html_to_file(content, error_file)
                raise aiohttp.ClientResponseError(
                    request_info=response.request_info,
                    history=response.history,
                    status=response.status,
                    message=f"Unexpected status: {response.status}"
                )

    async def fetch_html_from_share_uri(self, session: aiohttp.ClientSession, share_uri: str) -> str:
        facebook_logger.debug(f"Fetching Share URI: {share_uri}")
        async with session.get(share_uri, headers=HEADERS) as response:
            facebook_logger.debug(
                f"Response Status for Share URI: {response.status}")

            if response.status == 200:
                html_content = await response.text()
                return html_content
            elif response.status in (403, 401):
                facebook_logger.error(
                    f"Access denied for Share URI (Status {response.status}).")
                error_content = await response.text()
                facebook_logger.error(
                    f"Error page (Share URI, truncated):\n{error_content[:2000]}")
                raise aiohttp.ClientResponseError(
                    request_info=response.request_info,
                    history=response.history,
                    status=response.status,
                    message=f"Access denied: {response.status}"
                )
            elif response.status == 429:
                facebook_logger.error(
                    f"Rate limited (Status 429) for Share URI.")
                raise aiohttp.ClientResponseError(
                    request_info=response.request_info,
                    history=response.history,
                    status=response.status,
                    message="Rate limited on share URI"
                )
            else:
                facebook_logger.warning(
                    f"Non-200 status for Share URI: {response.status}")
                content = await response.text()
                facebook_logger.error(
                    f"Non-200 Response (Share URI, truncated):\n{content[:2000]}")
                raise aiohttp.ClientResponseError(
                    request_info=response.request_info,
                    history=response.history,
                    status=response.status,
                    message=f"Unexpected status on share URI: {response.status}"
                )

    async def fetch_html_from_share_uri_with_retry(self, session: aiohttp.ClientSession, share_uri: str, max_retries: int = 2) -> str:
        for attempt in range(max_retries + 1):
            try:
                return await self.fetch_html_from_share_uri(session, share_uri)
            except asyncio.TimeoutError:
                if attempt < max_retries:
                    backoff = (2 ** attempt) + random.uniform(0, 1)
                    facebook_logger.warning(
                        f"Timeout on {share_uri}, retry {attempt + 1}/{max_retries} after {backoff:.2f}s")
                    await asyncio.sleep(backoff)
                else:
                    facebook_logger.error(
                        f"Final timeout on {share_uri} after {max_retries} retries")
                    raise
        raise RuntimeError("Unreachable")

    def find_details_data_in_blob(self, parsed_json: Dict[str, Any], apartment_id: str = "unknown") -> Optional[Dict[str, Any]]:
        facebook_logger.debug(
            "Navigating JSON blob from share_uri for details...")
        for item in parsed_json.get('require', []):
            if isinstance(item, list) and len(item) >= 4 and isinstance(item[3], list) and len(item[3]) > 0:
                data_block = item[3][0]
                if (isinstance(data_block, dict) and '__bbox' in data_block and
                        isinstance(data_block['__bbox'], dict) and 'require' in data_block['__bbox']):
                    inner_require_list = data_block['__bbox']['require']
                    for inner_item in inner_require_list:
                        if (isinstance(inner_item, list) and len(inner_item) >= 4 and
                                isinstance(inner_item[3], list) and len(inner_item[3]) >= 2):
                            potential_key = inner_item[3][0]
                            potential_data_obj = inner_item[3][1]
                            if isinstance(potential_key, str) and potential_key.startswith(JSON_KEY_PREFIX_DETAILS):
                                nested_bbox = potential_data_obj.get(
                                    "__bbox", {})
                                result = nested_bbox.get("result", {})
                                data_viewer = result.get(
                                    "data", {}).get("viewer", {})
                                product_details = data_viewer.get(
                                    "marketplace_product_details_page", {})
                                if product_details:
                                    facebook_logger.debug(
                                        f"Found detailed data under key {potential_key} for apartment {apartment_id}.")

                                    # Save ONLY the marketplace_product_details_page fragment if debug is on
                                    if facebook_logger.isEnabledFor(logging.DEBUG):
                                        # Create the directory if it doesn't exist
                                        DEBUG_JSON_DIR.mkdir(exist_ok=True)
                                        safe_id = "".join(
                                            c if c.isalnum() or c in "._-" else "_" for c in str(apartment_id))
                                        # Save to the internal debug directory
                                        debug_path = DEBUG_JSON_DIR / \
                                            f"details_fragment_{safe_id}.json"
                                        with open(debug_path, 'w', encoding='utf-8') as f:
                                            json.dump(product_details, f,
                                                      ensure_ascii=False, indent=2)
                                        facebook_logger.debug(
                                            f"Saved marketplace_product_details_page fragment to {debug_path}")

                                    return product_details
        facebook_logger.debug("No detailed product data found in blob.")
        return None

    def safe_get(self, d: Any, *keys: str) -> Any:
        """Safely traverse nested dicts/lists. Returns None if any key missing or type mismatch."""
        for key in keys:
            if isinstance(d, dict) and key in d:
                d = d[key]
            else:
                return None
        return d

    def extract_additional_details(self, product_details: Dict[str, Any]) -> Dict[str, Any]:
        target = self.safe_get(product_details, 'target')
        if not isinstance(target, dict):
            raise Exception(
                "Invalid target data structure (not a dict...) in product details.")

        description = self.safe_get(
            target, 'redacted_description', 'text') or 'N/A'

        location_details = self.safe_get(
            target, 'location', 'reverse_geocode_detailed')
        address_city = self.safe_get(location_details, 'city') or 'N/A'
        street = self.safe_get(target, 'home_address', 'street') or 'N/A'
        full_address = f"{street}, {address_city}" if street != 'N/A' and address_city != 'N/A' else 'N/A'

        # Use full_address as location for unification
        location = full_address
        delivery_types = self.safe_get(target, 'delivery_types') or []
        if not isinstance(delivery_types, list):
            delivery_types = []

        unit_room_info = self.safe_get(target, 'unit_room_info') or 'N/A'

        # Extract rooms from unit_room_info if available
        import re
        rooms = 'N/A'
        if unit_room_info != 'N/A':
            # Extract room count from unit_room_info like "3 beds · 1 bath"
            room_match = re.search(r'(\d+)\s*beds?', unit_room_info)
            if room_match:
                rooms = room_match.group(1)

        comments = self.safe_get(target, 'marketplace_comments')
        comments_count = self.safe_get(comments, 'total_count') or 'N/A'

        return {
            'description': description,
            'full_address': full_address,
            'location': location,  # Add the unified location field
            'delivery_types': delivery_types,
            'unit_room_info': unit_room_info,
            'rooms': rooms,  # Add the unified rooms field
            'comments_count': comments_count,
        }

    async def fetch_and_parse_details(self, session: aiohttp.ClientSession, apartment: Dict[str, Any], apartments_list: List[Dict[str, Any]], index: int) -> Dict[str, Any]:
        share_uri = apartment.get('share_uri')
        apartment_id = apartment.get('id', 'Unknown')
        if not share_uri or share_uri == 'N/A':
            facebook_logger.warning(
                f"No valid share_uri for apartment ID {apartment_id}, skipping details.")
            return {}

        delay = random.uniform(MIN_DELAY_BETWEEN_REQUESTS,
                               MAX_DELAY_BETWEEN_REQUESTS)
        facebook_logger.debug(
            f"Delaying {delay:.2f}s before fetching details for {apartment_id}")
        await asyncio.sleep(delay)

        html_content = await self.fetch_html_from_share_uri_with_retry(session, share_uri)

        json_script_strs = self.extract_json_script_content(html_content)
        if not json_script_strs:
            raise ValueError(
                f"No JSON script content found on share_uri page for apartment ID {apartment_id}.")

        details_data = None
        for json_str in json_script_strs:
            try:
                parsed_json = json.loads(json_str)
            except json.JSONDecodeError as e:
                facebook_logger.error(
                    f"Invalid JSON in script for apartment {apartment_id}: {e}")
                facebook_logger.error(
                    f"Problematic JSON (first 500 chars): {json_str}")
                raise

            details_data = self.find_details_data_in_blob(
                parsed_json, apartment_id=apartment_id)
            if details_data:
                break

        if not details_data:
            raise ValueError(
                f"Failed to find detailed data on share_uri page for apartment ID {apartment_id}.")

        additional_details = self.extract_additional_details(details_data)
        apartments_list[index].update(additional_details)
        # Update the JSON file as details are fetched
        # self.save_apartments_to_json(
        #     apartments_list, JSON_OUTPUT_FILE_WITH_DETAILS)
        return additional_details

    async def enrich_apartments_with_details(self, session: aiohttp.ClientSession, apartments: List[Dict[str, Any]]):
        facebook_logger.debug("\n--- Fetching detailed data for each apartment in small batches ---")
        for i in range(0, len(apartments), BATCH_SIZE):
            batch = apartments[i:i + BATCH_SIZE]
            batch_indices = list(
                range(i, min(i + BATCH_SIZE, len(apartments))))
            facebook_logger.debug(
                f"\n--- Processing Batch {i // BATCH_SIZE + 1}/{(len(apartments) - 1) // BATCH_SIZE + 1} ---")

            async with asyncio.TaskGroup() as tg:
                for j, apartment in enumerate(batch):
                    idx = batch_indices[j]
                    tg.create_task(self.fetch_and_parse_details(
                        session, apartment, apartments, idx))

            facebook_logger.debug(f"--- Completed Batch {i // BATCH_SIZE + 1} ---")

    def process_json_scripts(self, json_script_strs: List[str]) -> List[Dict[str, Any]]:
        apartments = []
        found_data = False

        for i, json_str in enumerate(json_script_strs):
            facebook_logger.debug(
                f"--- Checking JSON Script {i+1}/{len(json_script_strs)} ---")
            try:
                parsed_json = json.loads(json_str)
            except json.JSONDecodeError as e:
                facebook_logger.error(f"Invalid JSON in script {i+1}: {e}")
                facebook_logger.error(
                    f"Problematic JSON string (first 500 chars): {json_str}")
                raise

            rental_edges = self.find_rental_data_in_blob(parsed_json)
            if rental_edges:
                facebook_logger.debug(
                    f"Found {len(rental_edges)} rental listings in JSON script {i+1}.")
                found_data = True
                for edge in rental_edges:
                    apartment_info = self.parse_rental_info(edge)
                    apartments.append(apartment_info)
                break
            else:
                facebook_logger.debug(
                    f"No rental data found in JSON script {i+1}.")

        if not found_data:
            raise ValueError(
                "Failed to find 'marketplace_rentals_map_view_stories' in any JSON script.")

        return apartments

    async def run(self):
        timeout = aiohttp.ClientTimeout(
            total=TOTAL_TIMEOUT,
            connect=CONNECT_TIMEOUT,
            sock_read=SOCK_READ_TIMEOUT
        )
        async with aiohttp.ClientSession(timeout=timeout) as session:
            html_content = await self.fetch_html(session, self.listings_url)
            facebook_logger.debug(f"--- Raw HTML Fetched (length: {len(html_content)}) ---")
            # self.save_html_to_file(html_content) # FOR DEBUGGING ONLY

            facebook_logger.debug("\n--- Parsing HTML for JSON Script ---")
            json_script_strs = self.extract_json_script_content(html_content)
            apartments = self.process_json_scripts(json_script_strs)

            facebook_logger.debug(f"\n--- Extracted {len(apartments)} Apartments ---")
            await self.enrich_apartments_with_details(session, apartments)

            facebook_logger.debug(f"\n--- Final data: {len(apartments)} Apartments with details ---")
            # Save the final enriched list to the JSON file
            # self.save_apartments_to_json(
            #     apartments, JSON_OUTPUT_FILE_WITH_DETAILS)
            # Return the list of apartments for potential use by the generic scraper
            return apartments


async def main():
    # Example usage with required parameters
    scraper = FacebookMarketplaceScraper(
        min_price=3000,
        max_price=10000,
        min_bedrooms=2,
        lat=32.0853,
        lng=34.7818,
        radius=5000
    )
    await scraper.run()


if __name__ == "__main__":
    asyncio.run(main())