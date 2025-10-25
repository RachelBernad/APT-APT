import asyncio
import datetime
import aiohttp
import logging
import json
from typing import List, Dict, Any
from bs4 import BeautifulSoup

logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)

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


def save_html_to_file(html_content: str, filename: str = "fetched_page.html"):
    """Saves the fetched HTML content to a file."""
    with open(filename, 'w', encoding='utf-8') as f:
        f.write(html_content)
    logger.info(f"Fetched HTML saved to {filename}")


def save_apartments_to_json(apartments: List[Dict[str, Any]], filename: str = "apartments.json"):
    """Saves the list of apartments to a JSON file."""
    with open(filename, 'w', encoding='utf-8') as f:
        json.dump(apartments, f, ensure_ascii=False, indent=2)
    logger.info(f"Extracted apartments saved to {filename}")


def extract_json_script_content(html_content: str) -> List[str]:
    """Extracts ALL JSON strings from script tags with data-sjs attribute using BeautifulSoup."""
    soup = BeautifulSoup(html_content, 'html.parser')
    script_tags = soup.find_all('script', attrs={'data-sjs': True})

    json_strings = []
    for tag in script_tags:
        content = tag.string
        if content:
            json_strings.append(content.strip())

    if json_strings:
        logger.info(
            f"Found {len(json_strings)} potential JSON script tags using BeautifulSoup.")
        return json_strings
    else:
        logger.error(
            "No script tags with data-sjs attribute found in HTML using BeautifulSoup.")
        return []


def find_rental_data_in_blob(parsed_json: Dict[str, Any]) -> List[Dict[str, Any]]:
    """Navigates a SINGLE parsed JSON blob to find the list of rental listings."""
    logger.debug("Starting to navigate JSON blob...")
    for item in parsed_json.get('require', []):
        logger.debug(
            f"Processing outer require item: {type(item)}, {len(item) if isinstance(item, list) else 'N/A'}")
        # Check outer require item structure: [str, str, null, [dict]]
        if isinstance(item, list) and len(item) >= 4 and isinstance(item[3], list) and len(item[3]) > 0:
            data_block = item[3][0]  # This is the first dict inside the list
            logger.debug(f"Found data_block: {type(data_block)}")

            # Check if data_block has __bbox -> require
            if (isinstance(data_block, dict) and '__bbox' in data_block and
                    isinstance(data_block['__bbox'], dict) and 'require' in data_block['__bbox']):

                logger.debug("Found inner require structure via __bbox.")
                # Navigate to the inner require list
                inner_require_list = data_block['__bbox']['require']

                for inner_item in inner_require_list:
                    logger.debug(
                        f"Processing inner require item: {type(inner_item)}, {len(inner_item) if isinstance(inner_item, list) else 'N/A'}")
                    # Check inner require item structure: [str, str, [], [str, dict]] or similar
                    # The key part is that inner_item[3] should be a list containing the adp_... key and its data object
                    if (isinstance(inner_item, list) and len(inner_item) >= 4 and
                            isinstance(inner_item[3], list) and len(inner_item[3]) >= 2):

                        # The first element of the list is the key string
                        potential_key = inner_item[3][0]
                        # The second element of the list is the data object
                        potential_data_obj = inner_item[3][1]

                        logger.debug(
                            f"Checking potential key: {potential_key} (type: {type(potential_key)})")
                        # Check if the key string matches the pattern
                        if isinstance(potential_key, str) and potential_key.startswith("adp_CometMarketplaceRealEstateMapStoryQueryRelayPreloader_"):
                            logger.info(
                                f"Checking data block under key: {potential_key}")
                            # The *value* associated with the key (potential_data_obj) contains the __bbox structure
                            logger.debug(
                                f"Data object for {potential_key}: {type(potential_data_obj)}")
                            nested_bbox = potential_data_obj.get("__bbox", {})
                            logger.debug(f"Nested __bbox: {type(nested_bbox)}")
                            result = nested_bbox.get("result", {})
                            logger.debug(
                                f"Result: {type(result)}")
                            data_viewer = result.get(
                                "data", {}).get("viewer", {})
                            logger.debug(f"Data viewer: {type(data_viewer)}")
                            stories_data = data_viewer.get(
                                "marketplace_rentals_map_view_stories", {})

                            if stories_data:
                                logger.info(
                                    f"Found 'marketplace_rentals_map_view_stories' in blob under key {potential_key}.")
                                edges = stories_data.get("edges", [])
                                logger.debug(f"Found {len(edges)} edges.")
                                return edges
                            else:
                                logger.debug(
                                    f"Key {potential_key} did not contain 'marketplace_rentals_map_view_stories'.")
    logger.debug("Did not find rental data in this blob.")
    return []


def parse_rental_info(edge: Dict[str, Any]) -> Dict[str, Any]:
    """Parses a single rental listing edge into a simplified dictionary."""
    node = edge.get('node', {})
    for_sale_item = node.get('for_sale_item', {})

    id = for_sale_item.get('id', 'N/A')
    location = for_sale_item.get('location', {})
    latitude = location.get('latitude', 'N/A')
    longitude = location.get('longitude', 'N/A')

    # Extract formatted_price text
    formatted_price_data = for_sale_item.get('formatted_price', {})
    formatted_price_text = formatted_price_data.get('text', 'N/A')

    # Extract share_uri
    share_uri = for_sale_item.get('share_uri', 'N/A')

    # Extract listing photos URLs
    listing_photos_data = for_sale_item.get('listing_photos', [])
    listing_photos_urls = []
    for photo_obj in listing_photos_data:
        image_data = photo_obj.get('image', {})
        uri = image_data.get('uri', None)
        if uri:
            listing_photos_urls.append(uri)

    seller_id = for_sale_item.get('seller', {}).get(
        'id', 'N/A') if for_sale_item.get('seller') else 'N/A'
    seller_name = for_sale_item.get('seller', {}).get(
        'name', 'N/A') if for_sale_item.get('seller') else 'N/A'
    title = for_sale_item.get('name', 'N/A')
    url = for_sale_item.get('url', 'N/A')
    image_url = for_sale_item.get('cover_photo', {}).get('image', {}).get(
        'uri', 'N/A') if for_sale_item.get('cover_photo') else 'N/A'
    bedrooms = for_sale_item.get('num_bedrooms', 'N/A')
    bathrooms = for_sale_item.get('num_bathrooms', 'N/A')
    is_sold = for_sale_item.get('is_sold', 'N/A')

    return {
        "id": id,
        "latitude": latitude,
        "longitude": longitude,
        "formatted_price": formatted_price_text,
        "share_uri": share_uri,
        "listing_photos_urls": listing_photos_urls,
        "seller_id": seller_id,
        "seller_name": seller_name,
        "title": title,
        "url": url,
        "image_url": image_url,
        "bedrooms": bedrooms,
        "bathrooms": bathrooms,
        "is_sold": is_sold
    }


async def fetch_html(session: aiohttp.ClientSession, url: str) -> str:
    """Asynchronously fetches the HTML content of the given URL."""
    logger.info(f"Fetching URL: {url}")
    async with session.get(url, headers=HEADERS) as response:
        logger.info(f"Response Status: {response.status}")

        if response.status == 200:
            html_content = await response.text()
            return html_content
        elif response.status == 403 or response.status == 401:
            logger.error(
                f"Access denied (Status {response.status}). This page likely requires login or has strong anti-bot measures.")
            error_content = await response.text()
            print(f"Error Page (Status {response.status}):\n{error_content}")
            raise aiohttp.ClientResponseError(
                request_info=response.request_info,
                history=response.history,
                status=response.status,
                message=f"Access denied: {response.status}"
            )
        elif response.status == 429:
            logger.error(
                f"Rate limited (Status 429). Slow down requests or use a proxy.")
            raise aiohttp.ClientResponseError(
                request_info=response.request_info,
                history=response.history,
                status=response.status,
                message=f"Rate limited: {response.status}"
            )
        else:
            logger.warning(f"Received non-200 status: {response.status}")
            content = await response.text()
            print(f"Non-200 Response (Status {response.status}):\n{content}")
            raise aiohttp.ClientResponseError(
                request_info=response.request_info,
                history=response.history,
                status=response.status,
                message=f"Unexpected status: {response.status}"
            )


def process_json_scripts(json_script_strs: List[str]) -> List[Dict[str, Any]]:
    """Processes a list of JSON script strings to find rental data."""
    apartments = []
    found_data = False

    for i, json_str in enumerate(json_script_strs):
        print(f"--- Checking JSON Script {i+1}/{len(json_script_strs)} ---")
        parsed_json = json.loads(json_str)

        rental_edges = find_rental_data_in_blob(parsed_json)
        if rental_edges:
            logger.info(
                f"Found {len(rental_edges)} rental listings in JSON script {i+1}.")
            found_data = True
            for edge in rental_edges:
                apartment_info = parse_rental_info(edge)
                apartments.append(apartment_info)
            break
        else:
            logger.debug(f"No rental data found in JSON script {i+1}.")

    if not found_data:
        raise ValueError(
            "Failed to find 'marketplace_rentals_map_view_stories' in any of the JSON script tags.")

    return apartments


async def main():
    url = "https://www.facebook.com/marketplace/telaviv/propertyrentals?minPrice=2&maxPrice=10000&minBedrooms=3&exact=false&latitude=32.0778&longitude=34.7677&radius=3"

    timeout = aiohttp.ClientTimeout(total=20)
    async with aiohttp.ClientSession(timeout=timeout) as session:
        html_content = await fetch_html(session, url)

        print("--- Raw HTML Fetched ---")
        print(
            f"Successfully fetched HTML, length: {len(html_content)} characters.")

        # Save the fetched HTML for investigation
        save_html_to_file(html_content)

        print("\n--- Parsing HTML for JSON Script using BeautifulSoup ---")
        json_script_strs = extract_json_script_content(html_content)

        if not json_script_strs:
            raise ValueError(
                "Failed to extract any JSON script content from HTML using BeautifulSoup.")

        apartments = process_json_scripts(json_script_strs)

        print(f"\n--- Extracted {len(apartments)} Apartments ---")
        for i, apt in enumerate(apartments):
            print(f"\n--- Apartment {i+1} ---")
            for key, value in apt.items():
                print(f"  {key}: {value}")

        # Save the extracted apartments to a JSON file
        save_apartments_to_json(apartments)


if __name__ == "__main__":
    asyncio.run(main())
