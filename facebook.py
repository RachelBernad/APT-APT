import asyncio
import aiohttp
import logging
import json
import re
from typing import List, Dict, Any

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Common browser headers to mimic a real browser request
HEADERS = {
    'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8',
    'accept-encoding': 'gzip, deflate, br',
    'accept-language': 'en-US,en;q=0.9,he;q=0.8',
    'cache-control': 'no-cache',
    'pragma': 'no-cache',
    # Note: User-Agent should ideally be rotated or updated periodically
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

# --- Parsing Functions ---


def extract_json_script_content(html_content: str) -> str:
    """
    Extracts the JSON string from the specific script tag in the HTML.
    """
    # Regex pattern to find the script tag with the specific data-sjs attribute
    # and capture its content.
    # This pattern looks for <script type="application/json" data-content-len="..."
    # data-sjs> ... </script>
    pattern = r'<script type="application/json" data-content-len="\d+" data-sjs>\s*(.*?)\s*</script>'
    match = re.search(pattern, html_content, re.DOTALL)
    if match:
        return match.group(1)
    else:
        logger.error("Script tag with data-sjs attribute not found in HTML.")
        return ""


def find_rental_data(parsed_json: Dict[str, Any]) -> List[Dict[str, Any]]:
    """
    Navigates the parsed JSON structure to find the list of rental listings.
    """
    try:
        # The structure is deep and dynamic keys are involved.
        # The require array contains elements like ["ModuleId", "method", "deps", [data]]
        # We need to find the entry related to the MarketplaceRealEstateMapStoryQuery.
        # The key part is `adp_CometMarketplaceRealEstateMapStoryQueryRelayPreloader_...`

        # Iterate through the main require list
        for item in parsed_json.get('require', []):
            # Check if it's a list with the expected structure [module, method, deps, [data]]
            if isinstance(item, list) and len(item) >= 4 and isinstance(item[3], list):
                # The actual data object is often the first element of the data list
                data_block = item[3][0]

                # Check if the data block itself has a 'require' key (indicating nested structure)
                if isinstance(data_block, dict) and 'require' in data_block:
                    for inner_item in data_block['require']:
                        if isinstance(inner_item, list) and len(inner_item) >= 4 and isinstance(inner_item[3], list):
                            inner_data_block = inner_item[3][0]
                            # Now check for the specific key containing the listings
                            if isinstance(inner_data_block, dict):
                                for key, value in inner_data_block.items():
                                    if key.startswith("adp_CometMarketplaceRealEstateMapStoryQueryRelayPreloader_"):
                                        logger.info(
                                            f"Found relevant data block under key: {key}")
                                        edges = (value.get("__bbox", {})
                                                 # Check 'complete' flag
                                                 .get("complete", True)
                                                 .get("result", {})
                                                 .get("data", {})
                                                 .get("viewer", {})
                                                 .get("marketplace_rentals_map_view_stories", {})
                                                 .get("edges", []))
                                        return edges
    except (KeyError, TypeError, IndexError) as e:
        logger.error(f"Error navigating JSON structure: {e}")
        return []
    logger.warning("Rental data structure not found in JSON.")
    return []


def parse_rental_info(edge: Dict[str, Any]) -> Dict[str, Any]:
    """
    Parses a single rental listing edge into a simplified dictionary.
    """
    node = edge.get('node', {})
    for_sale_item = node.get('for_sale_item', {})

    # Extract relevant fields, handling potential missing keys
    id = for_sale_item.get('id', 'N/A')
    location = for_sale_item.get('location', {})
    latitude = location.get('latitude', 'N/A')
    longitude = location.get('longitude', 'N/A')

    # Attempt to find price - common location in Facebook Marketplace GraphQL responses
    list_price = for_sale_item.get('list_price', {})
    price_amount = list_price.get('amount', 'N/A')
    price_currency = list_price.get('currency', 'N/A')

    seller_id = for_sale_item.get('seller', {}).get(
        'id', 'N/A') if for_sale_item.get('seller') else 'N/A'
    seller_name = for_sale_item.get('seller', {}).get(
        'name', 'N/A') if for_sale_item.get('seller') else 'N/A'
    title = for_sale_item.get('name', 'N/A')  # Assuming 'name' is the title
    url = for_sale_item.get('url', 'N/A')  # Assuming a URL field exists
    image_url = for_sale_item.get('cover_photo', {}).get('image', {}).get(
        'uri', 'N/A') if for_sale_item.get('cover_photo') else 'N/A'
    bedrooms = for_sale_item.get('num_bedrooms', 'N/A')
    bathrooms = for_sale_item.get('num_bathrooms', 'N/A')
    is_sold = for_sale_item.get('is_sold', 'N/A')

    return {
        "id": id,
        "latitude": latitude,
        "longitude": longitude,
        "price_amount": price_amount,
        "price_currency": price_currency,
        "seller_id": seller_id,
        "seller_name": seller_name,
        "title": title,
        "url": url,
        "image_url": image_url,
        "bedrooms": bedrooms,
        "bathrooms": bathrooms,
        "is_sold": is_sold
    }


async def fetch_marketplace_html(session, url):
    """
    Asynchronously fetches the HTML content of the given URL using the provided aiohttp session.
    Includes common headers to look like a browser request.
    """
    try:
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
                return f"Error Page (Status {response.status}):\n{error_content}..."
            elif response.status == 429:
                logger.error(
                    f"Rate limited (Status 429). Slow down requests or use a proxy.")
                return f"Rate Limited (Status 429)"
            else:
                logger.warning(f"Received non-200 status: {response.status}")
                content = await response.text()
                return f"Non-200 Response (Status {response.status}):\n{content}..."

    except asyncio.TimeoutError:
        logger.error(f"Request timed out for URL: {url}")
        return f"Error: Request timed out for {url}"
    except aiohttp.ClientError as e:
        logger.error(f"Client error occurred while fetching {url}: {e}")
        return f"Error: Client error for {url} - {e}"
    except Exception as e:
        logger.error(f"An unexpected error occurred while fetching {url}: {e}")
        return f"Error: Unexpected error for {url} - {e}"


async def main():
    """
    Main async function to fetch HTML, extract JSON, and parse rental data.
    """
    url = "https://www.facebook.com/marketplace/telaviv/propertyrentals?maxPrice=10000&minBedrooms=2&exact=false&latitude=32.0778&longitude=34.7677&radius=3"

    timeout = aiohttp.ClientTimeout(total=20)
    async with aiohttp.ClientSession(timeout=timeout) as session:
        html_content = await fetch_marketplace_html(session, url)

        if html_content and not html_content.startswith("Error"):
            print("--- Raw HTML Fetched ---")
            print(
                f"Successfully fetched HTML, length: {len(html_content)} characters.")

            # --- Parsing Section ---
            print("\n--- Parsing HTML for JSON Script ---")
            json_script_str = extract_json_script_content(html_content)

            if json_script_str:
                print(
                    f"Found JSON script content, length: {len(json_script_str)} characters.")
                try:
                    parsed_json = json.loads(json_script_str)
                    logger.info(
                        "Successfully parsed JSON content from script tag.")

                    rental_edges = find_rental_data(parsed_json)
                    logger.info(
                        f"Found {len(rental_edges)} rental listings in JSON.")

                    apartments = []
                    for edge in rental_edges:
                        apartment_info = parse_rental_info(edge)
                        apartments.append(apartment_info)

                    print(f"\n--- Extracted {len(apartments)} Apartments ---")
                    for i, apt in enumerate(apartments):
                        print(f"\n--- Apartment {i+1} ---")
                        for key, value in apt.items():
                            print(f"  {key}: {value}")

                except json.JSONDecodeError as e:
                    logger.error(f"Failed to decode JSON from script tag: {e}")
                except Exception as e:
                    logger.error(
                        f"An unexpected error occurred during parsing: {e}")
            else:
                print("Failed to extract JSON script content from HTML.")
                print(
                    "The page might be dynamic (JS rendered) or the structure might differ.")
        else:
            print(f"--- Failed to Fetch HTML ---")
            print(html_content)

if __name__ == "__main__":
    asyncio.run(main())
