import asyncio
import aiohttp
import logging
import json
from typing import Dict, Any
from bs4 import BeautifulSoup

# --- Import or define your headers and helper functions from the main script ---
# Assuming these are available from your main script or defined here:
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

logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)


def save_html_to_file(html_content: str, filename: str = "fetched_share_uri_page.html"):
    """Saves the fetched HTML content from the share_uri to a file."""
    with open(filename, 'w', encoding='utf-8') as f:
        f.write(html_content)
    logger.info(f"Fetched HTML from share_uri saved to {filename}")


async def fetch_html_from_share_uri(session: aiohttp.ClientSession, share_uri: str) -> str:
    """Asynchronously fetches the HTML content of the given share_uri."""
    logger.info(f"Fetching Share URI: {share_uri}")
    async with session.get(share_uri, headers=HEADERS) as response:
        logger.info(f"Response Status for Share URI: {response.status}")

        if response.status == 200:
            html_content = await response.text()
            return html_content
        elif response.status in [403, 401]:
            logger.error(
                f"Access denied for Share URI (Status {response.status}). This page likely requires login or has strong anti-bot measures.")
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
                f"Rate limited (Status 429) for Share URI. Slow down requests or use a proxy.")
            raise aiohttp.ClientResponseError(
                request_info=response.request_info,
                history=response.history,
                status=response.status,
                message=f"Rate limited: {response.status}"
            )
        else:
            logger.warning(f"Received non-200 status for Share URI: {response.status}")
            content = await response.text()
            print(f"Non-200 Response (Status {response.status}):\n{content}")
            raise aiohttp.ClientResponseError(
                request_info=response.request_info,
                history=response.history,
                status=response.status,
                message=f"Unexpected status: {response.status}"
            )


async def investigate_share_uri(share_uri: str):
    """Main function to investigate a single share_uri."""
    timeout = aiohttp.ClientTimeout(total=30) # Slightly longer timeout for individual page
    async with aiohttp.ClientSession(timeout=timeout) as session:
        try:
            html_content = await fetch_html_from_share_uri(session, share_uri)

            print("--- Raw HTML Fetched from Share URI ---")
            print(
                f"Successfully fetched HTML from Share URI, length: {len(html_content)} characters.")

            # Save the fetched HTML for investigation
            save_html_to_file(html_content)

            # Optional: Basic parsing to check for common structures
            print("\n--- Basic Analysis of Share URI HTML ---")
            soup = BeautifulSoup(html_content, 'html.parser')
            
            # Look for title
            title_tag = soup.find('title')
            title_text = title_tag.get_text(strip=True) if title_tag else "Not Found"
            print(f"Page Title: {title_text}")

            # Look for image tags (potential listing photos)
            img_tags = soup.find_all('img')
            print(f"Found {len(img_tags)} <img> tags.")

            # Look for script tags (potential JSON data)
            script_tags = soup.find_all('script')
            print(f"Found {len(script_tags)} <script> tags.")
            # Find script tags with data-sjs (like your main script)
            sjs_script_tags = soup.find_all('script', attrs={'data-sjs': True})
            print(f"Found {len(sjs_script_tags)} <script> tags with data-sjs attribute.")

            # Look for specific data attributes or divs that might contain listing details
            # This is highly dependent on Facebook's structure and might need adjustment
            # Example: Look for a div with a class that often contains the main content
            # main_content_div = soup.find('div', class_=re.compile(r'x[0-9a-f]{6}')) # Example placeholder, not real class
            # if main_content_div:
            #     print("Found potential main content div.")

        except Exception as e:
            logger.error(f"An error occurred while investigating the share URI: {e}")
            raise # Re-raise to stop execution if needed


# Example usage:
# Replace 'YOUR_SHARE_URI_HERE' with an actual share_uri from your main script's output
if __name__ == "__main__":
    # Example placeholder - you need to replace this with an actual URI from your main script's results
    example_share_uri = "https://www.facebook.com/marketplace/item/808474758570763/"
    # Example with a real URL structure (DO NOT RUN THIS AS IS WITHOUT YOUR OWN URI):
    # example_share_uri = "https://www.facebook.com/marketplace/item/12345678901234567?ref=share"

    if example_share_uri == "YOUR_SHARE_URI_HERE":
        print("Please replace 'YOUR_SHARE_URI_HERE' with an actual share_uri from your main script's output.")
    else:
        asyncio.run(investigate_share_uri(example_share_uri))