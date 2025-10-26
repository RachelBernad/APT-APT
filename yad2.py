import asyncio
import aiohttp
import hashlib
import json
from pathlib import Path
import datetime
import random
from typing import Dict, List, Any


# --- Configuration ---
APT_FILE = Path('./yad2_apts.json')
BASE_URL_TEMPLATE = 'https://www.yad2.co.il/realestate/_next/data/VNJEK8g5hoH41L9F3_H99/rent.json?minPrice=4000&maxPrice=10000&minRooms=2.5&maxRooms=4&topArea=2&area=1&city={ct}&page={pg}'
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

# --- Timing ---
MIN_DELAY = 2.5
MAX_DELAY = 6.0
REQUEST_TIMEOUT = 30  # seconds


class ApartmentScraper:
    def __init__(self, apt_file: Path = APT_FILE):
        self.apt_file = apt_file

    def _process_item(self, item: Dict[str, Any]) -> Dict[str, Any]:
        processed_item = {}

        # Extract address details - accessing 'text' field from nested objects
        address = item.get('address', {})
        processed_item['city'] = address.get('city', {}).get('text', '')
        processed_item['street'] = address.get('street', {}).get('text', '')

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
        return processed_item

    async def _get_page_data(self, page_number: int, city: int) -> Dict[str, Any]:
        await asyncio.sleep(random.uniform(MIN_DELAY, MAX_DELAY))
        url = BASE_URL_TEMPLATE.format(ct=city, pg=page_number)

        timeout = aiohttp.ClientTimeout(total=REQUEST_TIMEOUT)
        async with aiohttp.ClientSession(timeout=timeout) as session:
            async with session.get(url, headers=DEFAULT_HEADERS) as response:
                response.raise_for_status()
                page_data = await response.json()

                # Log the number of apartments found on this page
                feed_data = page_data.get('pageProps', {}).get('feed', {})
                private_ads = feed_data.get('private', [])
                commercial_ads = feed_data.get('commercial', [])
                total_ads_on_page = len(private_ads) + len(commercial_ads)

                print(
                    f"Fetched page {page_number} for city {city}: Found {total_ads_on_page} apartments")

                return page_data

    def _get_md5(self, thing: Any) -> str:
        return hashlib.md5(str(thing).encode()).hexdigest()

    def _load_existing_data(self) -> Dict[str, Dict[str, Any]]:
        if self.apt_file.exists():
            with self.apt_file.open('r', encoding='utf-8') as f:
                return json.load(f)
        else:
            return {}

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
            pagination_data = first_page.get('pageProps', {}).get('feed', {}).get('pagination', {})
            page_count = pagination_data.get('totalPages', 0)
            total_expected += pagination_data.get('total', 0)

            current.extend(await self._process_page(first_page))

            # Handle the case where there is only one page
            if page_count > 1:
                tasks = []
                for page_number in range(2, page_count + 1):
                    tasks.append(self._get_page_data(page_number, city))

                # Fetch remaining pages concurrently
                pages = await asyncio.gather(*tasks)
                for page in pages:
                    current.extend(await self._process_page(page))

        # Check if the number of fetched items matches the expected total
        if len(current) != total_expected:
            print(
                f"WARNING: Fetched {len(current)} items, but expected {total_expected} according to pagination.")

        return current

    def _update_or_add_items(self, old_by_md5: Dict[str, Dict[str, Any]], current: List[Dict[str, Any]]) -> tuple[Dict[str, Dict[str, Any]], Dict[str, Dict[str, Any]]]:
        new_items = {}
        updated_items = {}

        for current_item in current:
            current_md5 = current_item['md5']
            current_id = current_item['id']

            if current_md5 in old_by_md5:
                existing_item = old_by_md5[current_md5]
                if current_id != existing_item['id']:
                    # Update the ID and URL in the existing item
                    existing_item['id'] = current_id
                    existing_item['apartment_page_url'] = current_item['apartment_page_url']
                    updated_items[current_md5] = existing_item
            else:
                new_items[current_md5] = current_item

        return new_items, updated_items

    def _save_data(self, final_all_md5_based: Dict[str, Dict[str, Any]]) -> None:
        with self.apt_file.open('w', encoding='utf-8') as out:
            json.dump(final_all_md5_based, out, ensure_ascii=False, indent=2)

    async def run(self) -> None:
        old_data = self._load_existing_data()

        # Create a mapping from MD5 hash to the full item data for comparison
        old_by_md5 = {item['md5']: item for item in old_data.values()}

        current = await self.get_current()

        new_items, updated_items = self._update_or_add_items(
            old_by_md5, current)

        # Reconstruct the final dictionary
        final_all_md5_based = {}
        for md5_key, stored_item in old_by_md5.items():
            if md5_key not in new_items and md5_key not in updated_items:
                final_all_md5_based[md5_key] = stored_item

        final_all_md5_based.update(new_items)
        final_all_md5_based.update(updated_items)

        self._save_data(final_all_md5_based)

        now = str(datetime.datetime.now()).split('.')[0]
        print(f'{now}: New: {len(new_items)} | Updated: {len(updated_items)}')


async def main() -> None:
    scraper = ApartmentScraper()
    await scraper.run()


if __name__ == '__main__':
    asyncio.run(main())