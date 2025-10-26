import asyncio
import aiohttp
import hashlib
import json
import os
import datetime
import random
import time


# BOT_TOKEN = ''
# BOT_CHATID = ''

APT_FILE = r'.\yad2_apts.json'
BASE_URL = 'https://www.yad2.co.il/realestate/_next/data/VNJEK8g5hoH41L9F3_H99/rent.json?minPrice=4000&maxPrice=10000&minRooms=2.5&maxRooms=4&topArea=2&area=1&city={ct}&page={pg}'
CITIES = [
    5000,  # Tel Aviv
]
MD5 = 0
INFO = 1

LINK = 'https://www.yad2.co.il/item/{id}'
APARTMENT_PAGE_URL = 'https://www.yad2.co.il/realestate/item/{token}'

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

TELEGRAM_HEADERS = {
    'User-Agent': USER_AGENT,
    'Accept': '*/*',
    'Accept-Language': 'en-US,en;q=0.9',
}

async def process_page(page):
    processed_items = []
    feed_data = page['pageProps']['feed']

    for item in feed_data['private']:
        processed_items.append(process_item(item))

    return processed_items


def get_field(dicts, field):
    for d in dicts:
        if d['key'] == field:
            return str(d['value'])
    return ''


def process_item(item):
    item_info = {}

    # Extract address details
    address = item['address']
    item_info['city'] = address['city']['text']
    item_info['street'] = address['street']['text'] if 'street' in address and address['street'] else ''

    # Extract price
    item_info['price'] = item['price']

    # Extract ID (token is used as a unique identifier)
    item_info['id'] = item['token']

    # Extract the actual apartment page URL
    item_info['apartment_page_url'] = APARTMENT_PAGE_URL.format(
        token=item['token'])

    # Extract additional details if available
    additional_details = item.get('additionalDetails', {})
    item_info['rooms'] = str(additional_details.get('roomsCount', ''))
    item_info['size'] = str(additional_details.get('squareMeter', ''))

    # Extract metadata
    metadata = item.get('metaData', {})
    item_info['images'] = metadata.get('images', [])

    # Extract tags if available
    tags = item.get('tags', [])
    item_info['tags'] = [tag['name'] for tag in tags]

    # Extract floor if available in address.house
    house_details = address.get('house', {})
    item_info['floor'] = str(house_details.get('floor', ''))

    md5 = get_md5(item_info)
    return [md5, item_info]


async def get_page_data(pageNumber, city):
    # Add a random delay between requests to appear more human-like
    await asyncio.sleep(random.uniform(2.5, 6.0))
    url = BASE_URL.format(ct=city, pg=pageNumber)

    # Create a new session for each request to avoid connection reuse issues
    timeout = aiohttp.ClientTimeout(total=30)  # 30 second timeout
    async with aiohttp.ClientSession(timeout=timeout) as session:
        async with session.get(url, headers=DEFAULT_HEADERS) as response:
            response.raise_for_status()  # Raise an exception for bad status codes
            page_data = await response.json()

            # Log the number of apartments found on this page
            private_ads = page_data['pageProps']['feed'].get('private', [])
            commercial_ads = page_data['pageProps']['feed'].get(
                'commercial', [])
            total_ads_on_page = len(private_ads) + len(commercial_ads)

            print(
                f"Fetched page {pageNumber} for city {city}: Found {total_ads_on_page} apartments")

            return page_data


def get_md5(thing):
    return hashlib.md5(str(thing).encode()).hexdigest()


def get_hashes(path):
    if os.path.exists(path):
        with open(path, 'r', encoding='utf-8') as hashes_inp:
            return json.load(hashes_inp)
    else:
        return {}


async def get_current():
    current = []
    total_expected = 0

    for city in CITIES:
        first_page = await get_page_data(1, city)

        # Extract total pages and total items from the new JSON structure
        # Located under pageProps.pagination
        pagination_data = first_page['pageProps']['feed']['pagination']
        page_count = pagination_data['totalPages']
        # Accumulate total from all cities
        total_expected += pagination_data['total']

        current.extend(await process_page(first_page))

        # Handle the case where there is only one page
        if page_count > 1:
            tasks = []
            for pageNumber in range(2, page_count + 1):
                tasks.append(get_page_data(pageNumber, city))

            # Fetch remaining pages concurrently
            pages = await asyncio.gather(*tasks)
            for page in pages:
                current.extend(await process_page(page))

    # Check if the number of fetched items matches the expected total
    if len(current) != total_expected:
        print(
            f"WARNING: Fetched {len(current)} items, but expected {total_expected} according to pagination.")
        # Optionally, you could raise an exception here instead of just printing a warning
        # raise ValueError(f"Fetched {len(current)} items, but expected {total_expected} according to pagination.")

    return current


def merge_dicts(dicts):
    all = {}
    for d in dicts:
        all.update(d)
    return all


async def telegram_bot_sendtext(bot_message):
    send_text = f'https://api.telegram.org/bot{BOT_TOKEN}/sendMessage?chat_id={BOT_CHATID}&parse_mode=Markdown&text={bot_message}'

    # Create a new session for each request to avoid connection reuse issues
    timeout = aiohttp.ClientTimeout(total=30)  # 30 second timeout
    async with aiohttp.ClientSession(timeout=timeout) as session:
        async with session.get(send_text, headers=TELEGRAM_HEADERS) as response:
            return await response.json()


def format_message_new(item):
    link = LINK.format(id=item['id'])
    # Add the actual apartment page URL to the message
    apartment_link = item['apartment_page_url']
    return f'''Address: {item['street']} {item['city']}
Price: {item['price']}
Rooms: {item['rooms']}
Size: {item['size']}
Link: {link}
Apartment Page: {apartment_link}
'''


def format_message_updated(item):
    link = LINK.format(id=item['id'])
    # Add the actual apartment page URL to the message
    apartment_link = item['apartment_page_url']
    return f'''Address: {item['street']} {item['city']}
Price: {item['price']}
Rooms: {item['rooms']}
Size: {item['size']}
Link: {link}
Apartment Page: {apartment_link}
Updated: True
'''


async def send_items_telegram_new(items):
    for id, item in items.items():
        msg = format_message_new(item[INFO])
        await telegram_bot_sendtext(msg)


async def send_items_telegram_updated(items):
    for id, item in items.items():
        msg = format_message_updated(item[INFO])
        await telegram_bot_sendtext(msg)


async def main():
    old = get_hashes(APT_FILE)
    current = await get_current()
    new = {}
    updated = {}
    for item in current:
        id = item[INFO]['id']
        if id in old.keys():
            if old[id][MD5] != item[MD5]:
                updated[id] = item
        else:
            new[id] = item

    all = merge_dicts([old, new, updated])

    # Write the data to a JSON file with UTF-8 encoding
    with open(APT_FILE, 'w', encoding='utf-8') as out:
        json.dump(all, out, ensure_ascii=False, indent=2)

    now = str(datetime.datetime.now()).split('.')[0]
    print(f'{now}: New: {len(new)} | Updated: {len(updated)}')

    # await send_items_telegram_new(new)
    # await send_items_telegram_updated(updated)


if __name__ == '__main__':
    while True:
        asyncio.run(main())
        time.sleep(60 * 15)  # Every 15 minutes check for new apartments
