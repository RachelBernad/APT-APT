# Listing-style posts

Last verified: 2026-11-24

Apartment groups are plain-text Story posts. **No structured price/rooms/address fields were observed** in the feed
payload: price ("2500", "5000 ש'ח"), rooms ("3-room", "4.5 חדרים"), street, entry date and phone numbers are all free
text in `message.text`. Extraction is a text-parsing job downstream (regex/LLM), not part of the browser layer.
Facebook's "sell something" posts (`GroupCommerceProductItem`, with price/location fields) didn't appear in these
groups. **Unverified.**
