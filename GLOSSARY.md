# APT-APT apartment bot

A Telegram bot that watches apartment sources (Yad2 and Tel Aviv Facebook groups) and sends each user only the new
apartments matching their saved search.

## Sources and delivery

**Monitor**:
One user's saved search: filters plus the areas it watches. Every delivery is per Monitor.
_Avoid_: saved search (in user-facing text), subscription, alert

**Listing**:
A Yad2 apartment ad: structured data from the source.
_Avoid_: ad, item

**Post**:
A Facebook group post: free text written by a person, possibly cross-posted to several groups. One Post is one content
hash, however many groups it appeared in.
_Avoid_: listing (for Facebook), message

**Taster**:
The few newest items sent once when a Monitor starts, before it switches to only-new.
_Avoid_: backfill (in user-facing text), sample

## AI analysis

**Listing Facts**:
The fixed set of apartment attributes the AI reads out of one Post (kind, address, price, rooms, ממ״ד, floor…). Each
yes/no fact is true, false (explicitly denied) or unknown (not mentioned).
_Avoid_: analysis result, AI fields, metadata

**Post kind**:
What a Post offers: rent (a whole apartment), roommate (a room in a shared apartment), sublet (short term), sale (an
apartment for sale), seeking (the poster is looking for a home), or not a listing (items, services, spam).
_Avoid_: is_ad, ad flag, category

**Not a listing**:
A Post that offers no home at all: furniture or other items, movers, handymen, spam. An apartment ad by a broker is NOT
this.
_Avoid_: ad (ambiguous: apartment ads are ads too), spam (too narrow)

**Summary**:
The compact Hebrew rendering of a Post's Listing Facts, shown above the raw Post text.
_Avoid_: preview, card

**Analysis batch**:
Up to 15 Posts sent to the AI together in one request, each identified by its position in the batch.
_Avoid_: chunk, bulk request
