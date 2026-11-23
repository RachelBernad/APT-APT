# Tel Aviv-Yafo neighborhood catalog

[← back to index](./README.md)

Because Yad2 has **no hood-enumeration endpoint** ([autocomplete](./address-autocomplete.md) caps at
~5 results), this catalog was **harvested from the [map feed](./listings-map-feed.md)** by tiling
`bBox` boxes over the city and collecting distinct `neighborhood.text` where
`city.text == "תל אביב יפו"`.

- **Source:** 2,000 markers across 10 `bBox` tiles, filtered to Tel Aviv-Yafo (`city=5000`).
- **Result:** **56 distinct hoods** (below).
- **Caveat:** harvest reflects hoods that currently have rental listings — a hood with zero active
  rentals at harvest time may be missing. Re-harvest periodically. `bBox` spills across city lines,
  so the `city.text` filter is essential (spillover seen: Ramat Gan, Holon, Bat Yam, Givatayim, Azor).
- **Matching:** the bot matches hoods **locally by normalized name** (markers carry no hood id), so
  these names are the join key — keep them verbatim.

## Sub-neighborhoods

Yad2 splits some quarters into finer hoods; this is the "sub-neighborhood" structure the bot must
expose for navigation:

- **הצפון הישן** (Old North) → `הצפון הישן - צפון`, `הצפון הישן - דרום`
- **הצפון החדש** (New North) → `הצפון החדש - צפון`, `הצפון החדש - כיכר המדינה`, `הצפון החדש - דרום`
- Several entries are **comma-merged micro-hoods** Yad2 treats as one polygon, e.g.
  `אזורי חן, גימל החדשה` · `הגוש הגדול, רמת אביב החדשה, נופי ים` · `נווה אליעזר וכפר שלם מזרח`.

## Full list (56, verified)

```
אורות
אזורי חן, גימל החדשה
בבלי
ביצרון ורמת ישראל
גבעת הרצל, אזור המלאכה יפו
גלילות
גני צהלה, רמות צהלה
גני שרונה, קרית הממשלה
הגוש הגדול, רמת אביב החדשה, נופי ים
הדר יוסף
המשתלה
הצפון החדש - דרום
הצפון החדש - כיכר המדינה
הצפון החדש - צפון
הצפון הישן - דרום
הצפון הישן - צפון
התקוה, בית יעקב, נווה צה"ל
יד אליהו
יפו ד', גבעת התמרים
יפו העתיקה
כוכב הצפון
כרם התימנים
לב תל אביב, לב העיר צפון
לבנה
מונטיפיורי, הרכבת
מכללת תל אביב יפו, דקר
נאות אפקה א'
נאות אפקה ב'
נווה אביבים
נווה אליעזר וכפר שלם מזרח
נווה ברבור, כפר שלם מערב
נווה גולן, יפו ג'
נווה חן
נווה עופר, תל כביר
נווה צדק
נווה שאנן
נווה שרת
נחלת יצחק
ניר אביב
עג'מי, גבעת העליה
עזרא, הארגזים
פלורנטין
צהלון, שיכוני חסכון
צמרות איילון, פארק צמרת
צפון יפו, המושבה האמריקאית-גרמנית
קרית שלום
רביבים
רמת אביב
רמת אביב ג'
רמת החייל
רמת הטייסים
שיכון דן, נווה דן
שפירא
תכנית ל', למד
תל ברוך צפון
תל חיים
```

## Proposed quarter grouping (curation aid — verify before shipping)

For a tap-to-browse UI, the flat 56 are best grouped into ~6 geographic quarters. This grouping is a
**hand-curated starting point** (Yad2 does not return quarter membership), refine as needed:

| Quarter | Hoods |
|---------|-------|
| **צפון (North / Ramat Aviv)** | רמת אביב · רמת אביב ג' · נווה אביבים · הגוש הגדול, רמת אביב החדשה, נופי ים · אזורי חן, גימל החדשה · נאות אפקה א' · נאות אפקה ב' · תל ברוך צפון · תל חיים · גלילות · המשתלה · גני צהלה, רמות צהלה |
| **צפון-מזרח (Northeast)** | רמת החייל · הדר יוסף · כוכב הצפון · בבלי · נווה שרת · נחלת יצחק · רמת הטייסים · לבנה |
| **הצפון החדש / הישן** | הצפון הישן - צפון · הצפון הישן - דרום · הצפון החדש - צפון · הצפון החדש - כיכר המדינה · הצפון החדש - דרום |
| **מרכז (Center / Lev Ha'ir)** | לב תל אביב, לב העיר צפון · כרם התימנים · מונטיפיורי, הרכבת · גני שרונה, קרית הממשלה · מכללת תל אביב יפו, דקר |
| **דרום (South: Neve Tzedek→Florentin→Shapira & east)** | נווה צדק · פלורנטין · שפירא · נווה שאנן · יד אליהו · ביצרון ורמת ישראל · התקוה, בית יעקב, נווה צה"ל · עזרא, הארגזים · קרית שלום · נווה אליעזר וכפר שלם מזרח · נווה ברבור, כפר שלם מערב · נווה עופר, תל כביר · צהלון, שיכוני חסכון · תכנית ל', למד · ניר אביב · אורות · רביבים · נווה חן · שיכון דן, נווה דן |
| **יפו (Jaffa)** | יפו העתיקה · עג'מי, גבעת העליה · צפון יפו, המושבה האמריקאית-גרמנית · יפו ד', גבעת התמרים · נווה גולן, יפו ג' · גבעת הרצל, אזור המלאכה יפו · צמרות איילון, פארק צמרת |

## Re-harvesting

Run `python scripts/harvest_locations.py --skip-sweep` — it re-tiles the same 10 hard-coded
`TLV_TILES` bounding boxes (`region=3&city=5000&bBox=<lat1,lng1,lat2,lng2>&zoom=14`, covering
32.01–32.16 N × 34.74–34.85 E) and prints the current distinct `neighborhood.text` list filtered to
`city.text == "תל אביב יפו"` for review. It deliberately does **not** overwrite `data/hoods/5000.json`
automatically (the quarter grouping is hand-curated) — diff the printed list against the existing
file and update the quarters by hand. See [harvesting-all-areas.md](./harvesting-all-areas.md) for
the full method + script details.
