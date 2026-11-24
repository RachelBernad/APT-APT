"""Best-effort hints from free post text: prices, rooms, phone numbers. Pure; never raises.

Apartment posts carry no structured fields (api_docs/posts/sale-style-posts.md), so these are heuristics
for filtering, not facts. Downstream parsing (regex/LLM) should treat them as hints only.
"""
from __future__ import annotations

import re

from aptbot.scrapers.facebook_generic_browser.models import Hints

# 3,500 ₪ | ₪3500 | 3500 ש"ח | 3500 שח | 3,500 שקל | NIS 3500 | 3500 nis | "price: 5000" | "מחיר: 5000"
_CUR = r"(?:₪|ש[\"'״]?ח|שקל(?:ים)?|nis|ils)"
_NUM = r"(?<![\d,.])(\d{1,3}(?:[,.]\d{3})+|\d{3,6})(?![\d,.]*\d)"
_PRICE = [
    re.compile(rf"{_NUM}\s*{_CUR}", re.I),
    re.compile(rf"{_CUR}\s*{_NUM}", re.I),
    re.compile(rf"(?:מחיר|שכירות|price|rent)\s*[:\-]?\s*{_NUM}", re.I),
]
_ROOMS = [
    re.compile(r"(\d(?:[.,]5)?)\s*(?:-\s*)?(?:חדרים|חד['׳]?|rooms?|room\b)", re.I),
    re.compile(r"(?:דירת|apartment of)\s*(\d(?:[.,]5)?)\b", re.I),
]
_PHONE = re.compile(r"(?<!\d)(0(?:5\d|[2-489]|7\d)[-\s]?\d{3}[-\s]?\d{4})(?!\d)")
PRICE_MIN, PRICE_MAX = 500, 100_000      # monthly rent range considered plausible


def _to_int(s: str) -> int | None:
    try:
        return int(re.sub(r"[,.]", "", s))
    except ValueError:
        return None


def hints(text: str) -> Hints:
    h = Hints()
    if not text:
        return h
    prices: list[int] = []
    for rx in _PRICE:
        for m in rx.finditer(text):
            v = _to_int(m.group(1))
            if v and PRICE_MIN <= v <= PRICE_MAX and v not in prices:
                prices.append(v)
    h.prices = prices
    for rx in _ROOMS:
        m = rx.search(text)
        if m:
            try:
                r = float(m.group(1).replace(",", "."))
            except ValueError:
                continue
            if 1 <= r <= 12:
                h.rooms = r
                break
    phones = []
    for m in _PHONE.finditer(text):
        p = re.sub(r"\D", "", m.group(1))
        if p not in phones:
            phones.append(p)
    h.phones = phones
    return h
