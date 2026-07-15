# formatting.py
"""Render normalized listing dicts into Telegram HTML messages.

Ported unchanged in spirit from the old ``telegram_bot.format_apartment_message``
so downstream output stays identical; adds a per-source label and price-change
formatter.
"""
from __future__ import annotations

import html
from typing import Any, Dict, Optional

import config
from models import FEATURES


def _html_value(value: Any) -> str:
    return html.escape(str(value), quote=False)


def _trim_text(value: Any) -> str:
    text = str(value)
    if len(text) <= config.MAX_DESCRIPTION_CHARS:
        return text
    return text[: config.MAX_DESCRIPTION_CHARS].rstrip() + "..."


def _format_bool(value: Any) -> str:
    if value is True:
        return "Yes"
    if value is False:
        return "No"
    if value is None:
        return "Unknown"
    return html.escape(str(value), quote=False)


def _format_tags(tags: Any) -> str:
    if not tags:
        return "N/A"
    if not isinstance(tags, list):
        return _html_value(tags)
    return ", ".join(_html_value(tag) for tag in tags)


def _format_price(price: Any) -> str:
    if isinstance(price, (int, float)):
        return f"₪{int(price):,}"
    return str(price)


def _blank(value: Any) -> bool:
    return value in (None, "", "N/A")


def main_image(apt: Dict[str, Any]) -> Optional[str]:
    """First usable http(s) image URL for a listing, or None (both Yad2 &
    rentlyfly populate ``images``). Used to send the message as a photo preview."""
    for url in apt.get("images") or []:
        if isinstance(url, str) and url.startswith("http"):
            return url
    return None


def photo_caption(text: str, limit: int = 1024) -> str:
    """Trim a rendered message to Telegram's photo-caption limit on whole-line
    boundaries, so the HTML stays valid (we only ever drop trailing lines)."""
    if len(text) <= limit:
        return text
    out, total = [], 0
    for line in text.split("\n"):
        if total + len(line) + 1 > limit - 24:      # margin for safety
            break
        out.append(line)
        total += len(line) + 1
    return "\n".join(out)


def format_apartment_message(apt: Dict[str, Any]) -> str:
    location = _html_value(
        apt.get("location")
        or ", ".join(p for p in (apt.get("street"), apt.get("city")) if p)
        or "N/A"
    )
    url = html.escape(str(apt.get("apartment_page_url", "N/A")), quote=True)

    # Only include fields we actually know. Some sources carry no description /
    # tags / mamad and only known-elevator, so printing "Unknown"/"N/A" for them
    # reads as broken; richer listings do carry these and will show them.
    # The source is intentionally NOT shown to the user.
    lines = [
        "<b>🏠 Apartment Found!</b>",
        f"<b>Price:</b> {_format_price(apt.get('price', 'N/A'))}",
        f"<b>Location:</b> {location}",
    ]
    if not _blank(apt.get("property_type")):
        lines.append(f"<b>Type:</b> {_html_value(apt['property_type'])}")
    if not _blank(apt.get("rooms")):
        lines.append(f"<b>Rooms:</b> {_html_value(apt['rooms'])}")
    if not _blank(apt.get("size")):
        lines.append(f"<b>Size:</b> {_html_value(apt['size'])} sqm")
    if not _blank(apt.get("floor")):
        lines.append(f"<b>Floor:</b> {_html_value(apt['floor'])}")
    # Show only features known to be present: Yad2 carries the ones filtered
    # server-side, rentlyfly reports them inline.
    chips = []
    for key in apt.get("features") or []:
        spec = FEATURES.get(key)
        if spec:
            chips.append(f"{spec[3]} {spec[2]}")
    if apt.get("is_mamad") is True and "mamad" not in (apt.get("features") or []):
        chips.append("🛡 Safe room")
    if chips:
        lines.append(f"<b>Features:</b> {', '.join(chips)}")
    if apt.get("tags"):
        lines.append(f"<b>Tags:</b> {_format_tags(apt['tags'])}")
    if not _blank(apt.get("description")):
        lines.append(f"<b>Description:</b> {_html_value(_trim_text(apt['description']))}")
    lines.append(f"<b>URL:</b> <a href='{url}'>Link</a>")
    return "\n".join(lines)


def format_backfill_intro(label: str, sample: int, interval_minutes: int) -> str:
    """Header shown right before a new monitor's initial capped backfill batch."""
    who = f" for <b>{_html_value(label)}</b>" if label else ""
    return (
        f"✅ <b>Monitor started{who}!</b>\n\n"
        f"Here's a <b>sample of {sample} current listing"
        f"{'s' if sample != 1 else ''}</b> that already match. "
        f"This is just a taster — I won't resend these.\n\n"
        f"⏱ From now on I check <b>every {interval_minutes} minutes</b> and will "
        f"ping you the moment a <b>new</b> matching apartment is posted (and if a "
        f"price changes)."
    )


def format_monitor_started_empty(label: str, interval_minutes: int) -> str:
    """Shown when a new monitor has no current matches to sample."""
    who = f" for <b>{_html_value(label)}</b>" if label else ""
    return (
        f"✅ <b>Monitor started{who}!</b>\n\n"
        f"Nothing matches right now, so there's no sample to show. "
        f"⏱ I'll keep checking <b>every {interval_minutes} minutes</b> and notify "
        f"you as soon as a matching apartment appears."
    )


def format_price_change(apt: Dict[str, Any], old_price: Optional[int]) -> str:
    url = html.escape(str(apt.get("apartment_page_url", "N/A")), quote=True)
    location = _html_value(apt.get("location") or apt.get("city", ""))
    return (
        f"<b>🔄 Price Changed!</b>\n"
        f"<b>Location:</b> {location}\n"
        f"<b>Was:</b> {_format_price(old_price)}  →  <b>Now:</b> {_format_price(apt.get('price'))}\n"
        f"<b>URL:</b> <a href='{url}'>Link</a>"
    )
