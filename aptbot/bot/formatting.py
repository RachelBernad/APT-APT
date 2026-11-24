"""Render normalized listing dicts into Telegram HTML messages.

New-listing, price-change and monitor-started messages, plus the photo-preview
helpers (``main_image`` / ``photo_caption``). The listing source is never shown.
"""
from __future__ import annotations

import html
from typing import Any, Dict, Optional

from aptbot.models import FEATURES


def _html_value(value: Any) -> str:
    return html.escape(str(value), quote=False)


def _format_price(price: Any) -> str:
    if isinstance(price, (int, float)):
        return f"₪{int(price):,}"
    return str(price)


def _blank(value: Any) -> bool:
    return value in (None, "", "N/A")


def main_image(apt: Dict[str, Any]) -> Optional[str]:
    """First usable http(s) image URL for a listing, or None. Used to send the
    message as a photo preview."""
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

    # Only include fields we actually know (printing "Unknown"/"N/A" reads as
    # broken). The source is intentionally NOT shown to the user.
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
    # Show only features known to be present: the ones filtered server-side.
    chips = []
    for key in apt.get("features") or []:
        spec = FEATURES.get(key)
        if spec:
            chips.append(f"{spec[3]} {spec[2]}")
    if chips:
        lines.append(f"<b>Features:</b> {', '.join(chips)}")
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


# --- Facebook group posts ---------------------------------------------------

CAPTION_LIMIT = 1024    # Telegram photo / album caption
MESSAGE_LIMIT = 4096    # Telegram text message
_ISRAEL_TZ = "Asia/Jerusalem"


def _fb_header(post) -> str:
    from zoneinfo import ZoneInfo
    parts = [f"📘 <b>{_html_value(post.group_name or 'Facebook')}</b>"]
    if post.created_at is not None:
        parts.append(post.created_at.astimezone(ZoneInfo(_ISRAEL_TZ)).strftime("%d/%m %H:%M"))
    return " · ".join(parts)


def _fb_link(post) -> str:
    if not post.url:
        return ""
    return f"<a href='{html.escape(post.url, quote=True)}'>לפוסט בפייסבוק ↗</a>"


def format_fb_post(post, limit: int = MESSAGE_LIMIT) -> str:
    """Header (group · Israel time), the post text and a link, within ``limit`` chars.
    The raw text is cut BEFORE escaping, so an entity is never split."""
    head, link = _fb_header(post), _fb_link(post)
    text = (post.text or "").strip()
    frame = len(head) + len(link) + 4
    body = _html_value(text)
    if frame + len(body) > limit:
        room = max(0, limit - frame - 1)
        while room and len(_html_value(text[:room])) > room:   # escaping grows the text
            room -= 1
        body = _html_value(text[:room].rstrip()) + "…"
    return "\n\n".join(p for p in (head, body, link) if p)


def fb_post_fits_caption(post) -> bool:
    return len(format_fb_post(post, limit=MESSAGE_LIMIT)) <= CAPTION_LIMIT


def format_fb_short_caption(post) -> str:
    """Album caption when the full text doesn't fit (the text follows as a message)."""
    return "\n".join(p for p in (_fb_header(post), _fb_link(post)) if p)


def format_fb_backfill_intro(sample: int, groups: int) -> str:
    return (
        f"📘 <b>Facebook:</b> here are the <b>{sample} newest post"
        f"{'s' if sample != 1 else ''}</b> from {groups} Tel Aviv apartment groups. "
        f"From now on I'll send only new ones (each post once, even if it's cross-posted)."
    )


def format_fb_stopped(reason: str) -> str:
    return (
        "⚠️ <b>Facebook reading stopped</b> to keep the account safe.\n"
        f"Reason: {_html_value(reason)}\n"
        "Someone needs to check the account (log in / clear the warning) and restart the bot. "
        "Yad2 keeps working."
    )
