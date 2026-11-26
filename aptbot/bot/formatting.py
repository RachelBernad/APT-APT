"""Render normalized listing dicts into Telegram HTML messages.

New-listing, price-change and monitor-started messages, plus the photo-preview
helpers (``main_image`` / ``photo_caption``). The listing source is never shown.
"""
from __future__ import annotations

import html
from typing import Any, Dict, Optional

from aptbot.models import FEATURE_FACT_FIELD, FEATURES


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


def _has_range(s, field: str) -> bool:
    return s is not None and (getattr(s, f"min_{field}") is not None
                              or getattr(s, f"max_{field}") is not None)


def format_apartment_message(apt: Dict[str, Any], s=None) -> str:
    """``s`` (the monitor's SavedSearch) adds ❔ for filtered fields the listing doesn't
    report, and a ✅ / ❔ line for the monitor's wanted features."""
    location = _html_value(
        apt.get("location")
        or ", ".join(p for p in (apt.get("street"), apt.get("city")) if p)
        or "N/A"
    )
    url = html.escape(str(apt.get("apartment_page_url", "N/A")), quote=True)

    # Only include fields we actually know (printing "Unknown"/"N/A" reads as
    # broken). The source is intentionally NOT shown to the user.
    price = apt.get("price")
    lines = [
        "<b>🏠 Apartment Found!</b>",
        f"<b>Price:</b> {_format_price(price) if not _blank(price) else '❔ not listed'}",
        f"<b>Location:</b> {location}",
    ]
    if not _blank(apt.get("property_type")):
        lines.append(f"<b>Type:</b> {_html_value(apt['property_type'])}")
    for key, field, label, unit in (("rooms", "rooms", "Rooms", ""), ("size", "sqm", "Size", " sqm"),
                                    ("floor", "floor", "Floor", "")):
        if not _blank(apt.get(key)):
            lines.append(f"<b>{label}:</b> {_html_value(apt[key])}{unit}")
        elif _has_range(s, field):
            lines.append(f"<b>{label}:</b> ❔ not listed")
    have = set(apt.get("features") or [])
    wanted = list(s.features or []) if s is not None else []
    # Wanted features: ✅ ticked on Yad2, ❔ not ticked (could be "no" or "didn't say").
    # Without a monitor, show the features known to be present.
    chips = [f"{'✅' if k in have else '❔'} {FEATURES[k][2]}" for k in wanted if k in FEATURES]
    if not wanted:
        chips = [f"{FEATURES[k][3]} {FEATURES[k][2]}" for k in apt.get("features") or [] if k in FEATURES]
    if chips:
        lines.append(f"<b>Features:</b> {' · '.join(chips) if wanted else ', '.join(chips)}")
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


# --- AI summary (aptbot.listing_ai Listing Facts) ---------------------------------
SUMMARY_SEPARATOR = "────────"
_KIND_LABELS = {
    "rent": "🏠 השכרה", "roommate": "👥 שותפים", "sublet": "🧳 סאבלט", "sale": "🏷 מכירה",
    "seeking": "🔎 מחפשים דירה", "not_listing": "🚫 לא מודעת דירה",
}
# Three-state booleans: True -> listed after ✅, False -> listed after ❌, None -> hidden.
_FEATURE_LABELS = (
    ("has_mamad", "ממ״ד"), ("has_mamak", "ממ״ק"), ("has_shelter", "מקלט"), ("elevator", "מעלית"),
    ("renovated", "משופצת"), ("balcony", "מרפסת"), ("splitted_apt", "דירה מחולקת"),
    ("underground_apartment", "דירת מרתף"),
)


def _num(x) -> str:
    return f"{x:g}" if isinstance(x, float) else str(x)


def _ils(x) -> str:
    return f"{int(x):,} ₪"


def _floor(x) -> str:
    return "קרקע" if x == 0 else "מרתף" if x == -1 else _num(float(x))


def _entry(v: str) -> str:
    if v == "immediate":
        return "מיידית"
    parts = v.split("-")
    return "/".join(reversed(parts)) if all(p.isdigit() for p in parts) else v


def _unknown_wanted(facts, s) -> list:
    """Labels of the monitor's filters this post doesn't answer (it was kept anyway)."""
    if s is None:
        return []
    out = []
    for field, fact, label in (("price", "price_ils", "מחיר"), ("rooms", "rooms_count", "חדרים"),
                               ("sqm", "square_meters", "גודל"), ("floor", "floor", "קומה")):
        if _has_range(s, field) and getattr(facts, fact, None) is None:
            out.append(label)
    for key in s.features or []:
        fact = FEATURE_FACT_FIELD.get(key)
        if key in FEATURES and (fact is None or getattr(facts, fact, None) is None):
            out.append(FEATURES[key][1])
    return out


def format_fb_summary(facts, s=None) -> str:
    """Compact Hebrew summary of a post's Listing Facts (only fields that are not null).
    With the monitor ``s``, a last ❔ line names its filters the post doesn't answer."""
    kind = getattr(facts.post_kind, "value", facts.post_kind)
    head = _KIND_LABELS.get(kind, str(kind))
    if kind in ("seeking", "not_listing"):
        return head
    if facts.city_area_or_address:
        head += " · " + _html_value(facts.city_area_or_address) + (" 📍" if facts.has_accurate_address else "")
    lines = [head]

    money = []
    if facts.price_ils is not None:
        money.append(_ils(facts.price_ils))
    if facts.arnona_ils is not None:
        money.append(f"ארנונה {facts.arnona_ils:,}")
    if facts.vaad_bayit_ils is not None:
        money.append(f"ועד {facts.vaad_bayit_ils:,}")
    if facts.bills_included is not None:
        money.append("כולל חשבונות" if facts.bills_included else "לא כולל חשבונות")
    if facts.is_broker is True:
        money.append("תיווך" + (f" ({_html_value(facts.broker_fee)})" if facts.broker_fee else ""))
    elif facts.is_broker is False:
        money.append("ללא תיווך")
    if facts.entry_date:
        money.append("כניסה: " + _html_value(_entry(facts.entry_date)))
    if facts.lease_term_months:
        money.append(f"חוזה {facts.lease_term_months} חודשים")
    if money:
        lines.append("💰 " + " · ".join(money))

    size = []
    if facts.rooms_count is not None:
        size.append(f"{_num(facts.rooms_count)} חד׳")
    if facts.square_meters is not None:
        size.append(f"{_num(facts.square_meters)} מ״ר")
    if facts.floor is not None:
        size.append(f"קומה {_floor(facts.floor)}")
    if size:
        lines.append("🛏 " + " · ".join(size))

    yes, no = [], []
    for field, label in _FEATURE_LABELS:
        v = getattr(facts, field)
        if field == "balcony" and v and facts.balcony_type:
            label = _html_value(facts.balcony_type)
        (yes if v is True else no if v is False else []).append(label)
    feats = []
    if yes:
        feats.append("✅ " + " · ".join(yes))
    if no:
        feats.append("❌ " + " · ".join(no))
    if feats:
        lines.append("  ".join(feats))
    unknown = _unknown_wanted(facts, s)
    if unknown:
        lines.append("❔ לא צוין: " + " · ".join(unknown))
    return "\n".join(lines)


def format_fb_post(post, limit: int = MESSAGE_LIMIT, summary: Optional[str] = None) -> str:
    """Header (group · Israel time), the post text and a link, within ``limit`` chars.
    With an AI summary the top block is header + summary + link, then a separator, then the
    text (the link stays on top, visible without scrolling). The raw text is cut BEFORE
    escaping, so an entity is never split."""
    link = _fb_link(post)
    if summary:
        head = "\n".join(p for p in (_fb_header(post), summary, link) if p) + "\n" + SUMMARY_SEPARATOR
        tail = ""
    else:
        head, tail = _fb_header(post), link
    text = (post.text or "").strip()
    frame = len(head) + len(tail) + 4
    body = _html_value(text)
    if frame + len(body) > limit:
        room = max(0, limit - frame - 1)
        while room and len(_html_value(text[:room])) > room:   # escaping grows the text
            room -= 1
        body = _html_value(text[:room].rstrip()) + "…"
    return "\n\n".join(p for p in (head, body, tail) if p)


def fb_post_fits_caption(post, summary: Optional[str] = None) -> bool:
    return len(format_fb_post(post, limit=MESSAGE_LIMIT, summary=summary)) <= CAPTION_LIMIT


def format_fb_short_caption(post, summary: Optional[str] = None) -> str:
    """Album caption when the full text doesn't fit (the text follows as a message).
    The summary stays here, on the photos."""
    head = f"{_fb_header(post)}\n{summary}" if summary else _fb_header(post)
    return "\n".join(p for p in (head, _fb_link(post)) if p)


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
