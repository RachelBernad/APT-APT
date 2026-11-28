"""The control menu: inline buttons under every command reply.

The buttons follow the monitor's state, so only the next sensible actions show:

- no monitor yet:  ▶️ Start watching · ❓ Help
- watching:        🎛 Filters · 📊 Status / ⏸ Pause · ❓ Help
- paused:          ▶️ Resume · 🎛 Filters / 📊 Status · ❓ Help

The filters button reads "Set filters" until some are chosen, then shows how many.
Taps send ``menu:<action>``. ``menu:filters`` opens the /filters card (an entry point
of its ConversationHandler); the rest go to ``SimpleCommands.menu_tap``.
"""
from __future__ import annotations

import re
from typing import Optional

from telegram import InlineKeyboardButton, InlineKeyboardMarkup, ReplyKeyboardMarkup

from aptbot.models import SavedSearch

PATTERN = r"^menu:(monitor|stop|status|help)$"
FILTERS_PATTERN = r"^menu:filters$"


def filter_count(s: SavedSearch) -> int:
    """How many filter groups are set (a range counts once, however many bounds)."""
    ranges = [("min_price", "max_price"), ("min_rooms", "max_rooms"),
              ("min_sqm", "max_sqm"), ("min_floor", "max_floor")]
    n = sum(1 for lo, hi in ranges if getattr(s, lo) is not None or getattr(s, hi) is not None)
    n += bool(s.property_types) + (s.property_condition is not None) + len(s.features or [])
    return n


def _filters_button(s: SavedSearch) -> InlineKeyboardButton:
    n = filter_count(s)
    return InlineKeyboardButton(f"🎛 Filters · {n}" if n else "🎛 Set filters",
                                callback_data="menu:filters")


def menu_markup(s: Optional[SavedSearch]) -> InlineKeyboardMarkup:
    status = InlineKeyboardButton("📊 Status", callback_data="menu:status")
    help_ = InlineKeyboardButton("❓ Help", callback_data="menu:help")
    if s is None:
        rows = [[InlineKeyboardButton("▶️ Start watching", callback_data="menu:monitor"), help_]]
    elif s.is_active:
        rows = [[_filters_button(s), status],
                [InlineKeyboardButton("⏸ Pause", callback_data="menu:stop"), help_]]
    else:
        rows = [[InlineKeyboardButton("▶️ Resume", callback_data="menu:monitor"), _filters_button(s)],
                [status, help_]]
    return InlineKeyboardMarkup(rows)


async def retire(query) -> None:
    """Strip the buttons from the tapped menu, so only the newest menu in the chat is live."""
    try:
        await query.edit_message_reply_markup(reply_markup=None)
    except Exception:          # message too old / already edited: harmless
        pass


# --- the bottom button panel (Telegram reply keyboard, next to "/") ---
# Static, so it never needs re-sending when the monitor's state changes; each
# command already answers a tap in the wrong state ("Already watching" etc).
KEYS = {"🎛 Filters": "filters", "📊 Status": "status",
        "▶️ Start": "monitor", "⏸ Pause": "stop", "❓ Help": "help"}
KEY_REGEX = "^(" + "|".join(re.escape(k) for k in KEYS) + ")$"
FILTERS_KEY_REGEX = "^" + re.escape("🎛 Filters") + "$"
KEYBOARD_META = "keyboard_v1:{}"


def keyboard() -> ReplyKeyboardMarkup:
    return ReplyKeyboardMarkup(
        [["🎛 Filters", "📊 Status"], ["▶️ Start", "⏸ Pause", "❓ Help"]],
        resize_keyboard=True, is_persistent=True, input_field_placeholder="Tap a button or type /")
