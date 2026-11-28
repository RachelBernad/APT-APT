"""/filters: choose the monitor's filters with inline-keyboard templates.

One card for the chat's single monitor (areas stay the configured Tel Aviv hoods):
price / rooms / size / floor ranges (quick picks + Custom…), property types, condition
and features. Save re-primes the monitor, so a fresh taster under the new filters
follows within seconds.

Filters are lenient on every source: a listing is dropped only for a value it states
outside the filter; a missing value passes and shows ❔ (see ``aptbot.engine`` and
``aptbot.fb_engine.matches_facts``). Yad2 features only tag listings, never drop them.

Group-chat notes: the draft lives in ``chat_data`` and the conversation is per chat,
so either allowed person can tap the card. A custom number is asked with ForceReply,
because with privacy mode on the bot only sees replies to its own messages.
"""
from __future__ import annotations

import asyncio
import logging
from dataclasses import replace
from typing import Awaitable, Callable, List, Optional

from telegram import ForceReply, InlineKeyboardButton, InlineKeyboardMarkup, Update
from telegram.constants import ParseMode
from telegram.ext import (CallbackQueryHandler, CommandHandler, ContextTypes,
                          ConversationHandler, MessageHandler, TypeHandler, filters)

from aptbot.bot import menu
from aptbot.db import Database
from aptbot.models import (FEATURES, FILTER_FEATURES, PROPERTY_CONDITION_LABELS as PROPERTY_CONDITIONS,
                            PROPERTY_TYPE_LABELS as PROPERTY_TYPES,
                           SavedSearch)

logger = logging.getLogger(__name__)

FORM, AWAIT_VALUE = range(2)
TIMEOUT_SECONDS = 600
LENIENT_NOTE = "ℹ️ A listing that doesn't mention a field is never filtered out by it; it's marked ❔."

# field -> SavedSearch attrs + quick picks (Tel Aviv rent).
FIELD_SPECS = {
    "price": {"min": "min_price", "max": "max_price", "float": False, "title": "Price", "emoji": "💰",
              "min_opts": [3000, 4000, 5000, 6000],
              "max_opts": [5000, 6000, 7000, 8000, 9000, 10000, 12000], "example": "7500"},
    "rooms": {"min": "min_rooms", "max": "max_rooms", "float": True, "title": "Rooms", "emoji": "🛏",
              "min_opts": [1, 1.5, 2, 2.5, 3, 3.5, 4], "max_opts": [2, 2.5, 3, 3.5, 4, 5], "example": "2.5"},
    "sqm": {"min": "min_sqm", "max": "max_sqm", "float": False, "title": "Size", "emoji": "📐",
            "min_opts": [40, 50, 60, 70], "max_opts": [80, 100, 120, 150], "example": "55"},
    "floor": {"min": "min_floor", "max": "max_floor", "float": False, "title": "Floor", "emoji": "🏢",
              "min_opts": [0, 1, 2, 3], "max_opts": [3, 5, 10, 20], "example": "4"},
}

DRAFT_KEY = "filters_draft"
EDITING_KEY = "filters_editing"


# --- text ---

def fmt_val(v, field: str) -> str:
    if v is None:
        return "Any"
    if field == "price":
        return f"₪{int(v):,}"
    if field == "sqm":
        return f"{int(v)} m²"
    if field == "rooms":
        return f"{float(v):g}"
    return str(int(v))


def fmt_range(s: SavedSearch, field: str) -> str:
    spec = FIELD_SPECS[field]
    lo, hi = getattr(s, spec["min"]), getattr(s, spec["max"])
    if lo is None and hi is None:
        return "Any"
    if lo is not None and hi is not None:
        return f"{fmt_val(lo, field)}–{fmt_val(hi, field)}"
    return f"≥ {fmt_val(lo, field)}" if lo is not None else f"≤ {fmt_val(hi, field)}"


def filters_summary(s: SavedSearch) -> str:
    """Multi-line HTML summary of a monitor's filters (the card and /status)."""
    types = ", ".join(PROPERTY_TYPES.get(t, str(t)) for t in s.property_types or []) or "Any"
    cond = PROPERTY_CONDITIONS.get(s.property_condition, "Any") if s.property_condition else "Any"
    feats = ", ".join(f"{FEATURES[k][3]} {FEATURES[k][2]}" for k in s.features or [] if k in FEATURES)
    return "\n".join([
        f"💰 Price: <b>{fmt_range(s, 'price')}</b>",
        f"🛏 Rooms: <b>{fmt_range(s, 'rooms')}</b>",
        f"📐 Size: <b>{fmt_range(s, 'sqm')}</b>",
        f"🏢 Floor: <b>{fmt_range(s, 'floor')}</b>",
        f"🏠 Type: <b>{types}</b>",
        f"🛠 Condition: <b>{cond}</b>",
        f"✨ Features: <b>{feats or 'Any'}</b>",
    ])


def has_filters(s: SavedSearch) -> bool:
    return any(getattr(s, spec[b]) is not None for spec in FIELD_SPECS.values() for b in ("min", "max")) \
        or bool(s.property_types) or s.property_condition is not None or bool(s.features)


# --- screens: (text, markup) ---

def card_screen(s: SavedSearch) -> tuple:
    text = f"<b>🎛 Monitor filters</b>\n\n{filters_summary(s)}\n\n{LENIENT_NOTE}\nAdjust anything, then <b>Save</b>."
    rows = [
        [InlineKeyboardButton("💰 Price", callback_data="wz:field:price"),
         InlineKeyboardButton("🛏 Rooms", callback_data="wz:field:rooms")],
        [InlineKeyboardButton("📐 Size", callback_data="wz:field:sqm"),
         InlineKeyboardButton("🏢 Floor", callback_data="wz:field:floor")],
        [InlineKeyboardButton("🏠 Type", callback_data="wz:types"),
         InlineKeyboardButton("🛠 Condition", callback_data="wz:cond")],
        [InlineKeyboardButton("✨ Features", callback_data="wz:feats"),
         InlineKeyboardButton("🧹 Clear all", callback_data="wz:clear")],
        [InlineKeyboardButton("✅ Save", callback_data="wz:save"),
         InlineKeyboardButton("✖ Cancel", callback_data="wz:cancel")],
    ]
    return text, InlineKeyboardMarkup(rows)


def range_screen(field: str, bound: str, s: SavedSearch) -> tuple:
    spec = FIELD_SPECS[field]
    is_min = bound == "min"
    lines = [f"{spec['emoji']} <b>{spec['title']}: {'minimum' if is_min else 'maximum'}</b>"]
    if is_min:
        lines.append("Step 1 of 2: pick a lower bound (or none).")
    else:
        if getattr(s, spec["min"]) is not None:
            lines.append(f"Minimum: <b>{fmt_val(getattr(s, spec['min']), field)}</b>.")
        lines.append("Step 2 of 2: pick an upper bound (or none).")
    buttons = [InlineKeyboardButton(f"🚫 No {'minimum' if is_min else 'maximum'}",
                                    callback_data=f"rng:{field}:{bound}:none")]
    lo = getattr(s, spec["min"])
    for v in spec[f"{bound}_opts"]:
        if not is_min and lo is not None and v < lo:
            continue                                   # a max below the chosen min makes no sense
        buttons.append(InlineKeyboardButton(fmt_val(v, field), callback_data=f"rng:{field}:{bound}:{v}"))
    buttons.append(InlineKeyboardButton("✏ Custom…", callback_data=f"rng:{field}:{bound}:custom"))
    rows = [buttons[i:i + 3] for i in range(0, len(buttons), 3)]
    rows.append([InlineKeyboardButton("↩ Back", callback_data="wz:card")])
    return "\n".join(lines), InlineKeyboardMarkup(rows)


def toggle_screen(title: str, options: List[tuple], chosen: set, prefix: str) -> tuple:
    """Multi-select grid, options = [(value, label)]."""
    rows, row = [], []
    for value, label in options:
        row.append(InlineKeyboardButton(f"{'✅ ' if value in chosen else ''}{label}",
                                        callback_data=f"{prefix}:{value}"))
        if len(row) == 2:
            rows.append(row)
            row = []
    if row:
        rows.append(row)
    rows.append([InlineKeyboardButton("✔ Done", callback_data="wz:card")])
    return title, InlineKeyboardMarkup(rows)


def types_screen(s: SavedSearch) -> tuple:
    return toggle_screen("🏠 <b>Property type</b> (none selected = all):",
                         list(PROPERTY_TYPES.items()), set(s.property_types or []), "pt")


def features_screen(s: SavedSearch) -> tuple:
    return toggle_screen(
        "✨ <b>Features</b>\nYad2 posters often leave these boxes unticked, so a feature never "
        "hides a listing: ✅ means it's ticked, ❔ that it isn't. A Facebook post that says "
        "it does NOT have one is skipped.",
        [(k, f"{FEATURES[k][3]} {FEATURES[k][2]}") for k in FILTER_FEATURES],
        set(s.features or []), "ft")


def cond_screen(s: SavedSearch) -> tuple:
    rows, row = [], []
    for cid, label in PROPERTY_CONDITIONS.items():
        row.append(InlineKeyboardButton(f"{'✅ ' if s.property_condition == cid else ''}{label}",
                                        callback_data=f"cond:{cid}"))
        if len(row) == 2:
            rows.append(row)
            row = []
    if row:
        rows.append(row)
    rows.append([InlineKeyboardButton("Any condition", callback_data="cond:any")])
    return "🛠 <b>Condition</b>:", InlineKeyboardMarkup(rows)


def parse_number(text: str, allow_float: bool):
    """A non-negative number from free text ("7,500", "₪7500", "2.5"), else None."""
    t = (text or "").strip().replace(",", "").replace(" ", "").replace("₪", "")
    try:
        v = float(t) if allow_float else int(t)
    except ValueError:
        return None
    return v if v >= 0 else None


def _cleared(s: SavedSearch) -> SavedSearch:
    return replace(s, min_price=None, max_price=None, min_rooms=None, max_rooms=None,
                   min_sqm=None, max_sqm=None, min_floor=None, max_floor=None,
                   property_types=None, property_condition=None, features=[])


# --- handlers ---

class FilterCommands:
    def __init__(self, db: Database, *, on_saved: Callable[[int], Awaitable[None]]):
        self.db = db
        self.on_saved = on_saved        # search id -> immediate Yad2 scan + wake Facebook

    async def _own_search(self, chat_id: int) -> Optional[SavedSearch]:
        searches = await self.db.list_searches(chat_id)
        return searches[0] if searches else None

    async def entry(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """/filters, or the menu's Filters button (which retires that menu)."""
        if update.callback_query is not None:
            await update.callback_query.answer()
            await menu.retire(update.callback_query)
        s = await self._own_search(update.effective_chat.id)
        if s is None:
            await update.effective_message.reply_text(
                "No monitor yet. Start watching first.", reply_markup=menu.menu_markup(None))
            return ConversationHandler.END
        context.chat_data[DRAFT_KEY] = replace(s, features=list(s.features or []),
                                               property_types=list(s.property_types or []) or None)
        context.chat_data.pop(EDITING_KEY, None)
        text, markup = card_screen(context.chat_data[DRAFT_KEY])
        await update.effective_message.reply_text(text, reply_markup=markup, parse_mode=ParseMode.HTML)
        return FORM

    async def button(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        query = update.callback_query
        await query.answer()
        draft: Optional[SavedSearch] = context.chat_data.get(DRAFT_KEY)
        if draft is None:
            await query.edit_message_text(
                "This card expired.",
                reply_markup=menu.menu_markup(await self._own_search(update.effective_chat.id)))
            return ConversationHandler.END
        data = query.data
        parts = data.split(":")

        async def show(screen: tuple):
            await query.edit_message_text(screen[0], reply_markup=screen[1], parse_mode=ParseMode.HTML)
            return FORM

        if data == "wz:cancel":
            self._drop(context)
            await query.edit_message_text("Cancelled. The filters didn't change.",
                                          reply_markup=menu.menu_markup(await self._own_search(draft.chat_id)))
            return ConversationHandler.END
        if data == "wz:save":
            return await self._save(query, context, draft)
        if data == "wz:clear":
            draft = context.chat_data[DRAFT_KEY] = _cleared(draft)
            return await show(card_screen(draft))
        if data == "wz:card":
            return await show(card_screen(draft))
        if data == "wz:types":
            return await show(types_screen(draft))
        if data == "wz:cond":
            return await show(cond_screen(draft))
        if data == "wz:feats":
            return await show(features_screen(draft))
        if parts[:2] == ["wz", "field"] and parts[2] in FIELD_SPECS:
            return await show(range_screen(parts[2], "min", draft))
        if parts[0] == "pt":
            cur = set(draft.property_types or [])
            cur.symmetric_difference_update({int(parts[1])})
            draft.property_types = sorted(cur) or None
            return await show(types_screen(draft))
        if parts[0] == "ft" and parts[1] in FILTER_FEATURES:
            if parts[1] in draft.features:
                draft.features.remove(parts[1])
            else:
                draft.features.append(parts[1])
            return await show(features_screen(draft))
        if parts[0] == "cond":
            draft.property_condition = None if parts[1] == "any" else int(parts[1])
            return await show(card_screen(draft))
        if parts[0] == "rng" and len(parts) == 4 and parts[1] in FIELD_SPECS:
            field, bound, value = parts[1], parts[2], parts[3]
            spec = FIELD_SPECS[field]
            if value == "custom":
                context.chat_data[EDITING_KEY] = (field, bound)
                await query.edit_message_text(f"✏ Waiting for the {'minimum' if bound == 'min' else 'maximum'} "
                                              f"{spec['title'].lower()}…")
                await query.message.reply_text(
                    f"Reply with a number, e.g. <code>{spec['example']}</code>:",
                    reply_markup=ForceReply(input_field_placeholder=spec["example"]),
                    parse_mode=ParseMode.HTML)
                return AWAIT_VALUE
            v = None if value == "none" else (float(value) if spec["float"] else int(value))
            setattr(draft, spec[bound], v)
            return await show(range_screen(field, "max", draft) if bound == "min" else card_screen(draft))
        return await show(card_screen(draft))

    async def typed_value(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        draft: Optional[SavedSearch] = context.chat_data.get(DRAFT_KEY)
        editing = context.chat_data.get(EDITING_KEY)
        if draft is None or editing is None:
            return ConversationHandler.END
        field, bound = editing
        spec = FIELD_SPECS[field]
        v = parse_number(update.effective_message.text, spec["float"])
        if v is None:
            await update.effective_message.reply_text(
                f"That's not a number. Reply with one, e.g. <code>{spec['example']}</code>:",
                reply_markup=ForceReply(input_field_placeholder=spec["example"]), parse_mode=ParseMode.HTML)
            return AWAIT_VALUE
        setattr(draft, spec[bound], v)
        context.chat_data.pop(EDITING_KEY, None)
        text, markup = range_screen(field, "max", draft) if bound == "min" else card_screen(draft)
        await update.effective_message.reply_text(text, reply_markup=markup, parse_mode=ParseMode.HTML)
        return FORM

    async def _save(self, query, context, draft: SavedSearch):
        lo_hi = [(getattr(draft, spec["min"]), getattr(draft, spec["max"])) for spec in FIELD_SPECS.values()]
        if any(lo is not None and hi is not None and lo > hi for lo, hi in lo_hi):
            await query.answer("A minimum is above its maximum; fix it first.", show_alert=True)
            return FORM
        await self.db.update_search_filters(
            draft.id, min_price=draft.min_price, max_price=draft.max_price,
            min_rooms=draft.min_rooms, max_rooms=draft.max_rooms,
            min_sqm=draft.min_sqm, max_sqm=draft.max_sqm,
            min_floor=draft.min_floor, max_floor=draft.max_floor,
            property_types=draft.property_types, property_condition=draft.property_condition,
            features=draft.features)
        await self.db.reprime_search(draft.id)
        self._drop(context)
        fresh = await self.db.get_search(draft.id)      # /stop may have run while the card was open
        await query.edit_message_text(
            f"✅ <b>Filters saved</b>\n\n{filters_summary(draft)}\n\n"
            + ("🔎 A fresh taster under these filters is on its way, then only new listings."
               if fresh.is_active else "⏸ The monitor is paused; Resume to get listings under these filters."),
            parse_mode=ParseMode.HTML, reply_markup=menu.menu_markup(fresh))
        logger.info("Filters saved for monitor #%s in chat %s", draft.id, draft.chat_id)
        if fresh.is_active:
            asyncio.create_task(self.on_saved(draft.id))
        return ConversationHandler.END

    async def cancel(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        self._drop(context)
        await update.effective_message.reply_text(
            "Cancelled. The filters didn't change.",
            reply_markup=menu.menu_markup(await self._own_search(update.effective_chat.id)))
        return ConversationHandler.END

    async def timeout(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        self._drop(context)

    @staticmethod
    def _drop(context) -> None:
        context.chat_data.pop(DRAFT_KEY, None)
        context.chat_data.pop(EDITING_KEY, None)

    def conversation(self) -> ConversationHandler:
        return ConversationHandler(
            entry_points=[CommandHandler("filters", self.entry),
                          CallbackQueryHandler(self.entry, pattern=menu.FILTERS_PATTERN),
                          MessageHandler(filters.Regex(menu.FILTERS_KEY_REGEX), self.entry)],
            states={
                FORM: [CallbackQueryHandler(self.button, pattern=r"^(wz|rng|pt|ft|cond):")],
                AWAIT_VALUE: [MessageHandler(filters.TEXT & ~filters.COMMAND & ~filters.Regex(menu.KEY_REGEX),
                                             self.typed_value),
                              CallbackQueryHandler(self.button, pattern=r"^(wz|rng|pt|ft|cond):")],
                ConversationHandler.TIMEOUT: [TypeHandler(Update, self.timeout)],
            },
            fallbacks=[CommandHandler("cancel", self.cancel)],
            conversation_timeout=TIMEOUT_SECONDS,
            per_chat=True, per_user=False, per_message=False,
            allow_reentry=True,
        )

