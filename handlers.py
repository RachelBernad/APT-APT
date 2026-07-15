# handlers.py
"""Telegram command + conversation handlers.

UX: a persistent tap-bar (Add monitor / My searches / Help) drives an inline
drill-down. A *monitor* watches many areas (searched nationwide or browsed by
Tel-Aviv quarter) under one filter set. Copy is explicit about what happens next.
"""
from __future__ import annotations

import asyncio
import datetime
import logging
import os
import tempfile
from typing import Awaitable, Callable, Dict, List, Optional

import aiohttp
from telegram import (InlineKeyboardButton, InlineKeyboardMarkup,
                     KeyboardButton, ReplyKeyboardMarkup, Update)
from telegram.constants import ParseMode
from telegram.ext import (CallbackQueryHandler, CommandHandler, ContextTypes,
                         ConversationHandler, MessageHandler, filters)

import config
import engine
import locations
import report
from db import Database
from models import (FEATURES, PROPERTY_CONDITIONS, PROPERTY_TYPES,
                   LocationTarget, ResolvedLocation, SavedSearch, feature_label)

logger = logging.getLogger(__name__)

# Conversation states. Typing works in AREAS directly (no separate search state).
AREAS, FORM, AWAIT_VALUE = range(3)

# Persistent tap-bar labels (also the text those buttons send)
BAR_ADD = "➕ Add monitor"
BAR_LIST = "📋 My searches"
BAR_REPORT = "📄 Daily report"
BAR_HELP = "❓ Help"
BAR_REGEX = r"^(➕ Add monitor|📋 My searches|📄 Daily report|❓ Help)$"

_LEVEL_EMOJI = {"city": "🏙", "hood": "🏘", "area": "🗺", "region": "🌍", "street": "🛣"}


def bar_keyboard() -> ReplyKeyboardMarkup:
    return ReplyKeyboardMarkup(
        [[KeyboardButton(BAR_ADD)],
         [KeyboardButton(BAR_LIST), KeyboardButton(BAR_HELP)],
         [KeyboardButton(BAR_REPORT)]],
        resize_keyboard=True, is_persistent=True, input_field_placeholder="Tap a button or type an area…",
    )


# --- copy ---

HELP_TEXT = (
    "<b>🏠 Apartment Finder</b>\n"
    "I watch Yad2 (all of Israel) and Rentlyfly (Tel Aviv) and ping you when a "
    "<b>new</b> rental that matches your monitor is posted.\n\n"
    "<b>How it works</b>\n"
    "• A <b>monitor</b> = one or more <b>areas</b> + your filters.\n"
    "• Start by choosing a <b>city</b> (type any city, or tap a popular one), then "
    "watch the whole city or tap specific <b>neighborhoods</b> — add as many as you like.\n"
    "• When you create it, I send a small <b>sample of current matches</b>, then "
    "check <b>every {interval} minutes</b> and alert you about brand-new listings "
    "(and price drops).\n\n"
    "<b>Buttons</b>\n"
    "➕ <b>Add monitor</b> · 📋 <b>My searches</b> · 📄 <b>Daily report</b> · ❓ <b>Help</b>\n\n"
    "📄 <b>Daily report</b> builds a nicely-designed page of all your current matches "
    "(once a day).\n\n"
    "Tip: type in <b>Hebrew</b> (e.g. תל אביב, רמת גן, קריות) — English names for big "
    "cities work too."
)


def _help_text() -> str:
    return HELP_TEXT.format(interval=max(1, config.CHECK_INTERVAL_SECONDS // 60))


def _ask_area_text() -> str:
    return (
        "🔎 <b>Type an area to watch</b> (Hebrew works best):\n"
        "e.g. <code>תל אביב</code>, <code>פלורנטין</code>, <code>רמת גן</code>, "
        "<code>קרית ביאליק</code>, a whole district, or a street.\n\n"
        "I'll show matches to pick from. Send /cancel to stop."
    )


def _fmt_range(lo, hi, unit: str = "") -> str:
    if lo is None and hi is None:
        return "Any"
    if lo is not None and hi is not None:
        return f"{lo}–{hi}{unit}"
    if lo is not None:
        return f"≥{lo}{unit}"
    return f"≤{hi}{unit}"


# --- draft helpers ---

def _new_draft() -> dict:
    return {
        "edit_id": None, "label": "", "locations": [],
        "min_price": config.DEFAULT_MIN_PRICE, "max_price": config.DEFAULT_MAX_PRICE,
        "min_rooms": config.DEFAULT_MIN_ROOMS, "max_rooms": config.DEFAULT_MAX_ROOMS,
        "min_sqm": config.DEFAULT_MIN_SQM, "max_sqm": None,
        "min_floor": config.DEFAULT_MIN_FLOOR, "max_floor": config.DEFAULT_MAX_FLOOR,
        "property_types": None, "property_condition": config.DEFAULT_PROPERTY_CONDITION,
        "features": list(config.DEFAULT_FEATURES), "source_mode": "auto",
    }


def _loc_key(t: LocationTarget) -> tuple:
    return (t.level, t.region_id, t.area_id, t.city_id, t.hood_id, t.street_id, t.match_name)


def _add_location(draft: dict, target: LocationTarget) -> bool:
    keys = {_loc_key(t) for t in draft["locations"]}
    if _loc_key(target) in keys:
        return False
    draft["locations"].append(target)
    if not draft["label"]:
        draft["label"] = target.display_name
    return True


def _is_tlv(draft: dict) -> bool:
    return any(t.city_id == config.TEL_AVIV_CITY_ID for t in draft["locations"])


# --- screen renderers ---

def _city_picker_screen(draft: dict) -> tuple:
    """The FIRST step of add-monitor: pick a city (type any, or tap a popular one)."""
    locs: List[LocationTarget] = draft["locations"]
    lines = [
        "<b>🏙 Which areas should I watch?</b>",
        "<b>Type any city in Israel</b> to browse its neighborhoods — or tap a popular one below.",
    ]
    if locs:
        lines.append("\n<b>Added so far:</b>")
        for t in locs:
            lines.append(f"{_LEVEL_EMOJI.get(t.level, '📍')} {t.display_name}")
    rows, row = [], []
    for name, rid, cid in locations.popular_cities():
        row.append(InlineKeyboardButton(name, callback_data=f"az:city:{rid}:{cid}"))
        if len(row) == 2:
            rows.append(row); row = []
    if row:
        rows.append(row)
    for i, t in enumerate(locs[:10]):
        rows.append([InlineKeyboardButton(f"✖ {t.display_name[:30]}", callback_data=f"az:rm:{i}")])
    nav = []
    if locs:
        nav.append(InlineKeyboardButton(f"➡ Next: filters ({len(locs)})", callback_data="az:done"))
    nav.append(InlineKeyboardButton("✖ Cancel", callback_data="wz:cancel"))
    rows.append(nav)
    return "\n".join(lines), InlineKeyboardMarkup(rows)


def _cand_selected(draft: dict, cand: ResolvedLocation) -> bool:
    key = _loc_key(locations.target_from_resolved(cand))
    return any(_loc_key(t) == key for t in draft["locations"])


def _search_results_screen(draft: dict, cities: list, others: list) -> tuple:
    """Results after typing (Image #3): cities drill into neighborhoods at the top;
    every other result is a multi-select add/remove toggle so several can be picked."""
    rows = []
    for c in cities:
        rows.append([InlineKeyboardButton(f"🏙 {c.display} — browse neighborhoods",
                                         callback_data=f"az:city:{c.region_id}:{c.city_id}")])
    for i, c in enumerate(others):
        mark = "✅" if _cand_selected(draft, c) else "➕"
        rows.append([InlineKeyboardButton(f"{mark} {_LEVEL_EMOJI.get(c.level, '📍')} {c.display}",
                                         callback_data=f"az:tog:{i}")])
    n = len(draft["locations"])
    nav = [InlineKeyboardButton("↩ Back", callback_data="az:home")]
    if n:
        nav.append(InlineKeyboardButton(f"✔ Done ({n})", callback_data="az:done"))
    rows.append(nav)
    header = ("Tap a <b>city</b> to browse its neighborhoods, or tap areas to "
              "<b>add / remove</b> them (multi-select)")
    if n:
        header += f" — <b>{n}</b> added so far"
    return header + ":", InlineKeyboardMarkup(rows)


def _selected_hood_names(draft: dict) -> set:
    return {t.match_name for t in draft["locations"] if t.level == "hood"}


def _watch_all_button(rid: int, cid: int, name: str) -> InlineKeyboardButton:
    return InlineKeyboardButton(f"🏙 Watch ALL of {name}", callback_data=f"az:whole:{rid}:{cid}")


def _quarters_screen(rid: int, cid: int, name: str) -> tuple:
    quarters = locations.city_quarters(cid)
    rows = [[_watch_all_button(rid, cid, name)]]
    rows += [[InlineKeyboardButton(f"🗂 {q['name']}", callback_data=f"az:q:{rid}:{cid}:{i}")]
             for i, q in enumerate(quarters)]
    rows.append([InlineKeyboardButton("↩ Cities", callback_data="az:home")])
    return (f"<b>{name}</b> — watch the whole city, or open a quarter to pick specific "
            "neighborhoods (multi-select):", InlineKeyboardMarkup(rows))


def _hoods_screen(draft: dict, rid: int, cid: int, q_idx: int) -> tuple:
    from models import normalize_name
    quarters = locations.city_quarters(cid)
    if q_idx >= len(quarters):
        return _quarters_screen(rid, cid, locations.city_name(cid))
    quarter = quarters[q_idx]
    chosen = _selected_hood_names(draft)
    rows, count = [], 0
    for h_idx, hood in enumerate(quarter["hoods"]):
        on = normalize_name(hood) in chosen
        count += 1 if on else 0
        rows.append([InlineKeyboardButton(f"{'✅' if on else '➕'} {hood}",
                                         callback_data=f"az:h:{rid}:{cid}:{q_idx}:{h_idx}")])
    rows.append([InlineKeyboardButton("↩ Quarters", callback_data=f"az:city:{rid}:{cid}"),
                InlineKeyboardButton("✔ Done", callback_data="az:home")])
    return (f"<b>{quarter['name']}</b> — tap to add/remove ({count} selected here):",
            InlineKeyboardMarkup(rows))


def _flat_hoods_screen(draft: dict, browse: dict) -> tuple:
    from models import normalize_name
    rid, cid, name = browse["rid"], browse["cid"], browse["name"]
    chosen = _selected_hood_names(draft)
    hoods = browse.get("hoods", [])
    rows = [[_watch_all_button(rid, cid, name)]]
    rows += [[InlineKeyboardButton(
        f"{'✅' if normalize_name(h) in chosen else '➕'} {h}", callback_data=f"az:bh:{i}")]
        for i, h in enumerate(hoods)]
    total_sel = sum(1 for h in hoods if normalize_name(h) in chosen)
    done_label = f"✅ Done ({total_sel})" if total_sel else "↩ Back"
    rows.append([InlineKeyboardButton(done_label, callback_data="az:home")])
    return (f"<b>{name}</b> — watch the whole city, or tap specific neighborhoods "
            f"({total_sel} selected):", InlineKeyboardMarkup(rows))


def _render_card(draft: dict) -> tuple:
    locs = draft["locations"]
    area_names = ", ".join(t.display_name for t in locs[:4])
    if len(locs) > 4:
        area_names += f" +{len(locs) - 4} more"
    types = draft["property_types"]
    types_label = "Any" if not types else ", ".join(PROPERTY_TYPES.get(t, str(t)) for t in types)
    cond = draft["property_condition"]
    cond_label = PROPERTY_CONDITIONS.get(cond, "Any") if cond else "Any"
    feats = draft["features"]
    feats_label = "None" if not feats else ", ".join(feature_label(k) for k in feats)
    src = draft["source_mode"]
    src_label = "Yad2 + Rentlyfly" if src == "auto" else "Yad2 only"

    text = (
        f"<b>🎛 Monitor filters</b>\n"
        f"📍 <b>{area_names or '—'}</b>  ({len(locs)} area{'s' if len(locs) != 1 else ''})\n\n"
        f"💰 Price: <b>{_fmt_range(draft['min_price'], draft['max_price'])}</b>\n"
        f"🛏 Rooms: <b>{_fmt_range(draft['min_rooms'], draft['max_rooms'])}</b>\n"
        f"📐 Size: <b>{_fmt_range(draft['min_sqm'], draft['max_sqm'], ' m²')}</b>\n"
        f"🏢 Floor: <b>{_fmt_range(draft['min_floor'], draft['max_floor'])}</b>\n"
        f"🏠 Type: <b>{types_label}</b>\n"
        f"🛠 Condition: <b>{cond_label}</b>\n"
        f"✨ Features: <b>{feats_label}</b>\n"
    )
    if _is_tlv(draft):
        text += f"🔗 Source: <b>{src_label}</b>\n"
    text += "\nAdjust anything, then <b>Save</b>."

    rows = [
        [InlineKeyboardButton("💰 Price", callback_data="wz:field:price"),
         InlineKeyboardButton("🛏 Rooms", callback_data="wz:field:rooms")],
        [InlineKeyboardButton("📐 Size", callback_data="wz:field:sqm"),
         InlineKeyboardButton("🏢 Floor", callback_data="wz:field:floor")],
        [InlineKeyboardButton("🏠 Type", callback_data="wz:types"),
         InlineKeyboardButton("🛠 Condition", callback_data="wz:cond")],
        [InlineKeyboardButton("✨ Features", callback_data="wz:feats"),
         InlineKeyboardButton(f"📍 Areas ({len(locs)})", callback_data="az:home")],
    ]
    if _is_tlv(draft):
        rows.append([InlineKeyboardButton(f"🔗 Source: {src_label}", callback_data="wz:toggle:src")])
    rows.append([InlineKeyboardButton("✅ Save monitor", callback_data="wz:save"),
                InlineKeyboardButton("✖ Cancel", callback_data="wz:cancel")])
    return text, InlineKeyboardMarkup(rows)


def _multi_toggle_screen(title: str, options: List[tuple], chosen: set,
                        toggle_prefix: str, done_cb: str) -> tuple:
    """Generic multi-select grid. options = [(value, label), ...]."""
    rows, row = [], []
    for value, label in options:
        mark = "✅ " if value in chosen else ""
        row.append(InlineKeyboardButton(f"{mark}{label}", callback_data=f"{toggle_prefix}{value}"))
        if len(row) == 2:
            rows.append(row); row = []
    if row:
        rows.append(row)
    rows.append([InlineKeyboardButton("✔ Done", callback_data=done_cb)])
    return title, InlineKeyboardMarkup(rows)


class BotHandlers:
    def __init__(self, db: Database, http: aiohttp.ClientSession,
                on_saved: Optional[Callable[[int], Awaitable[None]]] = None):
        self.db = db
        self.http = http
        # Called with a search_id right after save so the bot scans it immediately
        # (backfill sample within seconds instead of waiting for the next cycle).
        self.on_saved = on_saved

    def _scan_soon(self, search_id: int) -> None:
        if self.on_saved:
            asyncio.create_task(self.on_saved(search_id))

    # --- basic commands ---
    async def start(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        chat, user = update.effective_chat, update.effective_user
        await self.db.upsert_user(chat.id, user.username if user else None)
        searches = await self.db.list_searches(chat.id)
        hint = "" if searches else "\n\n👉 Tap <b>➕ Add monitor</b> to create your first one."
        await update.effective_message.reply_text(
            _help_text() + hint, reply_markup=bar_keyboard(), parse_mode=ParseMode.HTML,
        )

    async def help(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        await update.effective_message.reply_text(
            _help_text(), reply_markup=bar_keyboard(), parse_mode=ParseMode.HTML)

    async def mute(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        await self.db.set_user_active(update.effective_chat.id, False)
        await update.effective_message.reply_text(
            "🔕 All notifications paused. Tap <b>❓ Help</b> anytime, or send /unmute to resume.",
            parse_mode=ParseMode.HTML)

    async def unmute(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        chat = update.effective_chat
        await self.db.upsert_user(chat.id, update.effective_user.username if update.effective_user else None)
        await self.db.set_user_active(chat.id, True)
        await self.db.reprime_user_searches(chat.id)
        await update.effective_message.reply_text(
            "🔔 Notifications resumed. I'll send a fresh sample from each monitor, then "
            "alert you about new listings.", reply_markup=bar_keyboard())

    async def home_button(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        query = update.callback_query
        await query.answer()
        if query.data == "home:list":
            await self._show_searches(update, context, edit=True)
        elif query.data == "home:help":
            await query.edit_message_text(_help_text(), parse_mode=ParseMode.HTML)

    # --- add / edit monitor conversation ---
    async def add_entry(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        context.user_data.clear()
        context.user_data["draft"] = _new_draft()
        text, markup = _city_picker_screen(context.user_data["draft"])
        if update.callback_query:
            await update.callback_query.answer()
            await update.callback_query.message.reply_text(text, reply_markup=markup, parse_mode=ParseMode.HTML)
        else:
            await update.effective_message.reply_text(text, reply_markup=markup, parse_mode=ParseMode.HTML)
        return AREAS

    async def edit_entry(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        query = update.callback_query
        await query.answer()
        search_id = int(query.data.split(":")[2])
        search = await self.db.get_search(search_id)
        if not search or search.chat_id != update.effective_chat.id:
            await query.edit_message_text("Monitor not found.")
            return ConversationHandler.END
        context.user_data.clear()
        draft = _new_draft()
        draft.update({
            "edit_id": search.id, "label": search.label,
            "locations": list(search.locations),
            "min_price": search.min_price, "max_price": search.max_price,
            "min_rooms": search.min_rooms, "max_rooms": search.max_rooms,
            "min_sqm": search.min_sqm, "max_sqm": search.max_sqm,
            "min_floor": search.min_floor, "max_floor": search.max_floor,
            "property_types": search.property_types, "property_condition": search.property_condition,
            "features": list(search.features), "source_mode": search.source_mode,
        })
        context.user_data["draft"] = draft
        text, markup = _render_card(draft)
        await query.edit_message_text(text, reply_markup=markup, parse_mode=ParseMode.HTML)
        return FORM

    # areas state: search + browse callbacks
    async def areas_cb(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        query = update.callback_query
        await query.answer()
        draft = context.user_data.get("draft")
        if draft is None:
            await query.edit_message_text("This wizard expired. Tap ➕ Add monitor to restart.")
            return ConversationHandler.END
        data = query.data
        parts = data.split(":")

        async def render(text, markup):
            await query.edit_message_text(text, reply_markup=markup, parse_mode=ParseMode.HTML)
            return AREAS

        if data == "wz:cancel":
            context.user_data.clear()
            await query.edit_message_text("Cancelled. Nothing was saved.")
            return ConversationHandler.END
        if data == "az:home":
            return await render(*_city_picker_screen(draft))
        if parts[:2] == ["az", "city"]:
            rid, cid = int(parts[2]), int(parts[3])
            name = locations.city_name(cid)
            context.user_data["browse"] = {"rid": rid, "cid": cid, "name": name}
            catalog = locations.load_city_hoods(cid)
            if catalog and catalog.get("quarters"):        # bundled → curated quarters
                return await render(*_quarters_screen(rid, cid, name))
            await query.edit_message_text(f"⏳ Loading {name} neighborhoods…")
            hoods = await locations.fetch_city_hoods(self.http, rid, cid)
            if not hoods:
                rows = [[_watch_all_button(rid, cid, name)],
                        [InlineKeyboardButton("↩ Cities", callback_data="az:home")]]
                await query.edit_message_text(
                    f"I couldn't list neighborhoods for <b>{name}</b> right now — you can still "
                    f"watch the whole city:", reply_markup=InlineKeyboardMarkup(rows), parse_mode=ParseMode.HTML)
                return AREAS
            context.user_data["browse"]["hoods"] = hoods
            return await render(*_flat_hoods_screen(draft, context.user_data["browse"]))
        if parts[:2] == ["az", "whole"]:
            rid, cid = int(parts[2]), int(parts[3])
            _add_location(draft, LocationTarget(level="city", region_id=rid, city_id=cid,
                                               display_name=locations.city_name(cid)))
            return await render(*_city_picker_screen(draft))
        if parts[:2] == ["az", "q"]:
            return await render(*_hoods_screen(draft, int(parts[2]), int(parts[3]), int(parts[4])))
        if parts[:2] == ["az", "h"]:
            self._toggle_bundled_hood(draft, int(parts[2]), int(parts[3]), int(parts[4]), int(parts[5]))
            return await render(*_hoods_screen(draft, int(parts[2]), int(parts[3]), int(parts[4])))
        if parts[:2] == ["az", "bh"]:
            self._toggle_flat_hood(draft, context.user_data.get("browse"), int(parts[2]))
            return await render(*_flat_hoods_screen(draft, context.user_data.get("browse", {"rid": 0, "cid": 0, "name": "", "hoods": []})))
        if parts[:2] == ["az", "tog"]:
            cands = context.user_data.get("cands", [])
            idx = int(parts[2])
            if idx < len(cands):
                self._toggle_candidate(draft, cands[idx])
            return await render(*_search_results_screen(
                draft, context.user_data.get("cand_cities", []), cands))
        if parts[:2] == ["az", "rm"]:
            idx = int(parts[2])
            if idx < len(draft["locations"]):
                draft["locations"].pop(idx)
            return await render(*_city_picker_screen(draft))
        if data == "az:done":
            if not draft["locations"]:
                await query.answer("Add at least one area first.", show_alert=True)
                return AREAS
            text, markup = _render_card(draft)
            await query.edit_message_text(text, reply_markup=markup, parse_mode=ParseMode.HTML)
            return FORM
        return AREAS

    def _toggle_bundled_hood(self, draft, region_id, city_id, q_idx, h_idx) -> None:
        quarters = locations.city_quarters(city_id)
        if q_idx >= len(quarters) or h_idx >= len(quarters[q_idx]["hoods"]):
            return
        hood = quarters[q_idx]["hoods"][h_idx]
        if not self._remove_hood_if_present(draft, hood):
            catalog = locations.load_city_hoods(city_id) or {}
            _add_location(draft, locations.hood_target(catalog, hood, locations.city_name(city_id)))

    def _toggle_candidate(self, draft, cand) -> None:
        """Add a typed-search result if absent, else remove it (multi-select)."""
        target = locations.target_from_resolved(cand)
        key = _loc_key(target)
        before = len(draft["locations"])
        draft["locations"] = [t for t in draft["locations"] if _loc_key(t) != key]
        if len(draft["locations"]) == before:
            _add_location(draft, target)

    def _toggle_flat_hood(self, draft, browse, idx) -> None:
        if not browse or idx >= len(browse.get("hoods", [])):
            return
        hood = browse["hoods"][idx]
        if not self._remove_hood_if_present(draft, hood):
            _add_location(draft, locations.make_hood_target(
                browse["rid"], browse["cid"], hood, browse.get("name", "")))

    def _remove_hood_if_present(self, draft, hood_name) -> bool:
        from models import normalize_name
        norm = normalize_name(hood_name)
        before = len(draft["locations"])
        draft["locations"] = [t for t in draft["locations"]
                             if not (t.level == "hood" and t.match_name == norm)]
        return len(draft["locations"]) < before

    async def area_search_text(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        draft = context.user_data.get("draft")
        if draft is None:
            return ConversationHandler.END
        text = (update.effective_message.text or "").strip()
        # Immediate feedback so the user knows we're working on it.
        try:
            await update.effective_chat.send_action("typing")
        except Exception:
            pass
        status = await update.effective_message.reply_text(f"🔎 Searching for “{text}”…")
        cands = await locations.resolve_candidates(self.http, text)
        if not cands:
            await status.edit_text(
                "🤷 Couldn't find that. Try <b>Hebrew</b> — e.g. תל אביב, רמת גן, קרית ביאליק.",
                parse_mode=ParseMode.HTML)
            return AREAS
        cities = [c for c in cands if c.level == "city"]
        others = [c for c in cands if c.level != "city"]
        context.user_data["cands"] = others
        context.user_data["cand_cities"] = cities
        text, markup = _search_results_screen(draft, cities, others)
        await status.edit_text(text, reply_markup=markup, parse_mode=ParseMode.HTML)
        return AREAS

    # form state
    async def form_cb(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        query = update.callback_query
        await query.answer()
        draft = context.user_data.get("draft")
        if draft is None:
            await query.edit_message_text("This wizard expired. Tap ➕ Add monitor to restart.")
            return ConversationHandler.END
        data = query.data
        parts = data.split(":")

        if data == "wz:cancel":
            context.user_data.clear()
            await query.edit_message_text("Cancelled. Nothing was saved.")
            return ConversationHandler.END
        if data == "wz:save":
            return await self._save_draft(update, context)
        if data == "az:home":
            text, markup = _city_picker_screen(draft)
            await query.edit_message_text(text, reply_markup=markup, parse_mode=ParseMode.HTML)
            return AREAS
        if parts[0] == "rng":
            return await self._range_cb(query, context, draft, parts)
        if data == "wz:toggle:src":
            draft["source_mode"] = "yad2" if draft["source_mode"] == "auto" else "auto"
        elif data == "wz:types":
            return await self._show_types(query, draft)
        elif data == "wz:cond":
            return await self._show_cond(query, draft)
        elif data == "wz:feats":
            return await self._show_feats(query, draft)
        elif parts[:2] == ["wz", "field"]:
            title, markup = _range_screen(parts[2], "min", draft)
            await query.edit_message_text(title, reply_markup=markup, parse_mode=ParseMode.HTML)
            return FORM
        elif parts[0] == "pt":       # property-type toggle / done
            if parts[1] == "done":
                pass
            else:
                self._toggle_type(draft, int(parts[1]))
                title, markup = _multi_toggle_screen(
                    "🏠 <b>Property type</b> (tap to select; none = all):",
                    [(t, PROPERTY_TYPES[t]) for t in PROPERTY_TYPES],
                    set(draft["property_types"] or []), "pt:", "pt:done")
                await query.edit_message_text(title, reply_markup=markup, parse_mode=ParseMode.HTML)
                return FORM
        elif parts[0] == "cond":     # condition set / clear
            draft["property_condition"] = None if parts[1] == "any" else int(parts[1])
        elif parts[0] == "ft":       # feature toggle / done
            if parts[1] != "done":
                self._toggle_feature(draft, parts[1])
                title, markup = self._feats_markup(draft)
                await query.edit_message_text(title, reply_markup=markup, parse_mode=ParseMode.HTML)
                return FORM

        text, markup = _render_card(draft)
        await query.edit_message_text(text, reply_markup=markup, parse_mode=ParseMode.HTML)
        return FORM

    async def _show_types(self, query, draft):
        title, markup = _multi_toggle_screen(
            "🏠 <b>Property type</b> (tap to select; none = all):",
            [(t, PROPERTY_TYPES[t]) for t in PROPERTY_TYPES],
            set(draft["property_types"] or []), "pt:", "pt:done")
        await query.edit_message_text(title, reply_markup=markup, parse_mode=ParseMode.HTML)
        return FORM

    async def _show_cond(self, query, draft):
        rows, row = [], []
        for cid, label in PROPERTY_CONDITIONS.items():
            mark = "✅ " if draft["property_condition"] == cid else ""
            row.append(InlineKeyboardButton(f"{mark}{label}", callback_data=f"cond:{cid}"))
            if len(row) == 2:
                rows.append(row); row = []
        if row:
            rows.append(row)
        rows.append([InlineKeyboardButton("Any condition", callback_data="cond:any")])
        await query.edit_message_text("🛠 <b>Condition</b>:", reply_markup=InlineKeyboardMarkup(rows),
                                     parse_mode=ParseMode.HTML)
        return FORM

    def _feats_markup(self, draft):
        return _multi_toggle_screen(
            "✨ <b>Required features</b> (tap to require):",
            [(k, feature_label(k)) for k in FEATURES], set(draft["features"]), "ft:", "ft:done")

    async def _show_feats(self, query, draft):
        title, markup = self._feats_markup(draft)
        await query.edit_message_text(title, reply_markup=markup, parse_mode=ParseMode.HTML)
        return FORM

    def _toggle_type(self, draft: dict, type_id: int) -> None:
        cur = set(draft["property_types"] or [])
        cur.symmetric_difference_update({type_id})
        draft["property_types"] = sorted(cur) or None

    def _toggle_feature(self, draft: dict, key: str) -> None:
        if key in draft["features"]:
            draft["features"].remove(key)
        elif key in FEATURES:
            draft["features"].append(key)

    async def _range_cb(self, query, context, draft, parts):
        if parts[1] == "cardback":
            text, markup = _render_card(draft)
            await query.edit_message_text(text, reply_markup=markup, parse_mode=ParseMode.HTML)
            return FORM
        field, bound, value = parts[1], parts[2], parts[3]
        spec = _FIELD_SPECS[field]
        if value == "custom":
            context.user_data["editing"] = (field, bound)
            await query.edit_message_text(_custom_prompt(field, bound), parse_mode=ParseMode.HTML)
            return AWAIT_VALUE
        draft[spec[bound]] = None if value == "none" else (float(value) if spec["float"] else int(value))
        if bound == "min":
            title, markup = _range_screen(field, "max", draft)
            await query.edit_message_text(title, reply_markup=markup, parse_mode=ParseMode.HTML)
        else:
            text, markup = _render_card(draft)
            await query.edit_message_text(text, reply_markup=markup, parse_mode=ParseMode.HTML)
        return FORM

    async def await_value_text(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        draft = context.user_data.get("draft")
        editing = context.user_data.get("editing")
        if not draft or not editing:
            return FORM
        field, bound = editing
        spec = _FIELD_SPECS[field]
        val = _parse_number(update.effective_message.text, spec["float"])
        if val == "ERR" or val < 0:
            await update.effective_message.reply_text(
                "Please send a non-negative number, e.g. " + spec["example"] + ".")
            return AWAIT_VALUE
        draft[spec[bound]] = val
        context.user_data.pop("editing", None)
        if bound == "min":
            title, markup = _range_screen(field, "max", draft)
            await update.effective_message.reply_text(title, reply_markup=markup, parse_mode=ParseMode.HTML)
        else:
            text, markup = _render_card(draft)
            await update.effective_message.reply_text(text, reply_markup=markup, parse_mode=ParseMode.HTML)
        return FORM

    async def _save_draft(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        query = update.callback_query
        draft = context.user_data["draft"]
        chat_id = update.effective_chat.id
        interval = max(1, config.CHECK_INTERVAL_SECONDS // 60)
        n = len(draft["locations"])
        areas = ", ".join(t.display_name for t in draft["locations"][:4])
        if n > 4:
            areas += f" +{n - 4} more"

        if draft.get("edit_id"):
            await self.db.update_search_filters(
                draft["edit_id"], label=draft["label"],
                min_price=draft["min_price"], max_price=draft["max_price"],
                min_rooms=draft["min_rooms"], max_rooms=draft["max_rooms"],
                min_sqm=draft["min_sqm"], max_sqm=draft["max_sqm"],
                min_floor=draft["min_floor"], max_floor=draft["max_floor"],
                property_types=draft["property_types"], property_condition=draft["property_condition"],
                features=draft["features"], source_mode=draft["source_mode"])
            await self.db.set_search_locations(draft["edit_id"], draft["locations"])
            await self.db.reprime_search(draft["edit_id"])
            await query.edit_message_text(
                f"✅ <b>Monitor updated</b> — {areas}.\n"
                f"🔎 Fetching a fresh sample now, then I'll keep watching every {interval} minutes…",
                parse_mode=ParseMode.HTML)
            self._scan_soon(draft["edit_id"])
        else:
            await self.db.upsert_user(chat_id, update.effective_user.username if update.effective_user else None)
            search = SavedSearch(
                id=0, chat_id=chat_id, label=draft["label"],
                min_price=draft["min_price"], max_price=draft["max_price"],
                min_rooms=draft["min_rooms"], max_rooms=draft["max_rooms"],
                min_sqm=draft["min_sqm"], max_sqm=draft["max_sqm"],
                min_floor=draft["min_floor"], max_floor=draft["max_floor"],
                property_types=draft["property_types"], property_condition=draft["property_condition"],
                features=draft["features"], source_mode=draft["source_mode"],
                is_active=True, is_primed=False)
            new_id = await self.db.add_search(search, draft["locations"])
            await query.edit_message_text(
                f"✅ <b>Monitor created</b> — {areas} ({n} area{'s' if n != 1 else ''}).\n\n"
                f"🔎 Fetching a <b>sample of current matches</b> now… then I'll check "
                f"<b>every {interval} minutes</b> and alert you about new listings. 🎯",
                parse_mode=ParseMode.HTML)
            self._scan_soon(new_id)
        context.user_data.clear()
        return ConversationHandler.END

    async def wiz_cancel(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        context.user_data.clear()
        await update.effective_message.reply_text("Cancelled.", reply_markup=bar_keyboard())
        return ConversationHandler.END

    # --- daily HTML report ---
    async def daily_report(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        chat_id = update.effective_chat.id
        # Rate limit: once per REPORT_COOLDOWN_HOURS per user (anti-spam).
        last_raw = await self.db.get_meta(f"last_report:{chat_id}")
        if last_raw:
            try:
                last_dt = datetime.datetime.fromisoformat(last_raw)
                elapsed = datetime.datetime.utcnow() - last_dt
                cooldown = datetime.timedelta(hours=config.REPORT_COOLDOWN_HOURS)
                if elapsed < cooldown:
                    hrs = int((cooldown - elapsed).total_seconds() // 3600) + 1
                    await update.effective_message.reply_text(
                        f"📄 You already got a report recently. You can request a fresh one in about "
                        f"<b>{hrs}h</b> (once per {config.REPORT_COOLDOWN_HOURS}h to avoid spam).",
                        parse_mode=ParseMode.HTML)
                    return
            except ValueError:
                pass

        searches = [s for s in await self.db.list_searches(chat_id) if s.is_active]
        if not searches:
            await update.effective_message.reply_text(
                "You have no active monitors yet. Tap ➕ Add monitor first.")
            return

        try:
            await update.effective_chat.send_action("upload_document")
        except Exception:
            pass
        status = await update.effective_message.reply_text(
            "⏳ Building your report — scanning all your monitors, this can take a few seconds…")

        listings = await engine.collect_user_matches(self.db, self.http, chat_id)
        if not listings:
            await status.edit_text("No current matches across your monitors right now. Try again later.")
            return

        when = datetime.datetime.utcnow().strftime("%Y-%m-%d %H:%M UTC")
        page = report.render(listings, when=when)
        path = None
        try:
            with tempfile.NamedTemporaryFile("w", suffix=".html", delete=False, encoding="utf-8") as fh:
                fh.write(page)
                path = fh.name
            with open(path, "rb") as doc:
                await update.effective_message.reply_document(
                    document=doc,
                    filename=f"apartments-{datetime.datetime.utcnow():%Y-%m-%d}.html",
                    caption=(f"📄 <b>{len(listings)} apartments</b> across your monitors. "
                             f"Open it on your phone 📱 — tap any card to view the listing."),
                    parse_mode=ParseMode.HTML)
            await self.db.set_meta(f"last_report:{chat_id}", datetime.datetime.utcnow().isoformat())
            try:
                await status.delete()
            except Exception:
                pass
        finally:
            if path and os.path.exists(path):
                os.remove(path)   # temp file: send then delete

    # --- my searches management ---
    async def mysearches(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        await self._show_searches(update, context, edit=False)

    def _search_summary(self, s: SavedSearch) -> str:
        areas = ", ".join(t.display_name for t in s.locations[:3])
        if len(s.locations) > 3:
            areas += f" +{len(s.locations) - 3}"
        bits = [f"₪{_fmt_range(s.min_price, s.max_price)}", f"{_fmt_range(s.min_rooms, s.max_rooms)} rm"]
        if s.features:
            bits.append("✨" + str(len(s.features)))
        return f"{areas or '—'} · " + " · ".join(bits)

    async def _show_searches(self, update: Update, context: ContextTypes.DEFAULT_TYPE, edit: bool):
        chat_id = update.effective_chat.id
        searches = await self.db.list_searches(chat_id)
        if not searches:
            text = "You have no monitors yet.\n\nTap <b>➕ Add monitor</b> to create one."
            markup = None
        else:
            lines = ["<b>📋 Your monitors</b>\n"]
            rows = []
            for s in searches:
                status = "🟢" if s.is_active else "⏸"
                lines.append(f"{status} {self._search_summary(s)}")
                label = (s.label or "monitor")[:16]
                rows.append([
                    InlineKeyboardButton(f"✏ {label}", callback_data=f"ms:edit:{s.id}"),
                    InlineKeyboardButton("▶" if not s.is_active else "⏸", callback_data=f"ms:toggle:{s.id}"),
                    InlineKeyboardButton("🗑", callback_data=f"ms:del:{s.id}"),
                ])
            text = "\n".join(lines)
            markup = InlineKeyboardMarkup(rows)

        if edit and update.callback_query:
            await update.callback_query.edit_message_text(text, reply_markup=markup, parse_mode=ParseMode.HTML)
        else:
            await update.effective_message.reply_text(
                text, reply_markup=markup or bar_keyboard(), parse_mode=ParseMode.HTML)

    async def manage_button(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        query = update.callback_query
        await query.answer()
        parts = query.data.split(":")
        action, search_id = parts[1], int(parts[2])
        search = await self.db.get_search(search_id)
        if not search or search.chat_id != update.effective_chat.id:
            await query.edit_message_text("Monitor not found.")
            return
        if action == "toggle":
            now_active = not search.is_active
            await self.db.set_search_active(search_id, now_active)
            if now_active:
                await self.db.reprime_search(search_id)
            await self._show_searches(update, context, edit=True)
        elif action == "del":
            markup = InlineKeyboardMarkup([[
                InlineKeyboardButton("🗑 Yes, delete", callback_data=f"ms:delok:{search_id}"),
                InlineKeyboardButton("↩ Keep", callback_data="ms:back"),
            ]])
            await query.edit_message_text(
                f"Delete monitor <b>{search.label or '—'}</b>?", reply_markup=markup, parse_mode=ParseMode.HTML)
        elif action == "delok":
            await self.db.delete_search(search_id)
            await self._show_searches(update, context, edit=True)

    async def manage_back(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        await update.callback_query.answer()
        await self._show_searches(update, context, edit=True)

    def build_conversation(self) -> ConversationHandler:
        area_text = MessageHandler(
            filters.TEXT & ~filters.COMMAND & ~filters.Regex(BAR_REGEX), self.area_search_text)
        return ConversationHandler(
            entry_points=[
                CommandHandler("add", self.add_entry),
                MessageHandler(filters.Regex(f"^{BAR_ADD}$"), self.add_entry),
                CallbackQueryHandler(self.add_entry, pattern=r"^home:add$"),
                CallbackQueryHandler(self.edit_entry, pattern=r"^ms:edit:\d+$"),
            ],
            states={
                # Typing anywhere in the area step searches; buttons browse/add/remove.
                AREAS: [
                    CallbackQueryHandler(self.areas_cb, pattern=r"^(az:|wz:cancel)"),
                    area_text,
                ],
                FORM: [CallbackQueryHandler(self.form_cb, pattern=r"^(wz:|az:home|pt:|cond:|ft:|rng:)")],
                AWAIT_VALUE: [MessageHandler(
                    filters.TEXT & ~filters.COMMAND & ~filters.Regex(BAR_REGEX), self.await_value_text)],
            },
            fallbacks=[CommandHandler("cancel", self.wiz_cancel)],
            conversation_timeout=600,
            per_message=False,
            allow_reentry=True,   # tapping ➕ Add monitor restarts even mid-wizard
        )


# Guided min→max range editing: quick-pick buttons + "No min/max" + Custom.
_FIELD_SPECS = {
    "price": {"min": "min_price", "max": "max_price", "float": False, "title": "Price", "emoji": "💰",
              "min_opts": [2000, 3000, 4000, 5000], "max_opts": [5000, 6000, 7000, 8000, 10000], "example": "6500"},
    "rooms": {"min": "min_rooms", "max": "max_rooms", "float": True, "title": "Rooms", "emoji": "🛏",
              "min_opts": [1, 2, 3, 4], "max_opts": [3, 4, 5, 6], "example": "3"},
    "sqm": {"min": "min_sqm", "max": "max_sqm", "float": False, "title": "Size", "emoji": "📐",
            "min_opts": [40, 50, 60, 70], "max_opts": [80, 100, 120, 150], "example": "70"},
    "floor": {"min": "min_floor", "max": "max_floor", "float": False, "title": "Floor", "emoji": "🏢",
              "min_opts": [0, 1, 2, 3], "max_opts": [3, 5, 10, 20], "example": "5"},
}


def _fmt_val(v, field: str) -> str:
    if v is None:
        return "Any"
    if field == "price":
        return f"₪{int(v):,}"
    if field == "sqm":
        return f"{int(v)} m²"
    if field == "rooms":
        return str(int(v)) if float(v).is_integer() else str(v)
    return str(int(v))


def _range_screen(field: str, bound: str, draft: dict) -> tuple:
    spec = _FIELD_SPECS[field]
    is_min = bound == "min"
    lines = [f"{spec['emoji']} <b>{spec['title']} — {'minimum' if is_min else 'maximum'}</b>"]
    if is_min:
        lines.append("Step 1 of 2 — pick a lower bound (or none):")
    else:
        if draft[spec["min"]] is not None:
            lines.append(f"Minimum set to <b>{_fmt_val(draft[spec['min']], field)}</b>.")
        lines.append("Step 2 of 2 — pick an upper bound (or none):")
    buttons = [InlineKeyboardButton(f"🚫 No {'minimum' if is_min else 'maximum'}",
                                   callback_data=f"rng:{field}:{bound}:none")]
    for v in spec[f"{bound}_opts"]:
        buttons.append(InlineKeyboardButton(_fmt_val(v, field), callback_data=f"rng:{field}:{bound}:{v}"))
    buttons.append(InlineKeyboardButton("✏ Custom…", callback_data=f"rng:{field}:{bound}:custom"))
    rows = [buttons[i:i + 3] for i in range(0, len(buttons), 3)]
    rows.append([InlineKeyboardButton("↩ Back to filters", callback_data="rng:cardback")])
    return "\n".join(lines), InlineKeyboardMarkup(rows)


def _custom_prompt(field: str, bound: str) -> str:
    spec = _FIELD_SPECS[field]
    return (f"✏ Type the <b>{'minimum' if bound == 'min' else 'maximum'} "
            f"{spec['title'].lower()}</b> as a number (e.g. <code>{spec['example']}</code>):")


def _parse_number(text: str, allow_float: bool):
    text = (text or "").strip().replace(",", "").replace(" ", "")
    if text == "":
        return "ERR"
    try:
        return float(text) if allow_float else int(text)
    except ValueError:
        return "ERR"
