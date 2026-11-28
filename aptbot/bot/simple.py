"""The command set: one Tel Aviv monitor per chat.

/monitor starts (or resumes) watching Tel Aviv on Yad2 and the Facebook groups,
/stop pauses it, /status shows both sources and the filters, /help explains.
/filters (the inline-keyboard card) lives in ``aptbot.bot.filters``.
Every reply carries the state-aware control menu (``aptbot.bot.menu``); its taps
land in ``menu_tap``.
"""
from __future__ import annotations

import asyncio
import logging
from datetime import datetime, timezone
from typing import Awaitable, Callable, Optional
from zoneinfo import ZoneInfo

from telegram import Update
from telegram.constants import ParseMode
from telegram.ext import ContextTypes

from aptbot import config
from aptbot.bot import menu
from aptbot.bot.filters import filters_summary, has_filters
from aptbot.db import Database
from aptbot.models import SavedSearch
from aptbot.scrapers.yad2 import locations

logger = logging.getLogger(__name__)

MONITOR_LABEL = "Tel Aviv"

_monitor_locks: dict = {}
# Background tasks keep a reference here until done (asyncio only holds weak ones).
_tasks: set = set()


def spawn(coro) -> "asyncio.Task":
    task = asyncio.create_task(coro)
    _tasks.add(task)
    task.add_done_callback(_tasks.discard)
    return task


def monitor_targets():
    return locations.monitor_targets(config.TEL_AVIV_CITY_ID, config.YAD2_HOODS)


def area_summary() -> str:
    if not config.YAD2_HOODS:
        return "all of Tel Aviv"
    return ", ".join(config.YAD2_HOODS)


def _interval_min() -> int:
    return max(1, config.CHECK_INTERVAL_SECONDS // 60)


def help_text(fb_on: bool) -> str:
    fb = (f"• <b>Facebook</b>: {len(config.FB_GROUP_IDS)} Tel Aviv apartment groups, every "
          f"{config.FB_POLL_MIN_SECONDS // 60}-{config.FB_POLL_MAX_SECONDS // 60} minutes. "
          "Each post once, with its photos, even if it's cross-posted.\n") if fb_on else ""
    return (
        "<b>🏠 Apartment watcher: Tel Aviv</b>\n\n"
        f"• <b>Yad2</b>: rentals in {area_summary()}, checked every {_interval_min()} minutes "
        "(new listings and price changes).\n"
        f"{fb}\n"
        "Use the button panel (the ⌨️ icon next to the text box), or send a command:\n"
        "/monitor: start watching (you get a short taster of what's there now)\n"
        "/filters: price, rooms, size, floor, type, condition and features like ממ״ד. "
        "A listing that doesn't mention a field is kept and marked ❔\n"
        "/stop: pause\n"
        "/status: what each source did last\n"
        "/help: this message"
    )


def _when(dt: Optional[datetime]) -> str:
    if dt is None:
        return "not yet"
    return dt.astimezone(ZoneInfo("Asia/Jerusalem")).strftime("%d/%m %H:%M")


class SimpleCommands:
    def __init__(self, db: Database, *, on_started: Callable[[int], Awaitable[None]],
                 fb_state=None, fb_api=None, analyzer=None):
        self.db = db
        self.on_started = on_started   # called with the search id: immediate Yad2 scan + wake FB
        self.fb_state = fb_state
        self.analyzer = analyzer
        self.fb_api = fb_api

    async def sync_areas(self) -> int:
        """At startup: point every existing monitor at the configured Yad2 areas (so an
        area change in config/.env applies without anyone re-sending /monitor)."""
        targets = monitor_targets()
        changed = 0
        for s in await self.db.get_active_searches():
            if _loc_keys(s.locations) != _loc_keys(targets):
                await self.db.set_search_locations(s.id, targets)
                await self.db.reprime_search(s.id)    # new areas: a taster, not a flood of "new"
                changed += 1
        if changed:
            logger.info("Yad2 areas updated on %d monitor(s): %s", changed, area_summary())
        return changed

    async def _own_search(self, chat_id: int) -> Optional[SavedSearch]:
        searches = await self.db.list_searches(chat_id)
        return searches[0] if searches else None

    async def _reply(self, update: Update, text: str) -> None:
        """Reply with the control menu for the chat's current state underneath."""
        s = await self._own_search(update.effective_chat.id)
        await update.effective_message.reply_text(
            text, parse_mode=ParseMode.HTML, reply_markup=menu.menu_markup(s))

    async def start(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """/start and /help bring up the persistent button panel."""
        await update.effective_message.reply_text(
            help_text(self.fb_api is not None), parse_mode=ParseMode.HTML, reply_markup=menu.keyboard())

    help = start

    async def menu_tap(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """A control-menu button: retire that menu, then run the command (its reply
        brings a fresh menu)."""
        query = update.callback_query
        await query.answer()
        await menu.retire(query)
        action = query.data.split(":", 1)[1]
        await {"monitor": self.monitor, "stop": self.stop,
               "status": self.status, "help": self.help}[action](update, context)

    async def key_tap(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """A bottom-panel button, or any supported "/cmd[@name]" the CommandHandlers didn't take."""
        text = (update.effective_message.text or "").strip()
        action = menu.KEYS.get(text) or menu.command_of(text)
        action = {"start": "help"}.get(action, action)
        fn = {"monitor": self.monitor, "stop": self.stop,
              "status": self.status, "help": self.help}.get(action)
        if fn is not None:
            await fn(update, context)

    async def monitor(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        # One /monitor per chat at a time: a double tap must not create two monitors.
        async with _monitor_locks.setdefault(update.effective_chat.id, asyncio.Lock()):
            await self._monitor(update, context)

    async def _monitor(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        chat = update.effective_chat
        user = update.effective_user
        await self.db.upsert_user(chat.id, user.username if user else None)
        await self.db.set_user_active(chat.id, True)
        s = await self._own_search(chat.id)
        targets = monitor_targets()
        if s and s.is_active:
            if _loc_keys(s.locations) != _loc_keys(targets):     # the configured areas changed
                await self.db.set_search_locations(s.id, targets)
                await self.db.reprime_search(s.id)
                await self._reply(update, f"✅ Already watching; Yad2 areas updated to: {area_summary()}.")
                return
            await self._reply(update, "✅ Already watching Tel Aviv.")
            return
        if s:
            await self.db.set_search_locations(s.id, targets)
            await self.db.set_search_active(s.id, True)
            await self.db.reprime_search(s.id)
            sid = s.id
        else:
            sid = await self.db.add_search(
                SavedSearch(id=0, chat_id=chat.id, label=MONITOR_LABEL), targets)
        sources = "Yad2 and Facebook" if self.fb_api is not None else "Yad2"
        await self._reply(update, f"👀 Watching <b>Tel Aviv</b> on {sources}. A short taster is on "
                                  "its way, then only new posts.")
        logger.info("Monitor #%s started in chat %s", sid, chat.id)
        spawn(self.on_started(sid))

    async def stop(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        s = await self._own_search(update.effective_chat.id)
        if not s or not s.is_active:
            await self._reply(update, "Not watching anything right now.")
            return
        await self.db.set_search_active(s.id, False)
        await self._reply(update, "⏸ Paused. Your filters are kept.")

    async def status(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        s = await self._own_search(update.effective_chat.id)
        lines = [f"<b>Monitor:</b> {'on' if s and s.is_active else 'off'}",
                 f"<b>Yad2 areas:</b> {area_summary()}"]
        if s is not None:
            lines.append("<b>Filters:</b> " + ("\n" + filters_summary(s) if has_filters(s)
                                                 else "none"))
        last = await self.db.get_meta("yad2_last_cycle")
        lines.append(f"<b>Yad2:</b> every {_interval_min()} min, last check "
                     f"{_when(datetime.fromisoformat(last)) if last else 'not yet'}")
        if self.fb_api is None:
            lines.append("<b>Facebook:</b> disabled")
        else:
            st = self.fb_state
            if st.stopped_reason:
                fb = f"⚠️ stopped ({st.stopped_reason})"
            else:
                fb = (f"last poll {_when(st.last_cycle_at)}, {st.last_new} new post(s), "
                      f"{st.last_sent} sent")
            lines.append(f"<b>Facebook:</b> {fb}")
            lines.append(f"<b>AI summaries:</b> {_ai_status(self.analyzer)}")
        await self._reply(update, "\n".join(lines))


def _ai_status(analyzer) -> str:
    if analyzer is None:
        return "off"
    st = analyzer.state
    if analyzer.paused:
        return f"⏸ paused (ChatGPT usage limit) until {_when(st.paused_until)}"
    text = f"on, {st.analyzed} post(s) analyzed"
    if st.failed:
        text += f", {st.failed} sent without a summary"
    return text


def _loc_keys(targets) -> set:
    return {(t.level, t.city_id, t.match_name) for t in targets}


def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")
