"""The current, minimal command set: one Tel Aviv monitor per chat, no filters.

/monitor starts (or resumes) watching Tel Aviv on Yad2 and the Facebook groups,
/stop pauses it, /status shows both sources, /help explains. The full filters
wizard (``aptbot.bot.handlers``) is parked until filters come back.
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
from aptbot.db import Database
from aptbot.models import SavedSearch
from aptbot.scrapers.yad2 import locations

logger = logging.getLogger(__name__)

MONITOR_LABEL = "תל אביב"


def _interval_min() -> int:
    return max(1, config.CHECK_INTERVAL_SECONDS // 60)


def help_text(fb_on: bool) -> str:
    fb = (f"• <b>Facebook</b>: {len(config.FB_GROUP_IDS)} Tel Aviv apartment groups, every "
          f"{config.FB_POLL_MIN_SECONDS // 60}-{config.FB_POLL_MAX_SECONDS // 60} minutes. "
          "Each post once, with its photos, even if it's cross-posted.\n") if fb_on else ""
    return (
        "<b>🏠 Apartment watcher: Tel Aviv</b>\n\n"
        f"• <b>Yad2</b>: every rental in Tel Aviv, checked every {_interval_min()} minutes "
        "(new listings and price changes).\n"
        f"{fb}\n"
        "/monitor: start watching (you get a short taster of what's there now)\n"
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
                 fb_state=None, fb_api=None):
        self.db = db
        self.on_started = on_started   # called with the search id: immediate Yad2 scan + wake FB
        self.fb_state = fb_state
        self.fb_api = fb_api

    async def _own_search(self, chat_id: int) -> Optional[SavedSearch]:
        searches = await self.db.list_searches(chat_id)
        return searches[0] if searches else None

    async def start(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        await update.effective_message.reply_text(
            help_text(self.fb_api is not None), parse_mode=ParseMode.HTML)

    help = start

    async def monitor(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        chat = update.effective_chat
        user = update.effective_user
        await self.db.upsert_user(chat.id, user.username if user else None)
        await self.db.set_user_active(chat.id, True)
        s = await self._own_search(chat.id)
        if s and s.is_active:
            await update.effective_message.reply_text(
                "✅ Already watching Tel Aviv. /status shows what's happening.")
            return
        if s:
            await self.db.set_search_active(s.id, True)
            await self.db.reprime_search(s.id)
            sid = s.id
        else:
            target = locations.city_target(config.TEL_AVIV_CITY_ID)
            sid = await self.db.add_search(
                SavedSearch(id=0, chat_id=chat.id, label=MONITOR_LABEL), [target])
        sources = "Yad2 and Facebook" if self.fb_api is not None else "Yad2"
        await update.effective_message.reply_text(
            f"👀 Watching <b>Tel Aviv</b> on {sources}. A short taster is on its way, "
            "then only new posts.", parse_mode=ParseMode.HTML)
        logger.info("Monitor #%s started in chat %s", sid, chat.id)
        asyncio.create_task(self.on_started(sid))

    async def stop(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        s = await self._own_search(update.effective_chat.id)
        if not s or not s.is_active:
            await update.effective_message.reply_text("Not watching anything. /monitor to start.")
            return
        await self.db.set_search_active(s.id, False)
        await update.effective_message.reply_text("⏸ Paused. /monitor resumes it.")

    async def status(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        s = await self._own_search(update.effective_chat.id)
        lines = [f"<b>Monitor:</b> {'on' if s and s.is_active else 'off'} (Tel Aviv, no filters)"]
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
            try:
                budget = (await self.fb_api.status()).budget_left
                fb += f", budget {budget}/hour left"
            except Exception:
                pass
            lines.append(f"<b>Facebook:</b> {fb}")
        await update.effective_message.reply_text("\n".join(lines), parse_mode=ParseMode.HTML)


def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")
