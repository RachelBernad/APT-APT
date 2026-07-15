# telegram_bot.py
"""Entry point: a multi-user apartment bot.

Runs a python-telegram-bot polling app (per-user saved searches) and, in the
same event loop, a periodic scrape/match/route cycle. State lives in SQLite.
"""
import asyncio
import logging
import random

import aiohttp
from telegram import BotCommand
from telegram.error import Forbidden, RetryAfter, TelegramError
from telegram.ext import (Application, CallbackQueryHandler, CommandHandler,
                          MessageHandler, filters)

import config
import engine
from db import Database
from handlers import BAR_HELP, BAR_LIST, BAR_REPORT, BotHandlers

config.setup_logging()
logger = logging.getLogger(__name__)


class Bot:
    def __init__(self, token: str):
        self.token = token
        # concurrent_updates(True): each update (button tap, keystroke) runs in its
        # own task instead of queuing behind the previous handler, so toggles feel
        # instant even while an autocomplete lookup or scan is in flight.
        self.application: Application = (
            Application.builder().token(token).concurrent_updates(True).build()
        )
        self.db: Database = Database(config.DB_PATH)
        self.http: aiohttp.ClientSession | None = None
        self.handlers: BotHandlers | None = None

    async def send(self, chat_id: int, text: str) -> bool:
        """Send an HTML message with flood-control + blocked-user handling."""
        try:
            await self.application.bot.send_message(
                chat_id=chat_id, text=text, parse_mode="HTML",
                disable_web_page_preview=True,
            )
            return True
        except RetryAfter as exc:
            wait = int(getattr(exc, "retry_after", 5)) + random.randint(1, 5)
            logger.info("Flood control: waiting %ss before retrying chat %s", wait, chat_id)
            await asyncio.sleep(wait)
            try:
                await self.application.bot.send_message(
                    chat_id=chat_id, text=text, parse_mode="HTML", disable_web_page_preview=True
                )
                return True
            except TelegramError as retry_exc:
                logger.error("Retry failed for chat %s: %s", chat_id, retry_exc)
                return False
        except Forbidden:
            logger.info("Chat %s blocked/removed the bot; deactivating.", chat_id)
            await self.db.set_user_active(chat_id, False)
            return False
        except TelegramError as exc:
            logger.error("Telegram error sending to chat %s: %s", chat_id, exc)
            return False

    async def scan_now(self, search_id: int):
        """Immediately scan one just-saved search (backfill sample within seconds)."""
        try:
            await engine.scan_search(self.send, self.db, self.http, search_id)
        except Exception as exc:
            logger.error("Immediate scan for search #%s failed: %s", search_id, exc)

    async def _cycle_loop(self):
        # Small initial delay so polling is fully up before the first scrape.
        await asyncio.sleep(5)
        while True:
            try:
                await engine.run_cycle(self.send, self.db, self.http)
            except Exception as exc:
                logger.error("Unexpected error in scrape cycle: %s", exc)
            await asyncio.sleep(config.CHECK_INTERVAL_SECONDS)

    def _register_handlers(self):
        h = self.handlers
        # Conversation first: owns /add, the ➕ Add monitor bar button, home:add
        # and the ms:edit entry.
        self.application.add_handler(h.build_conversation())
        self.application.add_handler(CommandHandler("start", h.start))
        self.application.add_handler(CommandHandler("help", h.help))
        self.application.add_handler(CommandHandler("mysearches", h.mysearches))
        self.application.add_handler(CommandHandler("report", h.daily_report))
        self.application.add_handler(CommandHandler("mute", h.mute))
        self.application.add_handler(CommandHandler("unmute", h.unmute))
        # Backward-compat aliases for the old commands.
        self.application.add_handler(CommandHandler("subscribe", h.unmute))
        self.application.add_handler(CommandHandler("unsubscribe", h.mute))
        # Persistent tap-bar text buttons (Add monitor is a conversation entry).
        self.application.add_handler(MessageHandler(filters.Regex(f"^{BAR_LIST}$"), h.mysearches))
        self.application.add_handler(MessageHandler(filters.Regex(f"^{BAR_REPORT}$"), h.daily_report))
        self.application.add_handler(MessageHandler(filters.Regex(f"^{BAR_HELP}$"), h.help))
        # Standalone callback buttons (patterns disjoint from the conversation's).
        self.application.add_handler(CallbackQueryHandler(h.home_button, pattern=r"^home:(list|help)$"))
        self.application.add_handler(CallbackQueryHandler(h.manage_button, pattern=r"^ms:(toggle|del|delok):\d+$"))
        self.application.add_handler(CallbackQueryHandler(h.manage_back, pattern=r"^ms:back$"))

    async def _set_commands(self):
        await self.application.bot.set_my_commands([
            BotCommand("start", "Welcome & main menu"),
            BotCommand("add", "Add a new monitor (areas + filters)"),
            BotCommand("mysearches", "View / edit / delete your monitors"),
            BotCommand("report", "Get today's HTML report of all matches"),
            BotCommand("mute", "Pause all notifications"),
            BotCommand("unmute", "Resume notifications"),
            BotCommand("help", "How the bot works"),
        ])

    async def run(self):
        await self.db.connect()
        self.http = aiohttp.ClientSession()
        self.handlers = BotHandlers(self.db, self.http, on_saved=self.scan_now)
        self._register_handlers()

        cycle_task = None
        try:
            await self.application.initialize()
            await self.application.start()
            await self._set_commands()
            await self.application.updater.start_polling()
            logger.info("Bot polling started; scrape interval = %ss", config.CHECK_INTERVAL_SECONDS)

            cycle_task = asyncio.create_task(self._cycle_loop())
            # Run until cancelled (e.g. SIGTERM).
            await asyncio.Event().wait()
        finally:
            if cycle_task is not None:
                cycle_task.cancel()
            for step in (
                lambda: self.application.updater.stop() if self.application.updater.running else None,
                self.application.stop,
                self.application.shutdown,
            ):
                try:
                    result = step()
                    if result is not None:
                        await result
                except Exception as exc:
                    logger.debug("Shutdown step skipped: %s", exc)
            if self.http:
                await self.http.close()
            await self.db.close()
            logger.info("Bot shut down.")


async def main():
    if not config.TELEGRAM_BOT_TOKEN:
        logger.error("TELEGRAM_BOT_TOKEN not configured!")
        return
    bot = Bot(config.TELEGRAM_BOT_TOKEN)
    await bot.run()


if __name__ == "__main__":
    asyncio.run(main())
