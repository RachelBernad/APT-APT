"""Entry point: the apartment bot (Yad2 + Facebook groups, Tel Aviv).

Runs a python-telegram-bot polling app and, in the same event loop, two source
loops: the Yad2 scrape/match/route cycle and the slower Facebook groups poll.
Only chats in ALLOWED_CHAT_IDS can use it or receive anything. State: SQLite.
"""
import asyncio
import logging
import random
import signal

import aiohttp
from telegram import BotCommand, InputMediaPhoto, Update
from telegram.error import Forbidden, RetryAfter, TelegramError
from telegram.ext import Application, CommandHandler, TypeHandler

from aptbot.bot import activity
from aptbot import config
from aptbot import engine
from aptbot import fb_engine
from aptbot.bot import formatting
from aptbot.bot.gate import ChatGate
from aptbot.bot.simple import SimpleCommands, now_iso
from aptbot.db import Database

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
        self.gate = ChatGate()
        self.fb = None                       # FacebookAPI when FB_ENABLED
        self.fb_state = fb_engine.FbState()
        self.commands: SimpleCommands | None = None

    async def send(self, chat_id: int, text: str, photo: str | None = None) -> bool:
        """Deliver a message. When ``photo`` is given, send it as a photo with the
        message as caption (image preview); on any photo failure — bad/expired image
        URL, caption too long, CDN blocking Telegram — fall back to an HTML text
        message so the listing is never lost. Handles flood-control + blocked users."""
        if not self.gate.is_allowed(chat_id):
            logger.warning("Refusing to send to chat %s: not in ALLOWED_CHAT_IDS.", chat_id)
            return False
        if photo:
            caption = formatting.photo_caption(text)
            try:
                await self.application.bot.send_photo(
                    chat_id=chat_id, photo=photo, caption=caption, parse_mode="HTML")
                return True
            except RetryAfter as exc:
                wait = int(getattr(exc, "retry_after", 5)) + random.randint(1, 5)
                logger.info("Flood control (photo): waiting %ss for chat %s", wait, chat_id)
                await asyncio.sleep(wait)
                try:
                    await self.application.bot.send_photo(
                        chat_id=chat_id, photo=photo, caption=caption, parse_mode="HTML")
                    return True
                except TelegramError:
                    pass  # fall through to text
            except Forbidden:
                logger.info("Chat %s blocked/removed the bot; deactivating.", chat_id)
                await self.db.set_user_active(chat_id, False)
                return False
            except TelegramError as exc:
                # Self-healing (we fall back to text) and fires per dead image URL,
                # so it's cycle noise rather than a problem worth surfacing.
                logger.debug("Photo send failed for chat %s (%s); sending text instead.",
                             chat_id, exc)
        return await self._send_text(chat_id, text)

    async def _send_text(self, chat_id: int, text: str) -> bool:
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

    async def send_post(self, chat_id: int, post) -> bool:
        """Deliver one Facebook post: an album of its photos (caption = the text when it
        fits), else/then the text. If the album fails (expired/blocked photo URL) the
        text still goes out, so the post is never lost."""
        if not self.gate.is_allowed(chat_id):
            logger.warning("Refusing to send to chat %s: not in ALLOWED_CHAT_IDS.", chat_id)
            return False
        urls = [p.url for p in post.photos if p.url][: max(1, config.FB_MAX_PHOTOS)]
        full = formatting.format_fb_post(post)
        if urls:
            fits = formatting.fb_post_fits_caption(post)
            caption = full if fits else formatting.format_fb_short_caption(post)
            media = [InputMediaPhoto(u, caption=caption if i == 0 else None,
                                     parse_mode="HTML" if i == 0 else None)
                     for i, u in enumerate(urls)]
            for attempt in (1, 2):
                try:
                    if len(media) == 1:
                        await self.application.bot.send_photo(
                            chat_id=chat_id, photo=urls[0], caption=caption, parse_mode="HTML")
                    else:
                        await self.application.bot.send_media_group(chat_id=chat_id, media=media)
                    return True if fits else await self._send_text(chat_id, full)
                except RetryAfter as exc:
                    if attempt == 2:
                        break
                    await asyncio.sleep(int(getattr(exc, "retry_after", 5)) + random.randint(1, 5))
                except Forbidden:
                    await self.db.set_user_active(chat_id, False)
                    return False
                except TelegramError as exc:
                    logger.info("FB album failed for chat %s (%s); sending text only.", chat_id, exc)
                    break
        return await self._send_text(chat_id, full)

    async def scan_now(self, search_id: int):
        """Immediately scan one just-started monitor (Yad2 taster within seconds) and
        then wake the Facebook loop so its taster follows (after Yad2's, not interleaved)."""
        try:
            await engine.scan_search(self.send, self.db, self.http, search_id)
        except Exception as exc:
            logger.error("Immediate scan for search #%s failed: %s", search_id, exc)
        finally:
            self.fb_state.wake.set()

    async def _cycle_loop(self):
        # Small initial delay so polling is fully up before the first scrape.
        await asyncio.sleep(5)
        while True:
            try:
                await engine.run_cycle(self.send, self.db, self.http)
                await self.db.set_meta("yad2_last_cycle", now_iso())
            except Exception as exc:
                logger.error("Unexpected error in scrape cycle: %s", exc)
            await asyncio.sleep(config.CHECK_INTERVAL_SECONDS)

    def _register_handlers(self):
        c = self.commands
        # Group -2: the allowlist. A refused update stops here (ApplicationHandlerStop).
        self.application.add_handler(TypeHandler(Update, self.gate), group=-2)
        # Group -1: passive observer that logs every user action (and new users)
        # before the real handlers run. PTB runs one handler per group, so this
        # never consumes the update.
        self.application.add_handler(
            TypeHandler(Update, activity.make_logger(self.db)), group=-1)
        for name, fn in (("start", c.start), ("help", c.help), ("monitor", c.monitor),
                         ("stop", c.stop), ("status", c.status)):
            self.application.add_handler(CommandHandler(name, fn))

    async def _set_commands(self):
        await self.application.bot.set_my_commands([
            BotCommand("monitor", "Start watching Tel Aviv (Yad2 + Facebook)"),
            BotCommand("stop", "Pause"),
            BotCommand("status", "What each source did last"),
            BotCommand("help", "How the bot works"),
        ])

    async def _open_facebook(self):
        if not config.FB_ENABLED:
            logger.info("Facebook source disabled (FB_ENABLED=0).")
            return None
        from aptbot.scrapers.facebook_generic_browser import FacebookAPI
        fb = FacebookAPI(max_tabs=config.FB_MAX_TABS)
        await fb.__aenter__()
        st = await fb.status()
        if st.halt_reason:
            self.fb_state.stopped_reason = st.halt_reason
            logger.error("Facebook is halted (%s); not polling until a human checks the account.",
                         st.halt_reason)
        logger.info("Facebook source on: %d group(s), poll every %d-%d min.",
                    len(config.FB_GROUP_IDS), config.FB_POLL_MIN_SECONDS // 60,
                    config.FB_POLL_MAX_SECONDS // 60)
        return fb

    async def run(self):
        await self.db.connect()
        self.http = aiohttp.ClientSession()
        self.fb = await self._open_facebook()
        self.commands = SimpleCommands(self.db, on_started=self.scan_now,
                                       fb_state=self.fb_state, fb_api=self.fb)
        await self.commands.sync_areas()
        self._register_handlers()

        cycle_task = fb_task = None
        try:
            await self.application.initialize()
            await self.application.start()
            await self._set_commands()
            await self.application.updater.start_polling()
            logger.info("Bot polling started; scrape interval = %ss", config.CHECK_INTERVAL_SECONDS)

            cycle_task = asyncio.create_task(self._cycle_loop())
            if self.fb is not None and not self.fb_state.stopped_reason:
                fb_task = asyncio.create_task(fb_engine.fb_loop(
                    self.fb, self.db, self.send_post, self.send, self.fb_state))
            # Run until cancelled (e.g. SIGTERM).
            await asyncio.Event().wait()
        finally:
            for task in (cycle_task, fb_task):
                if task is not None:
                    task.cancel()
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
            if self.fb is not None:
                try:
                    await self.fb.close()          # closes Chrome cleanly (flushes cookies)
                except Exception as exc:
                    logger.debug("Facebook close skipped: %s", exc)
            if self.http:
                await self.http.close()
            await self.db.close()
            logger.info("Bot shut down.")


async def main():
    if not config.TELEGRAM_BOT_TOKEN:
        logger.error("TELEGRAM_BOT_TOKEN not configured!")
        return
    bot = Bot(config.TELEGRAM_BOT_TOKEN)
    # SIGTERM (docker stop) → cancel run(), whose finally closes Chrome cleanly (cookies
    # flushed) and the DB. As PID 1 in a container, Python would otherwise ignore it.
    loop, task = asyncio.get_running_loop(), asyncio.current_task()
    for sig in (signal.SIGTERM, signal.SIGINT):
        try:
            loop.add_signal_handler(sig, task.cancel)
        except (NotImplementedError, RuntimeError):
            pass
    try:
        await bot.run()
    except asyncio.CancelledError:
        logger.info("Stop signal received.")


if __name__ == "__main__":
    asyncio.run(main())
