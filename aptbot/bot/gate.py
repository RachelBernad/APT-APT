"""Allowlist: only chats in ``config.ALLOWED_CHAT_IDS`` may use the bot, and inside them
only the people in ``config.ALLOWED_USER_IDS`` (when set) may give commands.

Registered as the very first handler group, so a refused update never reaches the
activity log, the DB or any command. Every refused chat is logged once (id + title)
so the right id can be found and put in ``.env``. If an allowed group is upgraded to
a supergroup, Telegram gives it a new ``-100…`` id; the gate follows that migration
for the lifetime of the process and logs the new id so ``.env`` can be updated.

Updates reach the bot only by long polling with its token, so ``chat.id`` and
``from_user.id`` are set by Telegram and cannot be forged by a sender. An anonymous
admin (``GroupAnonymousBot``) or a channel posting into the group carries a bot user
id, so it is refused like any other unknown user.
"""
from __future__ import annotations

import logging

from telegram import Update
from telegram.ext import ApplicationHandlerStop, ContextTypes

from aptbot import config

logger = logging.getLogger(__name__)


class ChatGate:
    def __init__(self, allowed=None, users=None, on_migrate=None):
        self.allowed = set(config.ALLOWED_CHAT_IDS if allowed is None else allowed)
        self.on_migrate = on_migrate     # async (old_id, new_id): move the DB rows over
        self._left: set = set()
        self.users = frozenset(config.ALLOWED_USER_IDS if users is None else users)
        self._refused_logged: set = set()
        if not self.allowed:
            logger.warning("ALLOWED_CHAT_IDS is empty: the bot will ignore every chat.")
        else:
            logger.info("Chat allowlist: %d chat(s).", len(self.allowed))
        if self.users:
            logger.info("User allowlist: %d person(s).", len(self.users))
        else:
            logger.warning("ALLOWED_USER_IDS is empty: ANY member of an allowed chat can give commands.")

    def is_allowed(self, chat_id) -> bool:
        return chat_id in self.allowed

    async def __call__(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        chat = update.effective_chat
        msg = update.effective_message
        if chat is not None and msg is not None and msg.migrate_to_chat_id and chat.id in self.allowed:
            new_id = msg.migrate_to_chat_id
            self.allowed.add(new_id)
            logger.warning("Allowed chat %s became supergroup %s; set ALLOWED_CHAT_IDS=%s in .env.",
                           chat.id, new_id, new_id)
            if self.on_migrate is not None:
                try:
                    await self.on_migrate(chat.id, new_id)
                except Exception as exc:                   # never let this block the update
                    logger.error("Moving chat %s to %s failed: %s", chat.id, new_id, exc)
        if chat is not None and chat.id in self.allowed:
            user = update.effective_user
            if not self.users or (user is not None and user.id in self.users):
                return
            key = ("user", user.id if user else None)
            if key not in self._refused_logged:
                self._refused_logged.add(key)
                logger.warning("Ignored user %s in allowed chat %s: not in ALLOWED_USER_IDS.",
                               user.id if user else None, chat.id)
            raise ApplicationHandlerStop
        if chat is None:
            raise ApplicationHandlerStop
        if chat.id not in self._refused_logged:
            self._refused_logged.add(chat.id)
            logger.info("Ignored chat %s (%s): not in ALLOWED_CHAT_IDS.", chat.id, chat.type)
        if chat.type in ("group", "supergroup") and chat.id not in self._left:
            # Someone added the bot to a group that isn't ours: leave, so nothing more arrives.
            self._left.add(chat.id)
            try:
                await context.bot.leave_chat(chat.id)
                logger.info("Left chat %s.", chat.id)
            except Exception as exc:
                logger.warning("Could not leave chat %s: %s", chat.id, exc)
        raise ApplicationHandlerStop
