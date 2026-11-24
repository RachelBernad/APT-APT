"""Chat allowlist: only chats in ``config.ALLOWED_CHAT_IDS`` may use the bot.

Registered as the very first handler group, so a refused update never reaches the
activity log, the DB or any command. Every refused chat is logged once (id + title)
so the right id can be found and put in ``.env``. If an allowed group is upgraded to
a supergroup, Telegram gives it a new ``-100…`` id; the gate follows that migration
for the lifetime of the process and logs the new id so ``.env`` can be updated.
"""
from __future__ import annotations

import logging

from telegram import Update
from telegram.ext import ApplicationHandlerStop, ContextTypes

from aptbot import config

logger = logging.getLogger(__name__)


class ChatGate:
    def __init__(self, allowed=None):
        self.allowed = set(config.ALLOWED_CHAT_IDS if allowed is None else allowed)
        self._refused_logged: set = set()
        if not self.allowed:
            logger.warning("ALLOWED_CHAT_IDS is empty: the bot will ignore every chat.")
        else:
            logger.info("Chat allowlist: %d chat(s).", len(self.allowed))

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
        if chat is not None and chat.id in self.allowed:
            return
        if chat is None:
            raise ApplicationHandlerStop
        if chat.id not in self._refused_logged:
            self._refused_logged.add(chat.id)
            logger.info("Ignored chat %s (%s %r): not in ALLOWED_CHAT_IDS.",
                        chat.id, chat.type, chat.title or chat.username or chat.first_name)
        raise ApplicationHandlerStop
