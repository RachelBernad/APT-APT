# activity.py
"""One log line per user action.

Registered as a ``TypeHandler(Update, ...)`` in **group -1**, so it observes every
update *before* the real handlers run. PTB handles at most one handler per group
and only ``ApplicationHandlerStop`` skips later groups, so observing here never
consumes the update — the ConversationHandler & friends in group 0 still run.

The callback is awaited before the real handler, so it must stay cheap: names are
cached in-process and only written to the DB when they actually change.
"""
from __future__ import annotations

import logging
from typing import Awaitable, Callable, Dict

from telegram import Update
from telegram.ext import ContextTypes

from db import Database
from handlers import BAR_ADD, BAR_HELP, BAR_LIST, BAR_REPORT

logger = logging.getLogger("activity")

MAX_TEXT = 40

# The persistent tap-bar buttons send their label as plain text.
_BAR_LABELS = {BAR_ADD, BAR_LIST, BAR_REPORT, BAR_HELP}

# Opaque callback data -> what the user actually did. Longest prefix wins, so
# "wz:field" is matched before the bare "wz:" namespace.
_CALLBACK_LABELS = {
    "home:add": "open add monitor",
    "home:list": "open my searches",
    "home:help": "open help",
    # area wizard
    "az:city": "browse city neighborhoods",
    "az:whole": "watch whole city",
    "az:q": "open quarter",
    "az:h": "toggle neighborhood",
    "az:bh": "toggle neighborhood",
    "az:tog": "toggle area",
    "az:rm": "remove area",
    "az:home": "back to cities",
    "az:done": "areas done → filters",
    # filter wizard
    "wz:field": "edit filter",
    "wz:types": "open property type",
    "wz:cond": "open condition",
    "wz:feats": "open features",
    "wz:save": "SAVE monitor",
    "wz:cancel": "cancel wizard",
    "wz:backlist": "back to list",
    "rng:cardback": "back to filters",
    "rng": "set range",
    "pt:done": "property types done",
    "ft:done": "features done",
    "cond:any": "condition: any",
    "pt": "toggle property type",
    "cond": "set condition",
    "ft": "toggle feature",
    # my-searches management
    "ms:edit": "edit monitor",
    "ms:toggle": "pause/resume monitor",
    "ms:del": "delete monitor (asked)",
    "ms:delok": "DELETE monitor",
    "ms:back": "back to list",
}

# Callbacks whose 3rd segment is a monitor id worth naming in the log.
_WITH_SEARCH_ID = ("ms:edit", "ms:toggle", "ms:del", "ms:delok")


def describe_callback(data: str) -> str:
    """Human label for callback data, e.g. ``az:h:3:5000:2:7`` -> toggle neighborhood."""
    if not data:
        return "button"
    parts = data.split(":")
    # Try the most specific prefix first (namespace:action), then the namespace.
    for depth in (2, 1):
        key = ":".join(parts[:depth])
        label = _CALLBACK_LABELS.get(key)
        if label is None:
            continue
        if key in _WITH_SEARCH_ID and len(parts) > 2:
            return f"{label} #{parts[2]}"
        if key == "wz:field" and len(parts) > 2:
            return f"edit {parts[2]}"
        if key == "rng" and len(parts) > 2:
            return f"set {parts[1]} range"
        if key in ("az:city", "az:whole") and len(parts) > 3:
            return f"{label} (city {parts[3]})"
        return label
    return f"button {data}"


def describe_message(text: str) -> str:
    """Human label for a text message: a command, a tap-bar button, or free text."""
    text = (text or "").strip()
    if not text:
        return "sent a non-text message"
    if text in _BAR_LABELS:
        return f'tap "{text}"'
    if text.startswith("/"):
        return text.split()[0].split("@")[0]   # /start@MyBot -> /start
    if len(text) > MAX_TEXT:
        text = text[:MAX_TEXT] + "…"
    return f'typed "{text}"'


def describe(update: Update) -> str:
    if update.callback_query is not None:
        return describe_callback(update.callback_query.data or "")
    if update.my_chat_member is not None:
        # Arrives for free in Telegram's default allowed_updates: the user blocked
        # or unblocked the bot. Complements the Forbidden -> deactivate path.
        return f"chat status → {update.my_chat_member.new_chat_member.status}"
    message = update.effective_message
    if message is not None:
        return describe_message(message.text or message.caption or "")
    return "update"


def full_name(user) -> str:
    if user is None:
        return ""
    return " ".join(p for p in (user.first_name, user.last_name) if p)


def who(update: Update) -> str:
    """``<chat_id> <First Last> (@username)`` — omitting whatever Telegram withheld.
    In a group the chat is not a person, so the title is shown and the member who
    tapped is named separately."""
    chat = update.effective_chat
    user = update.effective_user
    chat_id = chat.id if chat else (user.id if user else "?")
    if chat is not None and chat.type != "private":
        actor = full_name(user) or (f"@{user.username}" if user and user.username else "?")
        return f'{chat_id} [{chat.type} "{chat.title or "—"}"] by {actor}'
    bits = [str(chat_id)]
    name = full_name(user)
    if name:
        bits.append(name)
    if user and user.username:
        bits.append(f"@{user.username}")
    return " ".join(bits)


def make_logger(db: Database) -> Callable[[Update, ContextTypes.DEFAULT_TYPE], Awaitable[None]]:
    """Build the group -1 observer. Records/refreshes the user's name and logs one
    line per action; a brand-new chat is tagged ``[NEW USER]``. Never raises."""
    # chat_id -> (username, first_name, last_name) already written; avoids a DB
    # write on every single button tap.
    known: Dict[int, tuple] = {}

    async def log_activity(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        try:
            user = update.effective_user
            chat = update.effective_chat
            if chat is None:
                return
            if chat.type == "private":
                identity = (
                    user.username if user else None,
                    user.first_name if user else None,
                    user.last_name if user else None,
                )
            else:
                # A group is not a person: whoever happened to tap must not have
                # their name written into the group's row. Record the title instead.
                identity = (None, chat.title, None)
            new_user = False
            if known.get(chat.id) != identity:
                new_user = await db.upsert_user(chat.id, *identity)
                known[chat.id] = identity
            tag = ""
            if new_user:
                tag = " [NEW GROUP]" if chat.type != "private" else " [NEW USER]"
            logger.info("%s → %s%s", who(update), describe(update), tag)
        except Exception as exc:      # logging must never break an interaction
            logger.debug("Activity logging failed: %s", exc)

    return log_activity
