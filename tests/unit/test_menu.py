"""The control menu: state-aware buttons, taps run the command, Filters opens the card.

Offline: SimpleNamespace stands in for telegram Update / CallbackQuery. Fake ids only.
"""
from __future__ import annotations

import asyncio
from types import SimpleNamespace

from aptbot.bot import filters as fl
from aptbot.bot import menu
from aptbot.bot.activity import describe_callback
from aptbot.bot.simple import SimpleCommands
from aptbot.models import SavedSearch

CHAT = -1002
ALICE = 111111111


def _data(markup):
    return [b.callback_data for row in markup.inline_keyboard for b in row]


def _labels(markup):
    return [b.text for row in markup.inline_keyboard for b in row]


class Chat:
    """One chat: collects replies, builds a typed command or a menu tap."""

    def __init__(self):
        self.replies = []        # (text, markup)
        self.retired = 0
        self.context = SimpleNamespace(chat_data={})

    async def _reply(self, text, reply_markup=None, **kw):
        self.replies.append((text, reply_markup))

    def _base(self):
        return dict(effective_chat=SimpleNamespace(id=CHAT, type="group"),
                    effective_user=SimpleNamespace(id=ALICE, username="alice_test"),
                    effective_message=SimpleNamespace(text="/x", reply_text=self._reply))

    def command(self):
        return SimpleNamespace(callback_query=None, **self._base())

    def tap(self, data):
        async def answer(*a, **kw):
            return None

        async def retire(reply_markup=None):
            self.retired += 1
        query = SimpleNamespace(data=data, answer=answer, edit_message_reply_markup=retire,
                                edit_message_text=self._reply,
                                message=SimpleNamespace(reply_text=self._reply))
        return SimpleNamespace(callback_query=query, **self._base())

    def last_menu(self):
        return _data(self.replies[-1][1])


def test_buttons_follow_the_state():
    assert _data(menu.menu_markup(None)) == ["menu:monitor", "menu:help"]
    on = SavedSearch(id=1, chat_id=CHAT, label="x", is_active=True)
    assert _data(menu.menu_markup(on)) == ["menu:filters", "menu:status", "menu:stop", "menu:help"]
    assert "🎛 Set filters" in _labels(menu.menu_markup(on))
    off = SavedSearch(id=1, chat_id=CHAT, label="x", is_active=False, max_price=7000, min_price=4000,
                      features=["mamad", "elevator"])
    assert _data(menu.menu_markup(off)) == ["menu:monitor", "menu:filters", "menu:status", "menu:help"]
    assert "▶️ Resume" in _labels(menu.menu_markup(off))
    assert "🎛 Filters · 3" in _labels(menu.menu_markup(off))      # price range counts once


async def test_tapping_start_pause_resume_updates_the_menu(db):
    started = []

    async def on_started(sid):
        started.append(sid)
    cmds = SimpleCommands(db, on_started=on_started)
    chat = Chat()
    await cmds.help(chat.command(), chat.context)
    assert chat.replies[-1][1] is menu.keyboard() or _keys(chat.replies[-1][1]) == list(menu.KEYS)
    await cmds.stop(chat.command(), chat.context)
    assert chat.last_menu() == ["menu:monitor", "menu:help"]          # nothing to pause yet

    await cmds.menu_tap(chat.tap("menu:monitor"), chat.context)
    assert "Watching" in chat.replies[-1][0] and "menu:stop" in chat.last_menu()
    await asyncio.sleep(0)
    assert len(started) == 1

    await cmds.menu_tap(chat.tap("menu:stop"), chat.context)
    assert "Paused" in chat.replies[-1][0] and chat.last_menu()[0] == "menu:monitor"
    assert not (await db.list_searches(CHAT))[0].is_active

    await cmds.menu_tap(chat.tap("menu:status"), chat.context)
    assert "Monitor:</b> off" in chat.replies[-1][0]
    assert chat.retired == 3                                          # each tapped menu went inert


async def test_filters_button_opens_the_card_and_save_brings_the_menu_back(db):
    async def noop(sid):
        return None
    cmds = SimpleCommands(db, on_started=noop)
    card = fl.FilterCommands(db, on_saved=noop)
    chat = Chat()

    assert await card.entry(chat.tap("menu:filters"), chat.context) == fl.ConversationHandler.END
    assert chat.last_menu() == ["menu:monitor", "menu:help"]          # no monitor: offer to start

    await cmds.monitor(chat.command(), chat.context)
    assert await card.entry(chat.tap("menu:filters"), chat.context) == fl.FORM
    assert "wz:save" in chat.last_menu()
    await card.button(chat.tap("rng:price:max:7000"), chat.context)
    await card.button(chat.tap("wz:save"), chat.context)
    text, markup = chat.replies[-1]
    assert "הסינון נשמר" in text and "🎛 Filters · 1" in _labels(markup)


def _keys(markup):
    return [b.text if hasattr(b, "text") else b for row in markup.keyboard for b in row]


def test_bottom_panel_layout():
    kb = menu.keyboard()
    assert _keys(kb) == ["🎛 Filters", "📊 Status", "▶️ Start", "⏸ Pause", "❓ Help"]
    assert kb.is_persistent and kb.resize_keyboard
    import re
    assert all(re.match(menu.KEY_REGEX, k) for k in menu.KEYS)
    assert not re.match(menu.KEY_REGEX, "7000") and not re.match(menu.KEY_REGEX, "📊 Status please")


async def test_panel_keys_run_the_commands(db):
    async def noop(sid):
        return None
    cmds = SimpleCommands(db, on_started=noop)
    card = fl.FilterCommands(db, on_saved=noop)
    chat = Chat()

    def key(label):
        u = chat.command()
        u.effective_message.text = label
        return u
    await cmds.key_tap(key("▶️ Start"), chat.context)
    assert "Watching" in chat.replies[-1][0]
    await cmds.key_tap(key("📊 Status"), chat.context)
    assert "Monitor:</b> on" in chat.replies[-1][0]
    assert await card.entry(key("🎛 Filters"), chat.context) == fl.FORM
    assert "wz:save" in chat.last_menu()
    await cmds.key_tap(key("⏸ Pause"), chat.context)
    assert "Paused" in chat.replies[-1][0]
    await cmds.key_tap(key("❓ Help"), chat.context)
    assert "button panel" in chat.replies[-1][0]


def test_filter_names_are_hebrew():
    s = SavedSearch(id=1, chat_id=CHAT, label="x", property_types=[1, 4], property_condition=2,
                    features=["mamad"])
    text = fl.filters_summary(s)
    assert "דירה" in text and "סטודיו/לופט" in text and "משופץ" in text and "ממ״ד" in text
    assert "מחיר" in text and "Price" not in text
    labels = [b.text for screen in (fl.card_screen(s), fl.types_screen(s), fl.features_screen(s))
              for row in screen[1].inline_keyboard for b in row]
    assert "💰 מחיר" in labels and "✅ שמירה" in labels and "✅ 🛡 ממ״ד" in labels


def test_menu_taps_have_log_labels():
    assert describe_callback("menu:stop") == "menu: pause"
    assert describe_callback("menu:filters") == "menu: open filters"
