"""/filters card: open → pick ranges / features → Save (DB + re-prime) or Cancel.

Offline: SimpleNamespace stands in for telegram Update / CallbackQuery. Fake ids only.
"""
from __future__ import annotations

from types import SimpleNamespace

from telegram.ext import ConversationHandler

from aptbot.bot import filters as fl
from aptbot.bot.gate import ChatGate
from aptbot.models import LocationTarget, SavedSearch

CHAT = -1001
ALICE = 111111111


class Ui:
    """Collects what the bot shows; builds message and button updates."""

    def __init__(self):
        self.shown = []          # (text, markup)
        self.alerts = []
        self.context = SimpleNamespace(chat_data={})

    async def _show(self, text, reply_markup=None, **kw):
        self.shown.append((text, reply_markup))

    def message(self, text="/filters"):
        msg = SimpleNamespace(text=text, reply_text=self._show)
        return SimpleNamespace(effective_chat=SimpleNamespace(id=CHAT, type="group"),
                               effective_user=SimpleNamespace(id=ALICE), effective_message=msg,
                               callback_query=None)

    def tap(self, data):
        async def answer(text=None, show_alert=False):
            if text:
                self.alerts.append(text)
        query = SimpleNamespace(data=data, answer=answer, edit_message_text=self._show,
                                message=SimpleNamespace(reply_text=self._show))
        return SimpleNamespace(effective_chat=SimpleNamespace(id=CHAT, type="group"),
                               effective_user=SimpleNamespace(id=ALICE), callback_query=query)

    def buttons(self):
        markup = self.shown[-1][1]
        return [b.callback_data for row in markup.inline_keyboard for b in row] if markup else []


async def _setup(db):
    await db.upsert_user(CHAT, "g")
    tlv = LocationTarget(level="city", region_id=3, city_id="5000", display_name="תל אביב יפו")
    sid = await db.add_search(SavedSearch(id=0, chat_id=CHAT, label="תל אביב"), [tlv])
    await db.mark_primed(sid)
    await db.mark_fb_primed(sid)
    started = []

    async def on_saved(search_id):
        started.append(search_id)
    return sid, fl.FilterCommands(db, on_saved=on_saved), started


async def test_pick_price_and_mamad_then_save_updates_and_reprimes(db):
    sid, cmd, started = await _setup(db)
    ui = Ui()
    assert await cmd.entry(ui.message(), ui.context) == fl.FORM
    assert "wz:field:price" in ui.buttons() and "wz:save" in ui.buttons()

    await cmd.button(ui.tap("wz:field:price"), ui.context)
    await cmd.button(ui.tap("rng:price:min:none"), ui.context)
    assert "rng:price:max:7000" in ui.buttons()
    await cmd.button(ui.tap("rng:price:max:7000"), ui.context)
    await cmd.button(ui.tap("wz:feats"), ui.context)
    assert "ft:mamad" in ui.buttons() and "ft:price_only" not in ui.buttons()
    await cmd.button(ui.tap("ft:mamad"), ui.context)
    await cmd.button(ui.tap("wz:card"), ui.context)
    assert "≤ ₪7,000" in ui.shown[-1][0]

    assert await cmd.button(ui.tap("wz:save"), ui.context) == ConversationHandler.END
    s = await db.get_search(sid)
    assert (s.min_price, s.max_price, s.features) == (None, 7000, ["mamad"])
    assert not s.is_primed and not s.fb_primed           # fresh taster under the new filters
    import asyncio
    await asyncio.sleep(0)
    assert started == [sid]
    assert fl.DRAFT_KEY not in ui.context.chat_data


async def test_custom_value_by_reply_and_bad_input(db):
    sid, cmd, _ = await _setup(db)
    ui = Ui()
    await cmd.entry(ui.message(), ui.context)
    await cmd.button(ui.tap("wz:field:rooms"), ui.context)
    assert await cmd.button(ui.tap("rng:rooms:min:custom"), ui.context) == fl.AWAIT_VALUE
    assert await cmd.typed_value(ui.message("lots"), ui.context) == fl.AWAIT_VALUE
    assert await cmd.typed_value(ui.message("2.5"), ui.context) == fl.FORM
    assert "rng:rooms:max:3" in ui.buttons() and "rng:rooms:max:2" not in ui.buttons()  # max ≥ min
    await cmd.button(ui.tap("rng:rooms:max:none"), ui.context)
    await cmd.button(ui.tap("wz:save"), ui.context)
    s = await db.get_search(sid)
    assert (s.min_rooms, s.max_rooms) == (2.5, None)


async def test_cancel_changes_nothing(db):
    sid, cmd, started = await _setup(db)
    ui = Ui()
    await cmd.entry(ui.message(), ui.context)
    await cmd.button(ui.tap("wz:field:price"), ui.context)
    await cmd.button(ui.tap("rng:price:min:5000"), ui.context)
    assert await cmd.button(ui.tap("wz:cancel"), ui.context) == ConversationHandler.END
    s = await db.get_search(sid)
    assert s.min_price is None and s.is_primed and started == []


async def test_min_above_max_is_refused(db):
    sid, cmd, _ = await _setup(db)
    ui = Ui()
    await cmd.entry(ui.message(), ui.context)
    ui.context.chat_data[fl.DRAFT_KEY].min_price = 9000
    ui.context.chat_data[fl.DRAFT_KEY].max_price = 5000
    assert await cmd.button(ui.tap("wz:save"), ui.context) == fl.FORM
    assert ui.alerts and (await db.get_search(sid)).min_price is None


async def test_clear_all_and_existing_filters_load_into_the_card(db):
    sid, cmd, _ = await _setup(db)
    await db.update_search_filters(sid, max_price=8000, features=["elevator"], property_condition=2)
    ui = Ui()
    await cmd.entry(ui.message(), ui.context)
    assert "≤ ₪8,000" in ui.shown[-1][0] and "Elevator" in ui.shown[-1][0]
    await cmd.button(ui.tap("wz:clear"), ui.context)
    await cmd.button(ui.tap("wz:save"), ui.context)
    s = await db.get_search(sid)
    assert (s.max_price, s.features, s.property_condition) == (None, [], None)


async def test_no_monitor_yet(db):
    cmd = fl.FilterCommands(db, on_saved=None)
    ui = Ui()
    assert await cmd.entry(ui.message(), ui.context) == ConversationHandler.END
    assert "/monitor" in ui.shown[-1][0]


async def test_expired_card_button(db):
    _, cmd, _ = await _setup(db)
    ui = Ui()
    assert await cmd.button(ui.tap("ft:mamad"), ui.context) == ConversationHandler.END
    assert "/filters" in ui.shown[-1][0]


async def test_gate_refuses_a_strangers_button_tap():
    import pytest
    from telegram.ext import ApplicationHandlerStop
    gate = ChatGate(allowed={CHAT}, users={ALICE})
    ui = Ui()
    tap = ui.tap("wz:save")
    tap.effective_user = SimpleNamespace(id=999999999, is_bot=False)
    tap.effective_chat.title = "x"
    tap.effective_message = None
    with pytest.raises(ApplicationHandlerStop):
        await gate(tap, None)


def test_parse_number():
    assert fl.parse_number("7,500", False) == 7500
    assert fl.parse_number("₪ 6000", False) == 6000
    assert fl.parse_number("2.5", True) == 2.5
    assert fl.parse_number("2.5", False) is None
    assert fl.parse_number("-1", False) is None
    assert fl.parse_number("", True) is None
