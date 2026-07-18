"""Unit tests for the group -1 activity observer.

The description helpers are pure, and the observer only touches attributes on the
update, so a SimpleNamespace stub stands in for a real telegram.Update — these
tests stay offline and never import PTB's network stack.
"""
from types import SimpleNamespace

import pytest

import activity


# --- callback data -> human label ------------------------------------------

@pytest.mark.parametrize("data,expected", [
    ("az:h:3:5000:2:7", "toggle neighborhood"),      # bundled-quarter hood toggle
    ("az:bh:4", "toggle neighborhood"),              # flat hood-list toggle
    ("az:tog:2", "toggle area"),
    ("az:q:3:5000:1", "open quarter"),
    ("az:done", "areas done → filters"),
    ("wz:save", "SAVE monitor"),
    ("wz:backlist", "back to list"),
    ("wz:field:price", "edit price"),
    ("rng:price:min:5000", "set price range"),
    ("rng:cardback", "back to filters"),
    ("pt:done", "property types done"),
    ("ft:mamad", "toggle feature"),
    ("cond:any", "condition: any"),
    ("home:list", "open my searches"),
])
def test_describe_callback(data, expected):
    assert activity.describe_callback(data) == expected


def test_describe_callback_keeps_monitor_id():
    assert activity.describe_callback("ms:delok:7") == "DELETE monitor #7"
    assert activity.describe_callback("ms:edit:12") == "edit monitor #12"


def test_describe_callback_city_id_is_not_int_coerced():
    # Yad2 city ids are zero-padded strings; the log must not eat the padding.
    assert "0070" in activity.describe_callback("az:city:2:0070")


def test_describe_callback_unknown_data_does_not_raise():
    assert isinstance(activity.describe_callback("zz:nope:1"), str)
    assert isinstance(activity.describe_callback(""), str)


# --- message -> human label -------------------------------------------------

def test_describe_message_bar_button():
    assert activity.describe_message("📋 My searches") == 'tap "📋 My searches"'


def test_describe_message_command_strips_bot_suffix():
    assert activity.describe_message("/start@AptAptBot") == "/start"
    assert activity.describe_message("/report") == "/report"


def test_describe_message_free_text_is_truncated():
    out = activity.describe_message("א" * 200)
    assert out.startswith('typed "')
    assert len(out) < 80


# --- who --------------------------------------------------------------------

def _update(chat_type="private", chat_id=1, title=None, first="Tomer",
            last=None, username=None, data=None, text=None):
    user = SimpleNamespace(id=99, first_name=first, last_name=last, username=username)
    chat = SimpleNamespace(id=chat_id, type=chat_type, title=title)
    query = SimpleNamespace(data=data) if data is not None else None
    message = SimpleNamespace(text=text, caption=None) if text is not None else None
    return SimpleNamespace(effective_user=user, effective_chat=chat,
                           callback_query=query, effective_message=message,
                           my_chat_member=None)


def test_who_private_chat():
    out = activity.who(_update(chat_id=376938396, first="Tomer", last="B",
                               username="tomerb1234567"))
    assert out == "376938396 Tomer B @tomerb1234567"


def test_who_omits_missing_username():
    assert activity.who(_update(chat_id=735551731, first="Shahar", last="Ohava")) == \
        "735551731 Shahar Ohava"


def test_who_group_names_the_chat_not_the_member():
    out = activity.who(_update(chat_type="group", chat_id=-4996574371, title="דירדיר"))
    assert "דירדיר" in out and "group" in out and "by Tomer" in out


# --- the observer -----------------------------------------------------------

@pytest.mark.asyncio
async def test_new_user_tagged_once_then_not(db, caplog):
    observe = activity.make_logger(db)
    with caplog.at_level("INFO", logger="activity"):
        await observe(_update(chat_id=555, text="/start"), None)
        await observe(_update(chat_id=555, data="wz:save"), None)
    lines = [r.getMessage() for r in caplog.records]
    assert sum("[NEW USER]" in line for line in lines) == 1
    assert len(lines) == 2
    assert "SAVE monitor" in lines[1]


@pytest.mark.asyncio
async def test_observer_records_the_user_name(db):
    observe = activity.make_logger(db)
    await observe(_update(chat_id=777, first="Netneal", text="/start"), None)
    cur = await db._conn.execute(
        "SELECT first_name, username FROM users WHERE chat_id=777")
    row = await cur.fetchone()
    assert row["first_name"] == "Netneal"


@pytest.mark.asyncio
async def test_group_chat_does_not_get_a_member_name(db):
    observe = activity.make_logger(db)
    await observe(_update(chat_type="group", chat_id=-42, title="דירדיר",
                          first="Tomer", username="tomerb1234567", text="/start"), None)
    cur = await db._conn.execute(
        "SELECT username, first_name FROM users WHERE chat_id=-42")
    row = await cur.fetchone()
    assert row["username"] is None          # a group has no @handle
    assert row["first_name"] == "דירדיר"    # the title, not whoever tapped


@pytest.mark.asyncio
async def test_observer_never_raises(db, caplog):
    """A logging bug must never break a user's interaction."""
    observe = activity.make_logger(db)
    broken = SimpleNamespace()              # no effective_chat at all
    with caplog.at_level("INFO", logger="activity"):
        await observe(broken, None)         # must not raise
    assert [r for r in caplog.records if r.levelname == "INFO"] == []
