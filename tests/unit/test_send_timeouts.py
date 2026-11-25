"""Telegram TimedOut = slow ack, not a failure: never resend (that duplicated FB posts).

Regression for 2026-11-25: an album timed out at the 5 s default but was delivered, and the
text fallback then sent the same post again without photos. Fake chat ids only.
"""
from __future__ import annotations

from types import SimpleNamespace

import pytest
from telegram.error import TimedOut

from aptbot import config
from aptbot.scrapers.facebook_generic_browser import Photo, Post

CHAT = 1001


class FakeTgBot:
    def __init__(self, fail_album=None, fail_text=None):
        self.calls = []
        self.fail_album, self.fail_text = fail_album, fail_text

    async def send_media_group(self, **kw):
        self.calls.append("album")
        if self.fail_album:
            raise self.fail_album

    async def send_photo(self, **kw):
        self.calls.append("photo")
        if self.fail_album:
            raise self.fail_album

    async def send_message(self, **kw):
        self.calls.append("text")
        if self.fail_text:
            raise self.fail_text


@pytest.fixture
def make_bot(monkeypatch):
    monkeypatch.setattr(config, "ALLOWED_CHAT_IDS", frozenset({CHAT}))
    from aptbot.__main__ import Bot

    def make(fake):
        b = Bot("123:fake")
        b.application = SimpleNamespace(bot=fake)
        return b
    return make


def post(text="דירה", photos=2):
    return Post(group_id="g", post_id="1", url="https://fb.test/1", created_at=None, text=text,
                photos=[Photo(id=str(i), url=f"https://cdn.test/{i}.jpg") for i in range(photos)])


async def test_album_timeout_is_not_resent_as_text(make_bot):
    fake = FakeTgBot(fail_album=TimedOut())
    assert await make_bot(fake).send_post(CHAT, post()) is True
    assert fake.calls == ["album"]


async def test_album_timeout_with_long_text_still_sends_the_text_once(make_bot):
    fake = FakeTgBot(fail_album=TimedOut())
    assert await make_bot(fake).send_post(CHAT, post("א" * 2000)) is True
    assert fake.calls == ["album", "text"]


async def test_text_timeout_counts_as_sent(make_bot):
    fake = FakeTgBot(fail_text=TimedOut())
    assert await make_bot(fake).send_post(CHAT, post(photos=0)) is True
    assert fake.calls == ["text"]


async def test_single_photo_timeout_not_resent(make_bot):
    fake = FakeTgBot(fail_album=TimedOut())
    assert await make_bot(fake).send_post(CHAT, post(photos=1)) is True
    assert fake.calls == ["photo"]


async def test_real_album_failure_still_falls_back_to_text(make_bot):
    from telegram.error import BadRequest
    fake = FakeTgBot(fail_album=BadRequest("wrong file identifier/http url specified"))
    assert await make_bot(fake).send_post(CHAT, post()) is True
    assert fake.calls == ["album", "text"]


def test_bot_timeouts_are_raised():
    from aptbot.__main__ import Bot
    req = Bot("123:fake").application.bot.request
    assert req.read_timeout >= 30
