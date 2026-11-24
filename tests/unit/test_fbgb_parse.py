"""facebook_generic_browser.parse + extract on anonymized real payloads (offline).

Fixtures: tests/unit/fixtures/fbgb/*.json, made with explore/anonymize.py (synthetic texts, fake people).
INVARIANTS (api_docs/graphql/queries/*.md):
- Story nodes are found anywhere and parsed by key; every post has id, text, time, group, permalink, author;
- the feed payload carries full text, photos, counts and inline comment previews;
- group metadata merges every object with the group's id; join states include pending requests.
"""
import json
from pathlib import Path

import pytest

from aptbot.scrapers.facebook_generic_browser import extract, parse

FIX = Path(__file__).parent / "fixtures" / "fbgb"
GID = "305724686290054"


def load(name):
    d = json.loads((FIX / name).read_text())
    return d["chunks"] if isinstance(d, dict) and "chunks" in d else d


def test_feed_page_posts_have_core_fields():
    posts = parse.posts(load("feed_page.json"))
    assert len(posts) == 3                                  # one pagination = 3 posts
    for p in posts:
        assert p.post_id.isdigit() and p.group_id == GID and p.group_name
        assert p.text.startswith("Synthetic post") and p.created_at is not None
        assert p.url == f"https://www.facebook.com/groups/{GID}/posts/{p.post_id}/"   # tracking stripped
        assert p.author_id and p.author_name.startswith("Person")
        assert p.photo_count >= len(p.photos) and all(ph.url and ph.width for ph in p.photos)
        assert p.comment_count is not None and p.reaction_count is not None


def test_inline_comment_previews():
    posts = {p.post_id: p for p in parse.posts(load("feed_page.json"))}
    with_comments = [p for p in posts.values() if p.comments]
    assert with_comments
    c = with_comments[0].comments[0]
    assert c.text.startswith("Synthetic comment") and c.author_name.startswith("Person") and c.created_at


def test_translation_is_separate_from_original():
    posts = parse.posts(load("feed_page.json")) + parse.posts(load("cross_group_feed.json"))
    assert all(p.text != p.text_translated for p in posts)


def test_cross_group_feed_same_schema():
    posts = parse.posts(load("cross_group_feed.json"))
    assert posts and all(p.group_id and p.group_name and p.text for p in posts)


def test_left_rail_groups_have_last_post_time():
    groups, page_info = parse.left_rail_groups(load("left_rail.json"))
    assert len(groups) == 10 and page_info.get("has_next_page") is True
    assert all(g.last_post_time and g.join_state == "MEMBER" and g.url.startswith("https://") for g in groups)


def test_joins_page_has_all_groups_with_states_and_names():
    blobs = load("joins_page.json")
    states = parse.join_states(blobs)
    assert "REQUESTED" in states.values() and "MEMBER" in states.values()
    assert all(parse.group_meta(blobs, gid).name for gid in states)
    rail, _ = parse.left_rail_groups(blobs)
    # the embedded rail (first 10) has last_post_time; the full "Your groups" list (11th+) doesn't
    assert sum(1 for g in rail if g.last_post_time) == 10 and len(rail) == 11


def test_left_rail_merge_is_order_independent():
    # live 2026-11-24: the joins page sometimes lists the full group list (no times) before the rail
    blobs = load("joins_page.json")
    for order in (blobs, list(reversed(blobs))):
        rail, _ = parse.left_rail_groups(order)
        assert sum(1 for g in rail if g.last_post_time) == 10


@pytest.mark.parametrize("fixture,gid,privacy,state", [
    ("group_page_public.json", GID, "public", "MEMBER"),
    ("group_page_pending.json", "2065942027026008", "private", "REQUESTED"),
])
def test_group_meta(fixture, gid, privacy, state):
    g = parse.group_meta(load(fixture), gid)
    assert (g.privacy, g.join_state) == (privacy, state)
    assert g.name and g.member_count.endswith("members") and g.url.endswith(f"/groups/{gid}/")


def test_search_group_hits():
    hits = parse.search_group_hits(load("search_groups.json"))
    assert len(hits) >= 5
    assert all(h.id and h.name and h.url.startswith("https://www.facebook.com/groups/") for h in hits)
    assert {h.privacy for h in hits} <= {"public", "private", None} and any(h.privacy for h in hits)


def test_clean_url_strips_tracking():
    u = "https://www.facebook.com/groups/1/posts/2/?__cft__[0]=AZabc&__tn__=%2CO%2CP-R"
    assert parse.clean_url(u) == "https://www.facebook.com/groups/1/posts/2/"


def test_to_dict_is_json_safe():
    p = parse.posts(load("feed_page.json"))[0]
    json.dumps(p.to_dict())


@pytest.mark.parametrize("text,prices,rooms", [
    ("להשכרה דירת 3 חדרים ברחוב הדוגמה, 5,000 ₪ לחודש", [5000], 3.0),
    ("4.5 חדרים, מחיר: 7200", [7200], 4.5),
    ("3-room apartment, 2500 NIS including bills", [2500], 3.0),
    ("₪6,300 per month", [6300], None),
    ("בניין משנת 1950, קומה 3", [], None),               # years / floors aren't prices
    ("השכירות 1000000 ₪", [], None),                     # implausible amounts dropped
])
def test_extract_hints(text, prices, rooms):
    h = extract.hints(text)
    assert h.prices == prices and h.rooms == rooms


def test_extract_phones_normalized_and_deduped():
    h = extract.hints("call 050-123-4567 or 0501234567, office 03-5551234")
    assert h.phones == ["0501234567", "035551234"]
