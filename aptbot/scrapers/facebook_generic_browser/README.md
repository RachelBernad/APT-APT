# facebook_generic_browser

An async Python library that reads **Facebook groups** through a real, logged-in, human-paced Chrome:
list your groups, open one by id, read its newest posts (full text, photos, counts, comment previews), stream them,
poll for new ones, search groups, search inside a group, open a post with its comments, and filter everything
already read without touching Facebook. Many concurrent callers are served in parallel across several tabs of
the **same account**, while it still looks like one person.

Standalone: `aptbot` doesn't import it (yet), and it doesn't import `aptbot`.

> Automating a personal account is against Meta's Terms. The account can be checkpointed or banned. Use a
> secondary account, read-only, one tab acting at a time. The library stops itself on any warning screen.

## 5-minute quickstart (Mac)
```bash
uv venv ~/.fbgb/venv --python 3.13
uv pip install --python ~/.fbgb/venv/bin/python -r aptbot/scrapers/facebook_generic_browser/requirements.txt
fbgb() { ~/.fbgb/venv/bin/python -m aptbot.scrapers.facebook_generic_browser "$@"; }   # run from the repo root
fbgb login        # Chrome opens: log in BY HAND, then close the window
fbgb login --stdin   # or: email+password from stdin/prompt, typed for you (never stored)
fbgb groups       # your groups (members + pending requests)
fbgb posts 305724686290054 --limit 10
```
```python
import asyncio
from aptbot.scrapers.facebook_generic_browser import FacebookAPI

async with FacebookAPI(max_tabs=3) as fb:
    groups = await fb.list_groups()
    gid = next(g.id for g in groups if g.join_state == "MEMBER")
    posts, hits = await asyncio.gather(fb.latest_posts(gid, limit=20),       # two tabs at once
                                       fb.search_groups("דירה להשכרה"))
    async for p in fb.stream_posts(gid, limit=50): ...
    new = await fb.new_posts()                      # only posts since the last call; unchanged groups skipped
    cheap = await fb.filter_posts(max_price=6500, min_rooms=3)               # cache only
```

> **All times are UTC** (timezone-aware datetimes; the CLI prints a `UTC` suffix). Israel = UTC+2/+3. See
> [docs/data-model.md](docs/data-model.md).

## Functions
`status` · `resume` · `list_groups` · `get_group` · `latest_posts` · `stream_posts` · `new_posts` · `search_groups` ·
`search_group` · `get_post` · `get_comments` · `cross_group_feed` · `filter_posts`. Signatures, costs and errors:
[docs/api-reference.md](docs/api-reference.md).

Measured (2026-11-24, Mac): ~76 posts/min in one tab. Two feeds + a search over 3 tabs: 2.05× faster than
sequential. 8 live sessions, zero warnings.

## Documentation map
| You want to… | Read |
|---|---|
| call the functions | [docs/api-reference.md](docs/api-reference.md), [docs/usage-guide.md](docs/usage-guide.md), [examples/](examples/) |
| understand the returned objects | [docs/data-model.md](docs/data-model.md) |
| run many calls at once | [docs/concurrency.md](docs/concurrency.md) |
| handle errors / keep the account safe | [docs/errors-and-safety.md](docs/errors-and-safety.md) |
| install, log in, validate, run on the Pi | [docs/operations.md](docs/operations.md) |
| add a function / fix drift | [docs/extending.md](docs/extending.md) |
| explore this or **any other site** | [docs/exploration-playbook.md](docs/exploration-playbook.md) + `explore/tools/` |
| know Facebook's internals (URLs, graphql, DOM) | [api_docs/README.md](api_docs/README.md) |
| the architecture and decisions | [SPEC.md](SPEC.md) |

## Layout
```
api.py          FacebookAPI (public)          dispatcher.py  queue, tabs, single-flight, halt
jobs.py         one coroutine per operation   tabs.py        attention lock + pace gap   
parse.py        payload → models (pure)       extract.py     price/rooms/phone hints (pure)
models.py       dataclasses                   store.py       SQLite cache + watermarks
capture.py      per-tab response events       health.py      stop screens
browser.py      validated Chrome launch       human.py       human input timing
errors.py       exceptions                    __main__.py    CLI
explore/        validation, login, exploration sessions, anonymizer, tools/ (shape, timeline, islands, domcensus)
examples/       runnable scripts              docs/, api_docs/, SPEC.md
```
Tests: `tests/unit/test_fbgb_*.py` (offline: anonymized real payloads + fake-tab concurrency tests).
