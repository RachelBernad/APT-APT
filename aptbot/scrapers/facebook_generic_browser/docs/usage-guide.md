# Usage guide (recipes)

All snippets run inside `async with FacebookAPI() as fb:`. Runnable versions: `../examples/`.
Shell helper (zsh/bash, from the repo root): `fbgb() { ~/.fbgb/venv/bin/python -m aptbot.scrapers.facebook_generic_browser "$@"; }`

## 1. See the groups, pick one by id
```python
groups = await fb.list_groups()
readable = [g for g in groups if g.join_state == "MEMBER"]       # REQUESTED = pending, can't read private ones
g = await fb.get_group(readable[0].id)                            # privacy, members, description
```
CLI: `fbgb groups`, `fbgb group 305724686290054`

## 2. Latest posts of that group
```python
posts = await fb.latest_posts(g.id, limit=20)                     # newest first, full text, photos, counts
recent = await fb.latest_posts(g.id, since=datetime.now(timezone.utc) - timedelta(hours=3))
```
CLI: `fbgb posts 305724686290054 --limit 20`

## 3. React to posts as they arrive
```python
async for post in fb.stream_posts(g.id, limit=60):
    if "פלורנטין" in post.text:
        notify(post)
        break                                                     # stops scrolling, frees the tab
```

## 4. Several groups at once (parallel tabs)
```python
results = await asyncio.gather(*(fb.latest_posts(gid, limit=15) for gid in group_ids), return_exceptions=True)
```
Up to `max_tabs` run in parallel and the rest queue. Same-group calls are serialized, identical calls merged.

## 5. Polling for new posts (the bot's main loop)
```python
while True:
    try:
        for post in await fb.new_posts():                         # only posts since the previous call
            handle(post)
    except Halted as e:
        alert_human(e.reason); break                              # never retry automatically
    await asyncio.sleep(random.uniform(20, 35) * 60)              # SPEC §7: ≥ 20–30 min apart, jittered
```
Unchanged groups are skipped without being opened (their `last_post_time` didn't move).

## 6. Search
```python
hits = await fb.search_groups("דירה להשכרה", limit=20)            # groups to join (a human joins them)
found = await fb.search_group(g.id, "פלורנטין")                    # inside one group, relevance-ordered
```
CLI: `fbgb search-groups "דירה להשכרה"`, `fbgb search 305724686290054 "פלורנטין"`

## 7. Filter without touching Facebook
```python
cheap = await fb.filter_posts(max_price=6500, min_rooms=2.5, exclude=["תיווך"], has_photos=True,
                              since=datetime.now(timezone.utc) - timedelta(days=1))
```
CLI: `fbgb filter --max-price 6500 --min-rooms 2.5 --exclude תיווך --photos`

## 8. One post with its comments
```python
post = await fb.get_post(g.id, post_id)                           # embedded comments (≈ first 10)
```
The feed already gives a 1–2 comment preview per post (`post.comments`), so only open posts you need.

## 9. Handling errors
```python
try:
    posts = await fb.latest_posts(gid, timeout=90)
except NotMember as e:        # private group, pending request: skip it
    ...
except BudgetExhausted as e:  # account budget used up: retry after e.retry_after seconds
    ...
except RequestTimeout:        # slow page; the tab is already freed
    ...
except Halted as e:           # STOP. A human checks the account, then fb.resume()
    ...
```
Full table: [errors-and-safety.md](errors-and-safety.md).

## 10. Speed vs. safety knobs
- `max_age` on reads: serve repeat questions from the cache (default 60 s for posts, 10 min for groups).
- `Limiter(rate_per_min, burst, hourly_cap)`: the account budget. Start conservative and loosen only after
  weeks of clean runs.
- `max_tabs`: parallelism (3 Mac / 2 Pi).
