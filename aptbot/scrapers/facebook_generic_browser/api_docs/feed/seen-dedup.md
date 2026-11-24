# Dedup & incremental runs

Last verified: 2026-11-24 (design based on observed fields)

- Primary key: `(group_id, post_id)`. `post_id` is stable and appears in both permalink forms.
- Edge `deduplication_key` and the Relay `id` also exist; don't key on them.
- Watermark per group = max `creation_time` seen. With CHRONOLOGICAL sort, stop paging once a batch is entirely
  ≤ watermark (allow a little slack for posts approved late by admins: keep going one extra batch).
- Edits: `message.text` can change. Store a hash and re-emit on change if needed (like Yad2 price changes).
