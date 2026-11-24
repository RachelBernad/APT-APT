# Photos & media

Last verified: 2026-11-24

- Per post: `attachments[].styles.attachment.all_subattachments.{count, nodes[]}`. Each node has `media.id` (photo id),
  `media.image.uri` (feed-size), `media.viewer_image.{uri,width,height}` (full-size, e.g. 1536×2048), and `url` (the photo page).
- CDN: `https://scontent.<pop>.fna.fbcdn.net/v/t39.30808-6/...` with signed query params (`oh=`, `oe=`). **`oe` is a hex
  unix expiry**, so URLs stop working after some days. Store the photo `id` + post id. Download images promptly if needed.
- `count` can exceed the nodes delivered in the feed (the feed shows about 4–5). **Unverified** whether the rest need the album page.
