# Photos & media

Last verified: 2026-11-28

- Per post: `attachments[].styles.attachment.all_subattachments.{count, nodes[]}`. Each node has `media.id` (photo id),
  `media.image.uri` (feed-size), `media.viewer_image.{uri,width,height}` (full-size, e.g. 1536×2048), and `url` (the photo page).
- CDN: `https://scontent.<pop>.fna.fbcdn.net/v/t39.30808-6/...` with signed query params (`oh=`, `oe=`). **`oe` is a hex
  unix expiry**, so URLs stop working after some days. Store the photo `id` + post id. Download images promptly if needed.
- `count` can exceed the nodes delivered: the feed **and the permalink page** both deliver at most 5 (verified
  2026-11-28 on 12 albums of 6–17 photos).
- **The full album:** `/media/set/?set=pcb.<post_id>&type=1` (the page the "+N" tile leads to). Its HTML document
  embeds every photo at `__bbox.result.data.album.media.edges[].node` (`__typename: "Photo"`, `id`, `image.{uri,width,height}`
  at full size ~900–1300 px wide, `is_playable`, `owner`, `creation_story`). 10/10 photos were in the document with no
  graphql call. Unverified past 17 photos (scrolling should page further). The URLs are fetchable without cookies.
