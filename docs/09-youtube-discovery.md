# 09. YouTube Discovery

This document covers how the crawler discovers YouTube URLs: which URL
types it extracts from YouTube pages, how YouTube channels get scheduling
priority, and how those URLs flow through router → ingestor → offerer. The
goal of the design is to **maximize the number of discovered YouTube URLs
per fetch** under the fixed per-domain QPS budget
(`domain_qps.json`: `www.youtube.com` delay 0.333 s, concurrency 4).

## 9.1 What and why

YouTube pages are rendered by JavaScript; the server HTML has almost no
`<a>` links to content. Everything useful is in two JSON blobs embedded in
the page: `ytInitialData` (page layout, recommendations, shelves) and
`ytInitialPlayerResponse` (the current video's `videoDetails`).

Before this change the crawler only walked those blobs for `videoId`s and
emitted `/watch?v=` links, so discovery followed the recommendation sidebar
of whatever video was crawled last (~20 videos per fetch). Channels and
playlists were never extracted.

Measured on live pages (MrBeast, Rick Astley, YouTube official channel),
URLs discovered by one fetch:

| Page | Videos | Notes |
| --- | --- | --- |
| `/watch?v=…` | ~21 | plus ~20 channel IDs from recommendation bylines |
| `/playlist?list=UU…` (channel uploads playlist) | **100** | newest uploads, shorts included |
| `/channel/UC…` (home tab) | 24–81 | +12–56 videos not in UU (older popular, featured shelves), featured channels, playlists |
| `/channel/UC…/playlists` | 12–30 | up to 30 playlists (`PL…`) |
| `/channel/UC…/videos` | 30 | only +4–10 videos beyond UU |
| `/channel/UC…/shorts` | 48 | +0 beyond UU |
| `/playlist?list=PL…` | ≤ 100 | per playlist |

So a channel fetch discovers ~5x more videos than a watch fetch, and the
uploads playlist + home + `/playlists` tabs cover almost everything a
channel exposes. `/videos` and `/shorts` are not emitted.

Constraints from `https://www.youtube.com/robots.txt` (`ROBOTSTXT_OBEY =
True`): `/results` (search), `/youtubei/` (internal API), `/feeds/videos.xml`
(channel RSS), `/api/` are disallowed; `/watch`, `/shorts/`, `/channel/`,
`/@handle`, `/playlist` are allowed. Only the first page of each listing is
available (continuations go through `/youtubei/`). YouTube's public
sitemaps (`/sitemaps/sitemap.xml`, `/product/sitemap.xml`) contain no
watch / channel URLs — only marketing pages and ~234K dead `/product/` pages
— so they are not a discovery source.

## 9.2 Pipeline architecture

```
                          crawler container
┌──────────────────────────────────────────────────────────────────────┐
│ spider.parse(response)                                               │
│  1. generic LinkExtractor <a> links                                  │
│       canonicalize_url → normalize_youtube_url → dedupe (seen_urls)  │
│       watch?v=ID&t=.. / /shorts/ID / youtu.be/ID → watch?v=ID        │
│  2. host is youtube.com → extract_youtube_outlinks(response)         │
│       walk ytInitialData + ytInitialPlayerResponse:                  │
│         videoId / LOCKUP_CONTENT_TYPE_VIDEO  → /watch?v=ID           │
│         browseId UC… / videoDetails.channelId → channel              │
│         videoOwnerRenderer.subscriberCountText → owner's subscribers │
│         channel header "524M subscribers" + metadata.externalId      │
│         playlistId / LOCKUP_CONTENT_TYPE_PLAYLIST (PL…, OLAK5uy_…)   │
│       per channel:  priority = channel_priority(subscribers)         │
│         always    → /playlist?list=UU…            (+priority)        │
│         if prio   → /channel/UC…, /channel/UC…/playlists (+priority) │
│       playlists on that channel's own page inherit its priority      │
└───────────────┬──────────────────────────────────────────────────────┘
                │ crawl_result JSONL: outlinks [{url, domain, anchor, priority?}]
                ▼
┌─ router ─────────────────────────────────────────────────────────────┐
│ _priority_fields(link): accept priority in (1.0, 2.0]                │
│   → url_priority = priority, source = SOURCE_YOUTUBE_CHANNEL (4)     │
│ build_new_link_record(..., url_priority, source)  (shard 64)         │
└───────────────┬──────────────────────────────────────────────────────┘
                ▼
┌─ ingestor ───────────────────────────────────────────────────────────┐
│ aggregate_links / _bulk_links: dedupe by url, keep max url_priority  │
│ url_priority rows skip the inline ranker:                            │
│   url_score = url_priority, url_score_updated_at = now(), source = 4 │
│ INSERT … ON CONFLICT (url): raise url_score/source only when the     │
│   existing row is source 0/4 and the new score is higher             │
└───────────────┬──────────────────────────────────────────────────────┘
                ▼
┌─ offerer ────────────────────────────────────────────────────────────┐
│ per domain: ORDER BY url_score DESC → channel tier (1, 2] first,     │
│ then model-scored videos / channels / playlists [0, 1]               │
└──────────────────────────────────────────────────────────────────────┘
   scorer (rescore TTL): skips url_score > 1, so channel priority sticks
```

Watch pages additionally feed `youtube_video_meta` through the feature
extractor (`extract_youtube.py`, unchanged).

Code:

- `containers/crawler/crawler/middlewares/youtube.py` — extraction, priority, URL normalization
- `containers/crawler/crawler/spiders/spider.py` — generic-link normalization, extractor hook
- `containers/scheduler_ingest/router/service.py` — `_priority_fields`
- `libs/ipc/new_link_record.py` — `url_priority` / `source` fields, `SOURCE_YOUTUBE_CHANNEL = 4`
- `containers/scheduler_ingest/ingestor/db_ops.py` — `_prefer_higher_priority`, `_bulk_links`

No schema change: `source` already exists on `url_state_current_*` and
`url_state_history_*` (`scripts/migrate_add_source.py`).

## 9.3 Priority model

```
priority = 1 + min(subscribers / 10_000_000, 1)    if subscribers >= 10_000
         = none (model-scored, ≤ 1)                 otherwise / unknown
```

- Range `(1, 2]`: the same manual tier as the Wikipedia pageview injection,
  below golden `3.0`, above every ranker score `[0, 1]`. Channels with
  ≥ 10M subscribers all cap at `2.0`.
- A channel's uploads playlist, home tab, `/playlists` tab and the
  playlists listed on its own channel pages all share its priority.
- Tunables (`middlewares/youtube.py`): `CHANNEL_PRIORITY_MIN_SUBSCRIBERS`,
  `CHANNEL_PRIORITY_SUBSCRIBER_SCALE`, `CHANNEL_PRIORITY_MAX_BONUS`.
- Subscriber text is parsed from English UI strings (`"4.55M subscribers"`,
  `"1,234 subscribers"`); the crawler sends `Accept-Language: en-US`.

### Why the priority tier drains instead of starving videos

The offerer serves the tier strictly first, so the tier must not grow
faster than it is consumed. Subscriber counts appear only in two places:
the owner block of a watch page and the header of the channel's own pages.
Therefore:

- a **watch fetch** (model tier) admits at most one new channel (its owner);
- a **priority fetch** (UU / home / `/playlists` / playlist) admits no new
  channel — other channels on those pages have no subscriber count.

New priority work is bounded by the number of watch fetches, so the tier
empties and video pages keep getting slots. Giving every discovered channel
priority (the first iteration did, with `1 + 1e-6` for unknown counts)
breaks this: one watch fetch admitted ~20 channels and every UU fetch ~5
more, so the tier never drained.

### Late upgrade

Channels without a known count are still emitted (UU playlist, no
priority) and enter as `source = 0`. When a later watch page shows that
channel's owner block, its UU URL is emitted again with a priority, and the
ingestor's `ON CONFLICT` raises the existing row to `source = 4` with the
new `url_score` (never lowering a score, never touching golden `1`,
pageview `2` or patrol `3` rows). Its home and `/playlists` URLs are
inserted at that point.

## 9.4 Deduplication

1. **Spider, per page**: generic links are deduped after normalization;
   JSON-derived links are merged with them by URL, keeping the highest
   priority.
2. **Ingestor, per folder / batch**: `aggregate_links` and `_bulk_links`
   merge identical URLs (inlink counts summed, best parent, max priority).
3. **DB**: `url_state_current_064.url` is the PK (all `youtube.com` URLs
   pin to shard 64 via `ingest.yaml`). An existing URL is never re-inserted
   and rediscovery does not reset `should_crawl`, so crawled pages are not
   re-queued by being seen again.

Dedup is exact-string after canonicalization. `/channel/UC…` and `/@handle`
for the same channel are different rows; this extractor always emits the
`/channel/UC…` form, so duplicates only arise from generic `<a>` links
using handles.

## 9.5 Worked example

Crawling `https://www.youtube.com/watch?v=_c2e1NcI3Sc` (MrBeast, owner block
`525M subscribers`) emits 57 discovery URLs:

| Priority | Count | URLs |
| --- | --- | --- |
| 2.0 (`source = 4`) | 3 | `/playlist?list=UUX6OQ3DkcsbYNE6H8uQQuVA`, `/channel/UCX6OQ3DkcsbYNE6H8uQQuVA`, `/channel/UCX6OQ3DkcsbYNE6H8uQQuVA/playlists` |
| model | 21 | `/watch?v=…` (recommendations + the page itself, which conflicts and is not re-added) |
| model | 19 | UU playlists of recommended channels (MrBeast Gaming, Mark Rober, Beta Squad, …) |
| model | 13 | footer / nav `<a>` links (`/about/`, `/t/terms`, …) |

Crawling the MrBeast home and `/playlists` tabs then yields the PL
playlists on them, each at `2.0`.

## 9.6 Observability

Read-only check of the YouTube shard (YB3):

```sql
SELECT source,
       count(*)                                          AS total,
       count(*) FILTER (WHERE should_crawl)              AS pending,
       count(*) FILTER (WHERE last_fetch_ok IS NOT NULL) AS fetched
FROM url_state_current_064
WHERE domain_id = (SELECT domain_id FROM domain_state WHERE domain = 'youtube.com')
GROUP BY source;
```

Healthy: `source = 4` pending oscillates (waves after new owners appear)
while `source = 0` fetched keeps growing. `source = 4` pending growing
monotonically with flat `source = 0` fetched means the tier is starving
videos — raise `CHANNEL_PRIORITY_MIN_SUBSCRIBERS`.

Crawler log event `youtube.video_urls_extracted` carries `count`,
`channel_count` and `playlist_count` per page.

## 9.7 Limitations and follow-ups

- **New uploads**: UU playlists are recrawled only by the generic recrawl
  path; there is no per-channel refresh cadence (RSS is robots-disallowed).
- **First page only**: listings beyond the first page need `/youtubei/`
  continuations, which robots.txt disallows.
- **Handles**: `/@handle` links are not mapped to `UC…` ids (needs a fetch).
- **Channel metadata**: subscriber counts are only encoded into
  `url_score`; persisting them (e.g. a `youtube_channel_meta` table) would
  enable trend analysis and re-prioritization.
- **Model blind spot**: the v2 ranker's YouTube rules recognise
  `/channel/UC…`, `/@handle` and `watch?v=` but not `/playlist?list=`, so
  non-priority playlists may score low until upgraded.
