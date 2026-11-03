# 10. Frontier Trap Rules

URL-only heuristics that flag crawler traps (URL spaces that grow without
bound while serving little new content) and duplicate URLs (the same page
under a session id or tracking parameter). Code: `libs/frontier/trap_rules.py`
(detection) and `libs/frontier/url_normalize.py` (parameter stripping).

Rules look at the URL string only: no fetch, no DB lookup. That makes them
cheap enough to run on every discovered link, at the cost of not seeing
content (duplicate pages with clean-looking URLs need content-hash or
template analysis instead).

## How the rules are used

| Group | Rules | Action at ingest (`trap_filter`, see [02.4](02-container-runtime-design.md)) |
|---|---|---|
| High confidence | `A_path_repeat`, `A_too_many_segments`, `B_nested_redirect`, `B_faceted`, `B_calendar` | Link dropped |
| Duplicate class | `A_session_id`, `B_tracking_params` | Only `srsltid` stripped (`normalize_params`); every other tracking / session param only logged as `would_normalize`. Link kept |
| Medium / low | `B_big_page`, `B_big_offset`, `B_future_date`, `B_many_params`, `B_long_url`, `B_random_token` | Not acted on; signal only |

The groups are `HIGH_CONFIDENCE` and `DUPLICATE_CLASS` in `trap_rules.py`.
High-confidence rules are checked on the normalized URL, so
`/x/x/x/?srsltid=a` is stripped first and then dropped.

## Rule reference

Tier A follows rules that mainstream crawlers ship (Nutch `regex-urlfilter`,
StormCrawler `BasicURLFilter`, Heritrix `PathologicalPathDecideRule` /
`TooManyPathSegmentsDecideRule`, Archive-It session-id guidance). Tier B are
parameter heuristics tuned on the production frontier.

| Rule | Fires when | Example | Notes |
|---|---|---|---|
| `A_path_repeat` | The same path segment appears 3+ times (`PATH_REPEAT_MAX`) | `mipt.ru/event/1040/abitu.net/profile/abitu.net/profile/…` | Relative-link recursion. Pure-digit segments of up to 4 chars are ignored so `/2022/02/02/` dates do not count |
| `A_too_many_segments` | More than 20 path segments (`MAX_PATH_SEGMENTS`) | `ecareer.ne.jp/…/jobType/1020000/jobType/1020000/…` | Usually the same recursion as above |
| `A_session_id` | `phpsessid`, `jsessionid`, `cfid`, `cftoken`, `zenid`, `oscsid`, `aspsessionid*`, or `;jsessionid=` in the path; `sid` / `s` / `sessionid` / … only when the value looks like a random token | `…/index.php?sid=0f3a…(32 hex)` | `sid=154` is a category id, so generic names need a token-like value |
| `B_tracking_params` | Any `utm_*`, `gclid`, `fbclid`, `msclkid`, `srsltid`, `_ga`, `_gl`, `mc_cid`, … (`TRACKING_PARAMS`) | `…/p?id=5&utm_source=x` | Google appends `srsltid` to search result links, so golden URLs often carry it |
| `B_nested_redirect` | A parameter value is a full URL that itself has a query, or the query holds 2+ embedded URLs | `login?returnUrl=https%3A%2F%2F…%3Fa%3D1`, `share?u=…` | Login / share / redirect endpoints that mirror every page |
| `B_faceted` | 3+ facet parameters (`sort`, `order`, `filter`, `view`, `limit`, `price`, `color`, `brand`, `f[...]`, `facet*`, …) | DSpace browse, e-commerce sort × price × colour | Combinatorial filter space |
| `B_calendar` | Calendar-plugin paths (`/events/…/day/`, `/calendar/…/YYYY-MM`) or params (`tribe-bar-date`, `eventDisplay`, `ical`) | WordPress The Events Calendar | Infinite next-month links. Generic 4-digit numbers are deliberately not treated as years |
| `B_big_page` | Page number > 1000 in `page`, `pg`, `paged`, … or `/page/N/` | `?page=48213` | `p` excluded: `?p=N` is a WordPress / phpBB post id. Deep news pagination is often real content |
| `B_big_offset` | `offset` / `start` / `skip` > 10000 | `?start=250000` | Small volume |
| `B_future_date` | A date in the path or a date param later than 2028 (`YEAR_MAX`) | `/2099/01/` | Past dates are legitimate archives |
| `B_many_params` | 5+ non-tracking query params | `justice.gouv.fr` search pages | Too broad on its own |
| `B_long_url` | Percent-decoded URL longer than 300 chars | — | Measured decoded so non-ASCII paths are not penalized |
| `B_random_token` | A non-tracking param value is 32+ hex chars or a 40+ char base64-like token | — | Too broad on its own |

## Validation (2026-10-15)

Measured on a 0.05% `TABLESAMPLE SYSTEM` sample of the pending frontier
(2.3M rows, scaled to 4.62e9) and on the 138,638 distinct golden URLs in
metricdb `metric_url`. Golden is biased towards popular pages, so the golden
hit rate is a lower bound on false positives for long-tail content.

| Rule | Est. pending | Share | Golden hits | Verdict |
|---|---:|---:|---:|---|
| `A_path_repeat` | 1.11e8 | 2.39% | 1 | high confidence |
| `A_too_many_segments` | 0.50e8 | 1.09% | 1 | high confidence |
| `B_nested_redirect` | 0.57e8 | 1.25% | 0 | high confidence |
| `B_faceted` | 0.18e8 | 0.40% | 2 | high confidence |
| `B_calendar` | 0.08e8 | 0.18% | 0 | high confidence |
| `A_session_id` | 4.82e8 | 10.4% | 16 | duplicate |
| `B_tracking_params` | 0.45e8 | 0.98% | 1,327 | duplicate (almost all `srsltid`) |
| `B_big_page` | 0.014e8 | 0.03% | 9 | medium |
| `B_big_offset` | 0.004e8 | 0.01% | 0 | medium |
| `B_future_date` | 0.023e8 | 0.05% | 1 | medium |
| `B_many_params` | 4.07e8 | 8.8% | 92 | low |
| `B_long_url` | 1.77e8 | 3.8% | 16 | low |
| `B_random_token` | 3.69e8 | 8.0% | 386 | low |

Groups (a URL hitting several rules counts once): high confidence 1.95e8
pending (4.2%), 4 golden hits (0.003%); duplicate class 5.26e8 (11.4%).

### v1 → v2 fixes

The first version was checked against golden hits and sampled examples; these
false positives were removed:

| Rule | v1 problem | v2 fix |
|---|---|---|
| `B_big_page` | `?p=N` (WordPress / phpBB post id): 65 golden hits | `p` and `/p/N` removed |
| `A_session_id` | `sid=154`, `sid=mcr,96v` are category ids | generic names only with a token-like value |
| `B_calendar` | any 4-digit number read as a year; news titles containing "calendario" | calendar-plugin signatures only |
| `B_calendar_out_of_range` | 1932, 1944 are real archives | replaced by `B_future_date` (> 2028 only) |
| `A_path_repeat` | `/2022/02/02/…` date paths | short numeric segments ignored |
| `B_long_url` | percent-encoded Arabic / Hindi URLs: 0.43% of golden | length measured after decoding (0.012%) |
| tracking params | mixed into `random_token` | split out as `B_tracking_params`, classed as duplicate |

## Evidence for the high-confidence rules (2026-11-03)

These rules are heuristics: the claim is not zero false positives but that
what they reject is overwhelmingly repeat or low-value pages, at a measured
cost. Computed from the phase 1 samples with `trap_rule_evidence.py` (kept
with the eviction analysis, outside this repo, along with the per-domain and
example CSVs); re-run it after any rule change.

**Data.** Phase 1 `TABLESAMPLE SYSTEM (0.05%) REPEATABLE (42)` per shard on the
pre-YB3 PostgreSQL crawlerdb: 2,308,263 pending rows and 1,000,122
ever-scheduled rows (with fetch outcome, title, `content_hash`), scaled ×2000.
Golden: 138,459 distinct metricdb `metric_url` URLs. Pages are sampled by heap
page, so one domain's rows cluster: totals are reliable, single-domain counts
are rough (exact counts come from the full scan in `top_domains.csv`).

### Volume and fetch outcome

"Repeat title" = share of successfully fetched pages whose (domain, title)
occurs at least twice among that rule's sampled pages, i.e. the crawler got a
page it already had under another URL. The control row is pages from the same
domains that hit no rule.

| Rule | Est. pending | Share | Domains | Top-10 domain share | Fetched in sample | Fetch OK | Repeat title (OK pages) | Golden hits |
|---|---:|---:|---:|---:|---:|---:|---:|---:|
| `A_path_repeat` | 1.11e8 | 2.39% | 248 | 57% | 11,638 | 80% | **96%** | 1 |
| `A_too_many_segments` | 0.50e8 | 1.09% | 142 | 57% | 7,387 | 79% | **95%** | 1 |
| `B_nested_redirect` | 0.57e8 | 1.24% | 832 | 29% | 5,093 | 55% | **95%** | 0 |
| `B_faceted` | 0.18e8 | 0.40% | 126 | 55% | 2,763 | 87% | **90%** | 2 |
| `B_calendar` | 0.08e8 | 0.18% | 418 | 26% | 15,649 | 83% | 43% | 0 |
| any of the above | **1.95e8** | **4.22%** | 1,581 | 33% | 35,306 | 78% | 70% | **4** |
| control: same domains, no rule hit | — | — | — | — | 58,799 | 73% | 48% | — |

Reading it:
- Path recursion, nested redirects and faceted listings return a page the
  crawler already has 90–96% of the time, against 48% for other URLs of the
  same sites. `B_nested_redirect` also fails to fetch far more often (55% OK).
- `B_calendar` does not look like duplication by this measure (43%, below the
  control): each day / month view is a distinct listing. It is kept because the
  space is unbounded (every date, forwards and backwards), the pages are event
  listings rather than content, volume is small, and no golden URL hits it. It
  is the weakest of the five and the first to demote if the audit log shows
  golden hits.

### Where the volume comes from

Top domains by sampled hits (`exact_pending` / `fetch_yield` from the full scan):

| Rule | Domain | Share of rule | Exact pending | Fetch yield | Pattern |
|---|---|---:|---:|---:|---|
| `A_path_repeat` | goeshard.org | 13% | 19.6M | 1.5% | relative-link recursion |
| | anthropic.com | 9% | 10.0M | 2.0% | |
| | jma.go.jp | 9% | 9.7M | 3.3% | |
| | metropolia.fi | 9% | 11.9M | 2.8% | |
| `A_too_many_segments` | anthropic.com | 20% | 10.0M | 2.0% | same recursion, deeper |
| | eia.gov | 5% | 3.2M | 9.0% | |
| | ticket360.com.br | 5% | 2.3M | 3.9% | |
| `B_nested_redirect` | google.com | 5% | 33.3M | 1.8% | `plus.google.com/share?url=`, `calendar.google.com/calendar/event?action=TEMPLATE&details=…` |
| | bsky.app | 5% | 10.6M | 2.2% | `intent/compose?text=…https%3A…` share links |
| | wordpress.com | 4% | 10.5M | 3.9% | `log-in?redirect_to=…remote-login.php%3F…` |
| `B_faceted` | wisc.edu, ufgd.edu.br, bndes.gov.br, gwu.edu, uefs.br | 5–8% each | 1.1–3.5M | 5–17% | DSpace `browse?sort_by=&order=&rpp=&starts_with=` |
| `B_calendar` | vpa.uncg.edu, ccce.calpoly.edu, portal.ema.alabama.gov | ~3% each | — | — | The Events Calendar day / list views |

Several of these domains also have golden URLs (anthropic.com, jma.go.jp,
eia.gov). Only the URLs matching a rule are rejected; the rest of the domain
is crawled as before, which is why these rules work per URL rather than
freezing domains.

### Thresholds

| Threshold | Value | Why |
|---|---|---|
| `PATH_REPEAT_MAX` | 3 | Nutch `regex-urlfilter` default (`(/[^/]+)/[^/]+\1/[^/]+\1/`). 2 hits legitimate paths like `/news/region/news/`. Numeric segments of up to 4 chars are skipped so `/2022/02/02/` dates do not count |
| `MAX_PATH_SEGMENTS` | 20 | Heritrix `TooManyPathSegmentsDecideRule` default |
| `B_nested_redirect` | embedded URL that itself has a query, or 2+ embedded URLs | A plain `?url=https://site/page` is often a legitimate share link; one carrying its own query (`?returnUrl=…%3Fpage%3D40`) is a login / redirect wrapper that mirrors every page |
| `MIN_FACET_PARAMS` | 3 | 1–2 (`?sort=&page=`) is ordinary navigation; 3+ facet params is the combinatorial browse space |
| `B_calendar` | plugin signatures only | v1 matched any 4-digit "year" and news titles containing "calendario" (see v1 → v2 fixes) |

### Resource savings

- **Frontier storage**: 1.95e8 pending rows, 4.2% of the frontier. At ~820
  bytes per `url_state_current` row (table + indexes + TOAST ÷ rows on the
  pre-YB3 PostgreSQL, 2026-10-19) that is about **160 GB**. YugabyteDB's
  on-disk footprint per row differs; the row count is the stable number.
- **Fetch budget**: 3.5% of all sampled fetches (35,306 / 1,000,122) went to
  URLs these rules match, and 70% of those that succeeded returned a page the
  crawler already had (95% of 14,575 OK pages when `B_calendar` is left out).
- **Inflow**: the rules apply at ingest, so the rejected share of new links is
  measured per day by `ingest.folder_done.trap_dropped`.

### Useful pages rejected

**Golden URLs rejected (4 of 138,459, 0.0029%).** Kept explicit so the
tradeoff stays visible:

| Golden URL | Rule | Assessment |
|---|---|---|
| `nottinghamshire.police.uk/news/nottinghamshire/news/news/2026/may/incident-next-to-a617-rainworth-bypass/` | `A_path_repeat` | **Real article**: the site's own path repeats `news` three times. Accepted loss; golden injection still adds it |
| `codabench.org/competitions/12555/3aI2jK6zU4u8GcU52gVu3Qwebinterface---%3E/2.39.1%3C%2Fa%3E%29/…` | `A_too_many_segments` | Broken URL built from escaped HTML fragments; golden only because a search engine indexed it |
| `olivenoele.com/de/tests/auswahl/oekotest?dir=desc&limit=all&order=position` | `B_faceted` | **Real listing page**, sorted / unpaged variant of `/de/tests/auswahl/oekotest` |
| `sigmaaldrich.com/US/en/search/b27-supplement?focus=products&page=1&perpage=30&sort=relevance&term=…&type=product` | `B_faceted` | Site search results page |

**Non-golden pages with a unique title** (the candidates for useful content
the rules reject). Patterns
seen in samples:

| Rule | OK pages with unique title | What they are |
|---|---:|---|
| `A_path_repeat` | 348 of 9,253 (3.8%) | Real articles / pages served at a recursive URL (`wbtv.com/news/affordable-housing/www.wbtv.com/www.wbtv.com/…`). The content is real, but the same page exists at its canonical URL |
| `A_too_many_segments` | 288 of 5,863 (4.9%) | Same as above, deeper (`hindi.newslaundry.com/2026/02/12/2023/03/10/…`) |
| `B_nested_redirect` | 112 of 2,775 (4.0%) | Mostly login / sign-up pages; occasionally an article with a `?redirect_to=` suffix (`ruhrnachrichten.de/...?redirect_to=…`), whose canonical URL is linked elsewhere |
| `B_faceted` | 224 of 2,414 (9.3%) | Distinct browse / search permutations (DSpace author browse, shop price bands). Listing pages, not items |
| `B_calendar` | 7,394 of 12,973 (57%) | Distinct day / list views of event calendars. Real listings of low search value; see the note above |

Limits: the fetched sample only covers URLs the scheduler chose to fetch, the
repeat-title signal misses pages whose duplicate was never fetched, and golden
is biased towards popular pages, so long-tail false positives may be higher
than 0.0029%. The ingest audit log plus `trap_filter_golden_check.py` measures
the production rate.

## Normalization

Stripping changes a URL's identity, and every entry point (ingest,
`golden_inject.py`, the coverage metric) has to agree on it, so ingest strips
only `srsltid` for now and shadow-logs every other candidate.

| Param | Ingest | Evidence |
|---|---|---|
| `srsltid` | **stripped** | Google documents it as Merchant Center attribution only; a new value per search |
| `;jsessionid=`, `jsessionid`, `phpsessid` | logged (pending) | phase 2 duplicate check: two session variants of a page matched in 92.5% / 95.8% of fetched pairs (different-page baseline 4.9%). Strong evidence, but stripping waits until `golden_inject` and the coverage metric apply the same policy |
| `utm_*`, `fbclid`, `_gl`, other tracking params | logged (`would_normalize`) | stripped vs. original matched 83% / 92% / 83% within 30 days, but most domains had no fetched pair to check |
| `gclid` | logged | 59% match, 15 domains |
| `oscsid`, `cfid`, `cftoken`, `zenid`, `aspsessionid*` | logged | `oscsid` 63% (3 domains); others too rare to measure |
| `sid`, `s`, `sessionid`, … holding a 32-hex token | logged | often a session (`sid` 97% in variant pairs), but `saskatchewan.ca/…?s=<32 hex>` is a search id |

The stripped set is `trap_filter.normalize_params`
(`url_normalize.strip_named_params`). It must stay identical across entry
points: `golden_inject.py` strips `srsltid` with `libs/urls.strip_srsltid`
(#103) and the coverage metric matches the same forms (WebSearchEngine #19);
a test checks that ingest produces the same URL as `strip_srsltid`. Adding a
param means changing all three together. The shadow form is the full
`url_normalize.strip_params(..., generic_session_tokens=True)`. Widen
`normalize_params` only after the `would_normalize` golden hits and a
same-page check on the shadow-logged URLs look clean.

The rest of the query keeps its original encoding and order byte for byte.
`scripts/golden_inject.py` strips `srsltid` from golden URLs (#103)
the same way (`libs/urls.strip_srsltid`), so an injected golden row and the
link the crawler discovers end up as the same URL.

## Checking and changing the rules

- Ingest logs every dropped / normalized / would-be-normalized link; `scripts/trap_filter_golden_check.py`
  reports golden false kills per rule ([06.18](06-maintenance-scripts.md)).
- Before promoting a rule to `HIGH_CONFIDENCE` or changing a threshold:
  run `match_rules` over the golden URL list and a frontier sample, compare
  golden hits against the table above, and read sampled examples per domain.
  A rule that hits mostly a few domains needs those domains checked by hand.
- Add a test in `tests/test_ingest_trap_filter.py` (or a trap-rule test) for
  each false positive you fix, with the URL that triggered it.
