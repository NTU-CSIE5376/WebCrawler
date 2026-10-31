# 09. Frontier Trap Rules

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
| Duplicate class | `A_session_id`, `B_tracking_params` | `srsltid`, `jsessionid`, `phpsessid` stripped (`normalize_params`); every other tracking / session param only logged as `would_normalize`. Link kept |
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

## Normalization

Stripping changes a URL's identity, so ingest only strips params with strong
evidence that the page is the same without them, and shadow-logs the rest.

| Param | Ingest | Evidence |
|---|---|---|
| `srsltid` | **stripped** | Google documents it as Merchant Center attribution only; a new value per search |
| `;jsessionid=`, `jsessionid`, `phpsessid` | **stripped** | phase 2 duplicate check: two session variants of a page matched in 92.5% / 95.8% of fetched pairs (different-page baseline 4.9%) |
| `utm_*`, `fbclid`, `_gl`, other tracking params | logged (`would_normalize`) | stripped vs. original matched 83% / 92% / 83% within 30 days, but most domains had no fetched pair to check |
| `gclid` | logged | 59% match, 15 domains |
| `oscsid`, `cfid`, `cftoken`, `zenid`, `aspsessionid*` | logged | `oscsid` 63% (3 domains); others too rare to measure |
| `sid`, `s`, `sessionid`, … holding a 32-hex token | logged | often a session (`sid` 97% in variant pairs), but `saskatchewan.ca/…?s=<32 hex>` is a search id |

The stripped set is `trap_filter.normalize_params`
(`url_normalize.strip_named_params`); the shadow form is the full
`url_normalize.strip_params(..., generic_session_tokens=True)`. Widen
`normalize_params` only after the `would_normalize` golden hits and a
same-page check on the shadow-logged URLs look clean.

The rest of the query keeps its original encoding and order byte for byte.
With PR #103, `scripts/golden_inject.py` strips `srsltid` from golden URLs
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
