PREPARE ins_stmt (text, int8, timestamptz, timestamptz, timestamptz, int4, int4, int4, int4, text, text, bool, text, int4, bool, text, text, bool, int2, int2) AS
INSERT INTO url_state_current_148 (url, domain_id, last_fetch_ok, last_content_update, last_modified, num_fetch_ok_90d, num_fetch_fail_90d, num_content_update_90d, num_consecutive_fail, last_fail_reason, content_hash, should_crawl, title, hreflang_count, has_json_ld, etag, cache_control, is_redirect, redirect_hop_count, robots_bits)
VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,$19,$20)
ON CONFLICT (url) DO UPDATE SET
  last_fetch_ok = COALESCE(EXCLUDED.last_fetch_ok, url_state_current_148.last_fetch_ok),
  num_fetch_ok_90d = url_state_current_148.num_fetch_ok_90d + EXCLUDED.num_fetch_ok_90d;
\timing on
BEGIN;
EXECUTE ins_stmt('https://perftest.example.com/prepw1_0', 16530080, now(), now(), now(), 1, 0, 1, 0, NULL, 'abc123hash', FALSE, 'Test Page Title', 2, TRUE, 'etag123', 'no-cache', FALSE, 0, 1);
ROLLBACK;
BEGIN;
EXECUTE ins_stmt('https://perftest.example.com/prepw1_1', 747706023, now(), now(), now(), 1, 0, 1, 0, NULL, 'abc123hash', FALSE, 'Test Page Title', 2, TRUE, 'etag123', 'no-cache', FALSE, 0, 1);
ROLLBACK;
BEGIN;
EXECUTE ins_stmt('https://perftest.example.com/prepw1_2', 123042046, now(), now(), now(), 1, 0, 1, 0, NULL, 'abc123hash', FALSE, 'Test Page Title', 2, TRUE, 'etag123', 'no-cache', FALSE, 0, 1);
ROLLBACK;
-- [trimmed to 3 sample iterations out of original run; actual test looped this pattern 20000x per worker]
