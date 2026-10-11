BEGIN;
\timing on
INSERT INTO url_state_current (url, domain_id, last_fetch_ok, last_content_update, last_modified, num_fetch_ok_90d, num_fetch_fail_90d, num_content_update_90d, num_consecutive_fail, last_fail_reason, content_hash, should_crawl, title, hreflang_count, has_json_ld, etag, cache_control, is_redirect, redirect_hop_count, robots_bits)
VALUES ('https://perftest.example.com/c30_0', 490996524, now(), now(), now(), 1, 0, 1, 0, NULL, 'abc123hash', FALSE, 'Test Page Title', 2, TRUE, 'etag123', 'no-cache', FALSE, 0, 1)
ON CONFLICT (domain_id, url) DO UPDATE SET
  last_fetch_ok = COALESCE(EXCLUDED.last_fetch_ok, url_state_current.last_fetch_ok),
  num_fetch_ok_90d = url_state_current.num_fetch_ok_90d + EXCLUDED.num_fetch_ok_90d;
\timing off
ROLLBACK;
BEGIN;
\timing on
INSERT INTO url_state_current (url, domain_id, last_fetch_ok, last_content_update, last_modified, num_fetch_ok_90d, num_fetch_fail_90d, num_content_update_90d, num_consecutive_fail, last_fail_reason, content_hash, should_crawl, title, hreflang_count, has_json_ld, etag, cache_control, is_redirect, redirect_hop_count, robots_bits)
VALUES ('https://perftest.example.com/c30_1', 932612958, now(), now(), now(), 1, 0, 1, 0, NULL, 'abc123hash', FALSE, 'Test Page Title', 2, TRUE, 'etag123', 'no-cache', FALSE, 0, 1)
ON CONFLICT (domain_id, url) DO UPDATE SET
  last_fetch_ok = COALESCE(EXCLUDED.last_fetch_ok, url_state_current.last_fetch_ok),
  num_fetch_ok_90d = url_state_current.num_fetch_ok_90d + EXCLUDED.num_fetch_ok_90d;
\timing off
ROLLBACK;
BEGIN;
\timing on
INSERT INTO url_state_current (url, domain_id, last_fetch_ok, last_content_update, last_modified, num_fetch_ok_90d, num_fetch_fail_90d, num_content_update_90d, num_consecutive_fail, last_fail_reason, content_hash, should_crawl, title, hreflang_count, has_json_ld, etag, cache_control, is_redirect, redirect_hop_count, robots_bits)
VALUES ('https://perftest.example.com/c30_2', 806581850, now(), now(), now(), 1, 0, 1, 0, NULL, 'abc123hash', FALSE, 'Test Page Title', 2, TRUE, 'etag123', 'no-cache', FALSE, 0, 1)
ON CONFLICT (domain_id, url) DO UPDATE SET
  last_fetch_ok = COALESCE(EXCLUDED.last_fetch_ok, url_state_current.last_fetch_ok),
  num_fetch_ok_90d = url_state_current.num_fetch_ok_90d + EXCLUDED.num_fetch_ok_90d;
\timing off
ROLLBACK;
-- [trimmed to 3 sample iterations out of original run; actual test looped this pattern 20000x per worker]
