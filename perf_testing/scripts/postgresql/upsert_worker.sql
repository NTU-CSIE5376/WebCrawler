\timing on
BEGIN;
INSERT INTO url_state_current_148 (url, domain_id, last_fetch_ok, last_content_update, last_modified, num_fetch_ok_90d, num_fetch_fail_90d, num_content_update_90d, num_consecutive_fail, last_fail_reason, content_hash, should_crawl, title, hreflang_count, has_json_ld, etag, cache_control, is_redirect, redirect_hop_count, robots_bits)
VALUES ('https://perftest.example.com/w0', 550517526, now(), now(), now(), 1, 0, 1, 0, NULL, 'abc123hash', FALSE, 'Test Page Title', 2, TRUE, 'etag123', 'no-cache', FALSE, 0, 1)
ON CONFLICT (url) DO UPDATE SET
  last_fetch_ok = COALESCE(EXCLUDED.last_fetch_ok, url_state_current_148.last_fetch_ok),
  num_fetch_ok_90d = url_state_current_148.num_fetch_ok_90d + EXCLUDED.num_fetch_ok_90d;
ROLLBACK;
BEGIN;
INSERT INTO url_state_current_148 (url, domain_id, last_fetch_ok, last_content_update, last_modified, num_fetch_ok_90d, num_fetch_fail_90d, num_content_update_90d, num_consecutive_fail, last_fail_reason, content_hash, should_crawl, title, hreflang_count, has_json_ld, etag, cache_control, is_redirect, redirect_hop_count, robots_bits)
VALUES ('https://perftest.example.com/w1', 519285219, now(), now(), now(), 1, 0, 1, 0, NULL, 'abc123hash', FALSE, 'Test Page Title', 2, TRUE, 'etag123', 'no-cache', FALSE, 0, 1)
ON CONFLICT (url) DO UPDATE SET
  last_fetch_ok = COALESCE(EXCLUDED.last_fetch_ok, url_state_current_148.last_fetch_ok),
  num_fetch_ok_90d = url_state_current_148.num_fetch_ok_90d + EXCLUDED.num_fetch_ok_90d;
ROLLBACK;
BEGIN;
INSERT INTO url_state_current_148 (url, domain_id, last_fetch_ok, last_content_update, last_modified, num_fetch_ok_90d, num_fetch_fail_90d, num_content_update_90d, num_consecutive_fail, last_fail_reason, content_hash, should_crawl, title, hreflang_count, has_json_ld, etag, cache_control, is_redirect, redirect_hop_count, robots_bits)
VALUES ('https://perftest.example.com/w2', 265067904, now(), now(), now(), 1, 0, 1, 0, NULL, 'abc123hash', FALSE, 'Test Page Title', 2, TRUE, 'etag123', 'no-cache', FALSE, 0, 1)
ON CONFLICT (url) DO UPDATE SET
  last_fetch_ok = COALESCE(EXCLUDED.last_fetch_ok, url_state_current_148.last_fetch_ok),
  num_fetch_ok_90d = url_state_current_148.num_fetch_ok_90d + EXCLUDED.num_fetch_ok_90d;
ROLLBACK;
-- [trimmed to 3 sample iterations out of original run; actual test looped this pattern 20000x per worker]
