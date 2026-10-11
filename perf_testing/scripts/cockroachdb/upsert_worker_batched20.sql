\timing on
BEGIN;
INSERT INTO url_state_current_165 (url, domain_id, last_fetch_ok, last_content_update, last_modified, num_fetch_ok_90d, num_fetch_fail_90d, num_content_update_90d, num_consecutive_fail, last_fail_reason, content_hash, should_crawl, title, hreflang_count, has_json_ld, etag, cache_control, is_redirect, redirect_hop_count, robots_bits)
VALUES
('https://perftest.example.com/crbatch_1_0_0', 92285143, now(), now(), now(), 1, 0, 1, 0, NULL, 'abc123hash', FALSE, 'Test Page Title', 2, TRUE, 'etag123', 'no-cache', FALSE, 0, 1),
('https://perftest.example.com/crbatch_1_0_1', 465623511, now(), now(), now(), 1, 0, 1, 0, NULL, 'abc123hash', FALSE, 'Test Page Title', 2, TRUE, 'etag123', 'no-cache', FALSE, 0, 1),
('https://perftest.example.com/crbatch_1_0_2', 449008935, now(), now(), now(), 1, 0, 1, 0, NULL, 'abc123hash', FALSE, 'Test Page Title', 2, TRUE, 'etag123', 'no-cache', FALSE, 0, 1),
('https://perftest.example.com/crbatch_1_0_3', 75006692, now(), now(), now(), 1, 0, 1, 0, NULL, 'abc123hash', FALSE, 'Test Page Title', 2, TRUE, 'etag123', 'no-cache', FALSE, 0, 1),
('https://perftest.example.com/crbatch_1_0_4', 258409930, now(), now(), now(), 1, 0, 1, 0, NULL, 'abc123hash', FALSE, 'Test Page Title', 2, TRUE, 'etag123', 'no-cache', FALSE, 0, 1),
('https://perftest.example.com/crbatch_1_0_5', 97402359, now(), now(), now(), 1, 0, 1, 0, NULL, 'abc123hash', FALSE, 'Test Page Title', 2, TRUE, 'etag123', 'no-cache', FALSE, 0, 1),
('https://perftest.example.com/crbatch_1_0_6', 591682484, now(), now(), now(), 1, 0, 1, 0, NULL, 'abc123hash', FALSE, 'Test Page Title', 2, TRUE, 'etag123', 'no-cache', FALSE, 0, 1),
('https://perftest.example.com/crbatch_1_0_7', 455824010, now(), now(), now(), 1, 0, 1, 0, NULL, 'abc123hash', FALSE, 'Test Page Title', 2, TRUE, 'etag123', 'no-cache', FALSE, 0, 1),
('https://perftest.example.com/crbatch_1_0_8', 63469422, now(), now(), now(), 1, 0, 1, 0, NULL, 'abc123hash', FALSE, 'Test Page Title', 2, TRUE, 'etag123', 'no-cache', FALSE, 0, 1),
('https://perftest.example.com/crbatch_1_0_9', 887825708, now(), now(), now(), 1, 0, 1, 0, NULL, 'abc123hash', FALSE, 'Test Page Title', 2, TRUE, 'etag123', 'no-cache', FALSE, 0, 1),
('https://perftest.example.com/crbatch_1_0_10', 607151284, now(), now(), now(), 1, 0, 1, 0, NULL, 'abc123hash', FALSE, 'Test Page Title', 2, TRUE, 'etag123', 'no-cache', FALSE, 0, 1),
('https://perftest.example.com/crbatch_1_0_11', 132931337, now(), now(), now(), 1, 0, 1, 0, NULL, 'abc123hash', FALSE, 'Test Page Title', 2, TRUE, 'etag123', 'no-cache', FALSE, 0, 1),
('https://perftest.example.com/crbatch_1_0_12', 239701015, now(), now(), now(), 1, 0, 1, 0, NULL, 'abc123hash', FALSE, 'Test Page Title', 2, TRUE, 'etag123', 'no-cache', FALSE, 0, 1),
('https://perftest.example.com/crbatch_1_0_13', 677129423, now(), now(), now(), 1, 0, 1, 0, NULL, 'abc123hash', FALSE, 'Test Page Title', 2, TRUE, 'etag123', 'no-cache', FALSE, 0, 1),
('https://perftest.example.com/crbatch_1_0_14', 673701294, now(), now(), now(), 1, 0, 1, 0, NULL, 'abc123hash', FALSE, 'Test Page Title', 2, TRUE, 'etag123', 'no-cache', FALSE, 0, 1),
('https://perftest.example.com/crbatch_1_0_15', 625988157, now(), now(), now(), 1, 0, 1, 0, NULL, 'abc123hash', FALSE, 'Test Page Title', 2, TRUE, 'etag123', 'no-cache', FALSE, 0, 1),
('https://perftest.example.com/crbatch_1_0_16', 66423869, now(), now(), now(), 1, 0, 1, 0, NULL, 'abc123hash', FALSE, 'Test Page Title', 2, TRUE, 'etag123', 'no-cache', FALSE, 0, 1),
('https://perftest.example.com/crbatch_1_0_17', 619659572, now(), now(), now(), 1, 0, 1, 0, NULL, 'abc123hash', FALSE, 'Test Page Title', 2, TRUE, 'etag123', 'no-cache', FALSE, 0, 1),
('https://perftest.example.com/crbatch_1_0_18', 628720318, now(), now(), now(), 1, 0, 1, 0, NULL, 'abc123hash', FALSE, 'Test Page Title', 2, TRUE, 'etag123', 'no-cache', FALSE, 0, 1),
('https://perftest.example.com/crbatch_1_0_19', 425932422, now(), now(), now(), 1, 0, 1, 0, NULL, 'abc123hash', FALSE, 'Test Page Title', 2, TRUE, 'etag123', 'no-cache', FALSE, 0, 1)
ON CONFLICT (url) DO UPDATE SET
  last_fetch_ok = COALESCE(EXCLUDED.last_fetch_ok, url_state_current_165.last_fetch_ok),
  num_fetch_ok_90d = url_state_current_165.num_fetch_ok_90d + EXCLUDED.num_fetch_ok_90d;
ROLLBACK;
BEGIN;
INSERT INTO url_state_current_165 (url, domain_id, last_fetch_ok, last_content_update, last_modified, num_fetch_ok_90d, num_fetch_fail_90d, num_content_update_90d, num_consecutive_fail, last_fail_reason, content_hash, should_crawl, title, hreflang_count, has_json_ld, etag, cache_control, is_redirect, redirect_hop_count, robots_bits)
VALUES
('https://perftest.example.com/crbatch_1_1_0', 53246120, now(), now(), now(), 1, 0, 1, 0, NULL, 'abc123hash', FALSE, 'Test Page Title', 2, TRUE, 'etag123', 'no-cache', FALSE, 0, 1),
('https://perftest.example.com/crbatch_1_1_1', 237384805, now(), now(), now(), 1, 0, 1, 0, NULL, 'abc123hash', FALSE, 'Test Page Title', 2, TRUE, 'etag123', 'no-cache', FALSE, 0, 1),
('https://perftest.example.com/crbatch_1_1_2', 50017773, now(), now(), now(), 1, 0, 1, 0, NULL, 'abc123hash', FALSE, 'Test Page Title', 2, TRUE, 'etag123', 'no-cache', FALSE, 0, 1),
('https://perftest.example.com/crbatch_1_1_3', 597714384, now(), now(), now(), 1, 0, 1, 0, NULL, 'abc123hash', FALSE, 'Test Page Title', 2, TRUE, 'etag123', 'no-cache', FALSE, 0, 1),
('https://perftest.example.com/crbatch_1_1_4', 921773491, now(), now(), now(), 1, 0, 1, 0, NULL, 'abc123hash', FALSE, 'Test Page Title', 2, TRUE, 'etag123', 'no-cache', FALSE, 0, 1),
('https://perftest.example.com/crbatch_1_1_5', 142995372, now(), now(), now(), 1, 0, 1, 0, NULL, 'abc123hash', FALSE, 'Test Page Title', 2, TRUE, 'etag123', 'no-cache', FALSE, 0, 1),
('https://perftest.example.com/crbatch_1_1_6', 310965606, now(), now(), now(), 1, 0, 1, 0, NULL, 'abc123hash', FALSE, 'Test Page Title', 2, TRUE, 'etag123', 'no-cache', FALSE, 0, 1),
('https://perftest.example.com/crbatch_1_1_7', 450047121, now(), now(), now(), 1, 0, 1, 0, NULL, 'abc123hash', FALSE, 'Test Page Title', 2, TRUE, 'etag123', 'no-cache', FALSE, 0, 1),
('https://perftest.example.com/crbatch_1_1_8', 154892714, now(), now(), now(), 1, 0, 1, 0, NULL, 'abc123hash', FALSE, 'Test Page Title', 2, TRUE, 'etag123', 'no-cache', FALSE, 0, 1),
('https://perftest.example.com/crbatch_1_1_9', 580557052, now(), now(), now(), 1, 0, 1, 0, NULL, 'abc123hash', FALSE, 'Test Page Title', 2, TRUE, 'etag123', 'no-cache', FALSE, 0, 1),
('https://perftest.example.com/crbatch_1_1_10', 126478449, now(), now(), now(), 1, 0, 1, 0, NULL, 'abc123hash', FALSE, 'Test Page Title', 2, TRUE, 'etag123', 'no-cache', FALSE, 0, 1),
('https://perftest.example.com/crbatch_1_1_11', 613013911, now(), now(), now(), 1, 0, 1, 0, NULL, 'abc123hash', FALSE, 'Test Page Title', 2, TRUE, 'etag123', 'no-cache', FALSE, 0, 1),
('https://perftest.example.com/crbatch_1_1_12', 331229839, now(), now(), now(), 1, 0, 1, 0, NULL, 'abc123hash', FALSE, 'Test Page Title', 2, TRUE, 'etag123', 'no-cache', FALSE, 0, 1),
('https://perftest.example.com/crbatch_1_1_13', 601571671, now(), now(), now(), 1, 0, 1, 0, NULL, 'abc123hash', FALSE, 'Test Page Title', 2, TRUE, 'etag123', 'no-cache', FALSE, 0, 1),
('https://perftest.example.com/crbatch_1_1_14', 876309004, now(), now(), now(), 1, 0, 1, 0, NULL, 'abc123hash', FALSE, 'Test Page Title', 2, TRUE, 'etag123', 'no-cache', FALSE, 0, 1),
('https://perftest.example.com/crbatch_1_1_15', 732294822, now(), now(), now(), 1, 0, 1, 0, NULL, 'abc123hash', FALSE, 'Test Page Title', 2, TRUE, 'etag123', 'no-cache', FALSE, 0, 1),
('https://perftest.example.com/crbatch_1_1_16', 194053475, now(), now(), now(), 1, 0, 1, 0, NULL, 'abc123hash', FALSE, 'Test Page Title', 2, TRUE, 'etag123', 'no-cache', FALSE, 0, 1),
('https://perftest.example.com/crbatch_1_1_17', 110655225, now(), now(), now(), 1, 0, 1, 0, NULL, 'abc123hash', FALSE, 'Test Page Title', 2, TRUE, 'etag123', 'no-cache', FALSE, 0, 1),
('https://perftest.example.com/crbatch_1_1_18', 624488421, now(), now(), now(), 1, 0, 1, 0, NULL, 'abc123hash', FALSE, 'Test Page Title', 2, TRUE, 'etag123', 'no-cache', FALSE, 0, 1),
('https://perftest.example.com/crbatch_1_1_19', 613326043, now(), now(), now(), 1, 0, 1, 0, NULL, 'abc123hash', FALSE, 'Test Page Title', 2, TRUE, 'etag123', 'no-cache', FALSE, 0, 1)
ON CONFLICT (url) DO UPDATE SET
  last_fetch_ok = COALESCE(EXCLUDED.last_fetch_ok, url_state_current_165.last_fetch_ok),
  num_fetch_ok_90d = url_state_current_165.num_fetch_ok_90d + EXCLUDED.num_fetch_ok_90d;
ROLLBACK;
BEGIN;
INSERT INTO url_state_current_165 (url, domain_id, last_fetch_ok, last_content_update, last_modified, num_fetch_ok_90d, num_fetch_fail_90d, num_content_update_90d, num_consecutive_fail, last_fail_reason, content_hash, should_crawl, title, hreflang_count, has_json_ld, etag, cache_control, is_redirect, redirect_hop_count, robots_bits)
VALUES
('https://perftest.example.com/crbatch_1_2_0', 686028114, now(), now(), now(), 1, 0, 1, 0, NULL, 'abc123hash', FALSE, 'Test Page Title', 2, TRUE, 'etag123', 'no-cache', FALSE, 0, 1),
('https://perftest.example.com/crbatch_1_2_1', 201724978, now(), now(), now(), 1, 0, 1, 0, NULL, 'abc123hash', FALSE, 'Test Page Title', 2, TRUE, 'etag123', 'no-cache', FALSE, 0, 1),
('https://perftest.example.com/crbatch_1_2_2', 399858817, now(), now(), now(), 1, 0, 1, 0, NULL, 'abc123hash', FALSE, 'Test Page Title', 2, TRUE, 'etag123', 'no-cache', FALSE, 0, 1),
('https://perftest.example.com/crbatch_1_2_3', 104615285, now(), now(), now(), 1, 0, 1, 0, NULL, 'abc123hash', FALSE, 'Test Page Title', 2, TRUE, 'etag123', 'no-cache', FALSE, 0, 1),
('https://perftest.example.com/crbatch_1_2_4', 588136139, now(), now(), now(), 1, 0, 1, 0, NULL, 'abc123hash', FALSE, 'Test Page Title', 2, TRUE, 'etag123', 'no-cache', FALSE, 0, 1),
('https://perftest.example.com/crbatch_1_2_5', 764623113, now(), now(), now(), 1, 0, 1, 0, NULL, 'abc123hash', FALSE, 'Test Page Title', 2, TRUE, 'etag123', 'no-cache', FALSE, 0, 1),
('https://perftest.example.com/crbatch_1_2_6', 67419150, now(), now(), now(), 1, 0, 1, 0, NULL, 'abc123hash', FALSE, 'Test Page Title', 2, TRUE, 'etag123', 'no-cache', FALSE, 0, 1),
('https://perftest.example.com/crbatch_1_2_7', 605985841, now(), now(), now(), 1, 0, 1, 0, NULL, 'abc123hash', FALSE, 'Test Page Title', 2, TRUE, 'etag123', 'no-cache', FALSE, 0, 1),
('https://perftest.example.com/crbatch_1_2_8', 63996270, now(), now(), now(), 1, 0, 1, 0, NULL, 'abc123hash', FALSE, 'Test Page Title', 2, TRUE, 'etag123', 'no-cache', FALSE, 0, 1),
('https://perftest.example.com/crbatch_1_2_9', 664656493, now(), now(), now(), 1, 0, 1, 0, NULL, 'abc123hash', FALSE, 'Test Page Title', 2, TRUE, 'etag123', 'no-cache', FALSE, 0, 1),
('https://perftest.example.com/crbatch_1_2_10', 221146488, now(), now(), now(), 1, 0, 1, 0, NULL, 'abc123hash', FALSE, 'Test Page Title', 2, TRUE, 'etag123', 'no-cache', FALSE, 0, 1),
('https://perftest.example.com/crbatch_1_2_11', 533021002, now(), now(), now(), 1, 0, 1, 0, NULL, 'abc123hash', FALSE, 'Test Page Title', 2, TRUE, 'etag123', 'no-cache', FALSE, 0, 1),
('https://perftest.example.com/crbatch_1_2_12', 730573910, now(), now(), now(), 1, 0, 1, 0, NULL, 'abc123hash', FALSE, 'Test Page Title', 2, TRUE, 'etag123', 'no-cache', FALSE, 0, 1),
('https://perftest.example.com/crbatch_1_2_13', 570930265, now(), now(), now(), 1, 0, 1, 0, NULL, 'abc123hash', FALSE, 'Test Page Title', 2, TRUE, 'etag123', 'no-cache', FALSE, 0, 1),
('https://perftest.example.com/crbatch_1_2_14', 459123744, now(), now(), now(), 1, 0, 1, 0, NULL, 'abc123hash', FALSE, 'Test Page Title', 2, TRUE, 'etag123', 'no-cache', FALSE, 0, 1),
('https://perftest.example.com/crbatch_1_2_15', 834543047, now(), now(), now(), 1, 0, 1, 0, NULL, 'abc123hash', FALSE, 'Test Page Title', 2, TRUE, 'etag123', 'no-cache', FALSE, 0, 1),
('https://perftest.example.com/crbatch_1_2_16', 337312956, now(), now(), now(), 1, 0, 1, 0, NULL, 'abc123hash', FALSE, 'Test Page Title', 2, TRUE, 'etag123', 'no-cache', FALSE, 0, 1),
('https://perftest.example.com/crbatch_1_2_17', 499936197, now(), now(), now(), 1, 0, 1, 0, NULL, 'abc123hash', FALSE, 'Test Page Title', 2, TRUE, 'etag123', 'no-cache', FALSE, 0, 1),
('https://perftest.example.com/crbatch_1_2_18', 628742261, now(), now(), now(), 1, 0, 1, 0, NULL, 'abc123hash', FALSE, 'Test Page Title', 2, TRUE, 'etag123', 'no-cache', FALSE, 0, 1),
('https://perftest.example.com/crbatch_1_2_19', 991537634, now(), now(), now(), 1, 0, 1, 0, NULL, 'abc123hash', FALSE, 'Test Page Title', 2, TRUE, 'etag123', 'no-cache', FALSE, 0, 1)
ON CONFLICT (url) DO UPDATE SET
  last_fetch_ok = COALESCE(EXCLUDED.last_fetch_ok, url_state_current_165.last_fetch_ok),
  num_fetch_ok_90d = url_state_current_165.num_fetch_ok_90d + EXCLUDED.num_fetch_ok_90d;
ROLLBACK;
-- [trimmed to 3 sample iterations out of original run; actual test looped this pattern 20000x per worker]
