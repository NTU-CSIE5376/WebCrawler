\timing on
BEGIN;
INSERT INTO url_state_current (url, domain_id, last_fetch_ok, last_content_update, last_modified, num_fetch_ok_90d, num_fetch_fail_90d, num_content_update_90d, num_consecutive_fail, last_fail_reason, content_hash, should_crawl, title, hreflang_count, has_json_ld, etag, cache_control, is_redirect, redirect_hop_count, robots_bits)
VALUES
('https://perftest.example.com/batch_1_0_0', 292548247, now(), now(), now(), 1, 0, 1, 0, NULL, 'abc123hash', FALSE, 'Test Page Title', 2, TRUE, 'etag123', 'no-cache', FALSE, 0, 1),
('https://perftest.example.com/batch_1_0_1', 968984364, now(), now(), now(), 1, 0, 1, 0, NULL, 'abc123hash', FALSE, 'Test Page Title', 2, TRUE, 'etag123', 'no-cache', FALSE, 0, 1),
('https://perftest.example.com/batch_1_0_2', 308373323, now(), now(), now(), 1, 0, 1, 0, NULL, 'abc123hash', FALSE, 'Test Page Title', 2, TRUE, 'etag123', 'no-cache', FALSE, 0, 1),
('https://perftest.example.com/batch_1_0_3', 917603325, now(), now(), now(), 1, 0, 1, 0, NULL, 'abc123hash', FALSE, 'Test Page Title', 2, TRUE, 'etag123', 'no-cache', FALSE, 0, 1),
('https://perftest.example.com/batch_1_0_4', 719629804, now(), now(), now(), 1, 0, 1, 0, NULL, 'abc123hash', FALSE, 'Test Page Title', 2, TRUE, 'etag123', 'no-cache', FALSE, 0, 1),
('https://perftest.example.com/batch_1_0_5', 822928700, now(), now(), now(), 1, 0, 1, 0, NULL, 'abc123hash', FALSE, 'Test Page Title', 2, TRUE, 'etag123', 'no-cache', FALSE, 0, 1),
('https://perftest.example.com/batch_1_0_6', 395070151, now(), now(), now(), 1, 0, 1, 0, NULL, 'abc123hash', FALSE, 'Test Page Title', 2, TRUE, 'etag123', 'no-cache', FALSE, 0, 1),
('https://perftest.example.com/batch_1_0_7', 406766697, now(), now(), now(), 1, 0, 1, 0, NULL, 'abc123hash', FALSE, 'Test Page Title', 2, TRUE, 'etag123', 'no-cache', FALSE, 0, 1),
('https://perftest.example.com/batch_1_0_8', 569874833, now(), now(), now(), 1, 0, 1, 0, NULL, 'abc123hash', FALSE, 'Test Page Title', 2, TRUE, 'etag123', 'no-cache', FALSE, 0, 1),
('https://perftest.example.com/batch_1_0_9', 84905737, now(), now(), now(), 1, 0, 1, 0, NULL, 'abc123hash', FALSE, 'Test Page Title', 2, TRUE, 'etag123', 'no-cache', FALSE, 0, 1),
('https://perftest.example.com/batch_1_0_10', 208447654, now(), now(), now(), 1, 0, 1, 0, NULL, 'abc123hash', FALSE, 'Test Page Title', 2, TRUE, 'etag123', 'no-cache', FALSE, 0, 1),
('https://perftest.example.com/batch_1_0_11', 587406305, now(), now(), now(), 1, 0, 1, 0, NULL, 'abc123hash', FALSE, 'Test Page Title', 2, TRUE, 'etag123', 'no-cache', FALSE, 0, 1),
('https://perftest.example.com/batch_1_0_12', 553431374, now(), now(), now(), 1, 0, 1, 0, NULL, 'abc123hash', FALSE, 'Test Page Title', 2, TRUE, 'etag123', 'no-cache', FALSE, 0, 1),
('https://perftest.example.com/batch_1_0_13', 831773841, now(), now(), now(), 1, 0, 1, 0, NULL, 'abc123hash', FALSE, 'Test Page Title', 2, TRUE, 'etag123', 'no-cache', FALSE, 0, 1),
('https://perftest.example.com/batch_1_0_14', 572324821, now(), now(), now(), 1, 0, 1, 0, NULL, 'abc123hash', FALSE, 'Test Page Title', 2, TRUE, 'etag123', 'no-cache', FALSE, 0, 1),
('https://perftest.example.com/batch_1_0_15', 686750682, now(), now(), now(), 1, 0, 1, 0, NULL, 'abc123hash', FALSE, 'Test Page Title', 2, TRUE, 'etag123', 'no-cache', FALSE, 0, 1),
('https://perftest.example.com/batch_1_0_16', 117337576, now(), now(), now(), 1, 0, 1, 0, NULL, 'abc123hash', FALSE, 'Test Page Title', 2, TRUE, 'etag123', 'no-cache', FALSE, 0, 1),
('https://perftest.example.com/batch_1_0_17', 381348034, now(), now(), now(), 1, 0, 1, 0, NULL, 'abc123hash', FALSE, 'Test Page Title', 2, TRUE, 'etag123', 'no-cache', FALSE, 0, 1),
('https://perftest.example.com/batch_1_0_18', 350168498, now(), now(), now(), 1, 0, 1, 0, NULL, 'abc123hash', FALSE, 'Test Page Title', 2, TRUE, 'etag123', 'no-cache', FALSE, 0, 1),
('https://perftest.example.com/batch_1_0_19', 512070283, now(), now(), now(), 1, 0, 1, 0, NULL, 'abc123hash', FALSE, 'Test Page Title', 2, TRUE, 'etag123', 'no-cache', FALSE, 0, 1)
ON CONFLICT (domain_id, url) DO UPDATE SET
  last_fetch_ok = COALESCE(EXCLUDED.last_fetch_ok, url_state_current.last_fetch_ok),
  num_fetch_ok_90d = url_state_current.num_fetch_ok_90d + EXCLUDED.num_fetch_ok_90d;
ROLLBACK;
BEGIN;
INSERT INTO url_state_current (url, domain_id, last_fetch_ok, last_content_update, last_modified, num_fetch_ok_90d, num_fetch_fail_90d, num_content_update_90d, num_consecutive_fail, last_fail_reason, content_hash, should_crawl, title, hreflang_count, has_json_ld, etag, cache_control, is_redirect, redirect_hop_count, robots_bits)
VALUES
('https://perftest.example.com/batch_1_1_0', 781610491, now(), now(), now(), 1, 0, 1, 0, NULL, 'abc123hash', FALSE, 'Test Page Title', 2, TRUE, 'etag123', 'no-cache', FALSE, 0, 1),
('https://perftest.example.com/batch_1_1_1', 409147926, now(), now(), now(), 1, 0, 1, 0, NULL, 'abc123hash', FALSE, 'Test Page Title', 2, TRUE, 'etag123', 'no-cache', FALSE, 0, 1),
('https://perftest.example.com/batch_1_1_2', 525658642, now(), now(), now(), 1, 0, 1, 0, NULL, 'abc123hash', FALSE, 'Test Page Title', 2, TRUE, 'etag123', 'no-cache', FALSE, 0, 1),
('https://perftest.example.com/batch_1_1_3', 100873600, now(), now(), now(), 1, 0, 1, 0, NULL, 'abc123hash', FALSE, 'Test Page Title', 2, TRUE, 'etag123', 'no-cache', FALSE, 0, 1),
('https://perftest.example.com/batch_1_1_4', 873510364, now(), now(), now(), 1, 0, 1, 0, NULL, 'abc123hash', FALSE, 'Test Page Title', 2, TRUE, 'etag123', 'no-cache', FALSE, 0, 1),
('https://perftest.example.com/batch_1_1_5', 810326427, now(), now(), now(), 1, 0, 1, 0, NULL, 'abc123hash', FALSE, 'Test Page Title', 2, TRUE, 'etag123', 'no-cache', FALSE, 0, 1),
('https://perftest.example.com/batch_1_1_6', 927194745, now(), now(), now(), 1, 0, 1, 0, NULL, 'abc123hash', FALSE, 'Test Page Title', 2, TRUE, 'etag123', 'no-cache', FALSE, 0, 1),
('https://perftest.example.com/batch_1_1_7', 612150706, now(), now(), now(), 1, 0, 1, 0, NULL, 'abc123hash', FALSE, 'Test Page Title', 2, TRUE, 'etag123', 'no-cache', FALSE, 0, 1),
('https://perftest.example.com/batch_1_1_8', 881370688, now(), now(), now(), 1, 0, 1, 0, NULL, 'abc123hash', FALSE, 'Test Page Title', 2, TRUE, 'etag123', 'no-cache', FALSE, 0, 1),
('https://perftest.example.com/batch_1_1_9', 369555667, now(), now(), now(), 1, 0, 1, 0, NULL, 'abc123hash', FALSE, 'Test Page Title', 2, TRUE, 'etag123', 'no-cache', FALSE, 0, 1),
('https://perftest.example.com/batch_1_1_10', 616971250, now(), now(), now(), 1, 0, 1, 0, NULL, 'abc123hash', FALSE, 'Test Page Title', 2, TRUE, 'etag123', 'no-cache', FALSE, 0, 1),
('https://perftest.example.com/batch_1_1_11', 175540396, now(), now(), now(), 1, 0, 1, 0, NULL, 'abc123hash', FALSE, 'Test Page Title', 2, TRUE, 'etag123', 'no-cache', FALSE, 0, 1),
('https://perftest.example.com/batch_1_1_12', 461922106, now(), now(), now(), 1, 0, 1, 0, NULL, 'abc123hash', FALSE, 'Test Page Title', 2, TRUE, 'etag123', 'no-cache', FALSE, 0, 1),
('https://perftest.example.com/batch_1_1_13', 325923229, now(), now(), now(), 1, 0, 1, 0, NULL, 'abc123hash', FALSE, 'Test Page Title', 2, TRUE, 'etag123', 'no-cache', FALSE, 0, 1),
('https://perftest.example.com/batch_1_1_14', 730121901, now(), now(), now(), 1, 0, 1, 0, NULL, 'abc123hash', FALSE, 'Test Page Title', 2, TRUE, 'etag123', 'no-cache', FALSE, 0, 1),
('https://perftest.example.com/batch_1_1_15', 488016290, now(), now(), now(), 1, 0, 1, 0, NULL, 'abc123hash', FALSE, 'Test Page Title', 2, TRUE, 'etag123', 'no-cache', FALSE, 0, 1),
('https://perftest.example.com/batch_1_1_16', 919522342, now(), now(), now(), 1, 0, 1, 0, NULL, 'abc123hash', FALSE, 'Test Page Title', 2, TRUE, 'etag123', 'no-cache', FALSE, 0, 1),
('https://perftest.example.com/batch_1_1_17', 203151126, now(), now(), now(), 1, 0, 1, 0, NULL, 'abc123hash', FALSE, 'Test Page Title', 2, TRUE, 'etag123', 'no-cache', FALSE, 0, 1),
('https://perftest.example.com/batch_1_1_18', 910745471, now(), now(), now(), 1, 0, 1, 0, NULL, 'abc123hash', FALSE, 'Test Page Title', 2, TRUE, 'etag123', 'no-cache', FALSE, 0, 1),
('https://perftest.example.com/batch_1_1_19', 466410789, now(), now(), now(), 1, 0, 1, 0, NULL, 'abc123hash', FALSE, 'Test Page Title', 2, TRUE, 'etag123', 'no-cache', FALSE, 0, 1)
ON CONFLICT (domain_id, url) DO UPDATE SET
  last_fetch_ok = COALESCE(EXCLUDED.last_fetch_ok, url_state_current.last_fetch_ok),
  num_fetch_ok_90d = url_state_current.num_fetch_ok_90d + EXCLUDED.num_fetch_ok_90d;
ROLLBACK;
BEGIN;
INSERT INTO url_state_current (url, domain_id, last_fetch_ok, last_content_update, last_modified, num_fetch_ok_90d, num_fetch_fail_90d, num_content_update_90d, num_consecutive_fail, last_fail_reason, content_hash, should_crawl, title, hreflang_count, has_json_ld, etag, cache_control, is_redirect, redirect_hop_count, robots_bits)
VALUES
('https://perftest.example.com/batch_1_2_0', 613432131, now(), now(), now(), 1, 0, 1, 0, NULL, 'abc123hash', FALSE, 'Test Page Title', 2, TRUE, 'etag123', 'no-cache', FALSE, 0, 1),
('https://perftest.example.com/batch_1_2_1', 723242555, now(), now(), now(), 1, 0, 1, 0, NULL, 'abc123hash', FALSE, 'Test Page Title', 2, TRUE, 'etag123', 'no-cache', FALSE, 0, 1),
('https://perftest.example.com/batch_1_2_2', 526998768, now(), now(), now(), 1, 0, 1, 0, NULL, 'abc123hash', FALSE, 'Test Page Title', 2, TRUE, 'etag123', 'no-cache', FALSE, 0, 1),
('https://perftest.example.com/batch_1_2_3', 838343660, now(), now(), now(), 1, 0, 1, 0, NULL, 'abc123hash', FALSE, 'Test Page Title', 2, TRUE, 'etag123', 'no-cache', FALSE, 0, 1),
('https://perftest.example.com/batch_1_2_4', 445010792, now(), now(), now(), 1, 0, 1, 0, NULL, 'abc123hash', FALSE, 'Test Page Title', 2, TRUE, 'etag123', 'no-cache', FALSE, 0, 1),
('https://perftest.example.com/batch_1_2_5', 654149748, now(), now(), now(), 1, 0, 1, 0, NULL, 'abc123hash', FALSE, 'Test Page Title', 2, TRUE, 'etag123', 'no-cache', FALSE, 0, 1),
('https://perftest.example.com/batch_1_2_6', 573938795, now(), now(), now(), 1, 0, 1, 0, NULL, 'abc123hash', FALSE, 'Test Page Title', 2, TRUE, 'etag123', 'no-cache', FALSE, 0, 1),
('https://perftest.example.com/batch_1_2_7', 985924221, now(), now(), now(), 1, 0, 1, 0, NULL, 'abc123hash', FALSE, 'Test Page Title', 2, TRUE, 'etag123', 'no-cache', FALSE, 0, 1),
('https://perftest.example.com/batch_1_2_8', 721955591, now(), now(), now(), 1, 0, 1, 0, NULL, 'abc123hash', FALSE, 'Test Page Title', 2, TRUE, 'etag123', 'no-cache', FALSE, 0, 1),
('https://perftest.example.com/batch_1_2_9', 560977028, now(), now(), now(), 1, 0, 1, 0, NULL, 'abc123hash', FALSE, 'Test Page Title', 2, TRUE, 'etag123', 'no-cache', FALSE, 0, 1),
('https://perftest.example.com/batch_1_2_10', 754739743, now(), now(), now(), 1, 0, 1, 0, NULL, 'abc123hash', FALSE, 'Test Page Title', 2, TRUE, 'etag123', 'no-cache', FALSE, 0, 1),
('https://perftest.example.com/batch_1_2_11', 926408302, now(), now(), now(), 1, 0, 1, 0, NULL, 'abc123hash', FALSE, 'Test Page Title', 2, TRUE, 'etag123', 'no-cache', FALSE, 0, 1),
('https://perftest.example.com/batch_1_2_12', 792930438, now(), now(), now(), 1, 0, 1, 0, NULL, 'abc123hash', FALSE, 'Test Page Title', 2, TRUE, 'etag123', 'no-cache', FALSE, 0, 1),
('https://perftest.example.com/batch_1_2_13', 571104179, now(), now(), now(), 1, 0, 1, 0, NULL, 'abc123hash', FALSE, 'Test Page Title', 2, TRUE, 'etag123', 'no-cache', FALSE, 0, 1),
('https://perftest.example.com/batch_1_2_14', 919592741, now(), now(), now(), 1, 0, 1, 0, NULL, 'abc123hash', FALSE, 'Test Page Title', 2, TRUE, 'etag123', 'no-cache', FALSE, 0, 1),
('https://perftest.example.com/batch_1_2_15', 725255243, now(), now(), now(), 1, 0, 1, 0, NULL, 'abc123hash', FALSE, 'Test Page Title', 2, TRUE, 'etag123', 'no-cache', FALSE, 0, 1),
('https://perftest.example.com/batch_1_2_16', 157152011, now(), now(), now(), 1, 0, 1, 0, NULL, 'abc123hash', FALSE, 'Test Page Title', 2, TRUE, 'etag123', 'no-cache', FALSE, 0, 1),
('https://perftest.example.com/batch_1_2_17', 524975511, now(), now(), now(), 1, 0, 1, 0, NULL, 'abc123hash', FALSE, 'Test Page Title', 2, TRUE, 'etag123', 'no-cache', FALSE, 0, 1),
('https://perftest.example.com/batch_1_2_18', 838058324, now(), now(), now(), 1, 0, 1, 0, NULL, 'abc123hash', FALSE, 'Test Page Title', 2, TRUE, 'etag123', 'no-cache', FALSE, 0, 1),
('https://perftest.example.com/batch_1_2_19', 882712416, now(), now(), now(), 1, 0, 1, 0, NULL, 'abc123hash', FALSE, 'Test Page Title', 2, TRUE, 'etag123', 'no-cache', FALSE, 0, 1)
ON CONFLICT (domain_id, url) DO UPDATE SET
  last_fetch_ok = COALESCE(EXCLUDED.last_fetch_ok, url_state_current.last_fetch_ok),
  num_fetch_ok_90d = url_state_current.num_fetch_ok_90d + EXCLUDED.num_fetch_ok_90d;
ROLLBACK;
-- [trimmed to 3 sample iterations out of original run; actual test looped this pattern 20000x per worker]
