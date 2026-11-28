"""Typed contracts for the router's discovery and fetched-content outputs."""
import pyarrow as pa

# Match build_new_link_record; explicit types avoid per-batch inference.
DISCOVERY_SCHEMA = pa.schema([
    ('url', pa.string()),
    ('status', pa.string()),
    ('shard_id', pa.int64()),
    ('domain_id', pa.int64()),
    ('domain_score', pa.float64()),
    ('discovered_from', pa.string()),
    ('discovery_source_type', pa.int64()),
    ('parent_page_score', pa.float64()),
    ('inlink_count_approx', pa.int64()),
    ('inlink_count_external', pa.int64()),
    ('anchor_text', pa.string()),
    ('url_priority', pa.float64()),
    ('source', pa.int64()),
])


CONTENT_SCHEMA = pa.schema([
    ('url', pa.string()),
    ('status', pa.string()),
    ('fetched_at', pa.string()),
    ('fail_reason', pa.string()),
    ('content', pa.large_string()),
    ('outlinks', pa.list_(pa.struct([
        ('url', pa.string()), ('domain_id', pa.int64()), ('anchor', pa.string()),
    ]))),
    ('shard_id', pa.int64()),
    ('domain_id', pa.int64()),
    ('domain_score', pa.float64()),
    ('content_hash', pa.string()),
    ('title', pa.string()),
    ('hreflang_count', pa.int64()),
    ('has_json_ld', pa.bool_()),
    ('last_modified', pa.string()),
    ('etag', pa.string()),
    ('cache_control', pa.string()),
    ('is_redirect', pa.bool_()),
    ('redirect_hop_count', pa.int64()),
])
CONTENT_METADATA_COLUMNS = [name for name in CONTENT_SCHEMA.names if name not in {'content', 'outlinks'}]
ROUTED_SCHEMAS = {'discovery': DISCOVERY_SCHEMA, 'content': CONTENT_SCHEMA}
