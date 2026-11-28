import pyarrow as pa
import pyarrow.compute as pc

def extract_basic_batch(batch: pa.RecordBatch) -> pa.RecordBatch:
    """Extract successful rows column-wise, retaining ISO dates until SQL binding.

    The caller filters status first. Null lengths remain null so the caller can
    report malformed records individually before writing to the database.
    """
    return pa.RecordBatch.from_arrays([
        batch['url'], batch['shard_id'], batch['domain_id'], batch['fetched_at'],
        pc.utf8_length(batch['content']), batch['content_hash'],
        pc.list_value_length(batch['outlinks']),
    ], names=['url', 'shard_id', 'domain_id', 'fetched_at', 'content_length',
              'content_hash', 'num_links'])
