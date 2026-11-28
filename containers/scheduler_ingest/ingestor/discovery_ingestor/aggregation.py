"""Native Arrow aggregation for one discovery shard, with stable provenance."""
import pyarrow as pa
import pyarrow.compute as pc


COUNTERS = ['inlink_count_approx', 'inlink_count_external', '__inputs']
PARENT_FIELDS = ['discovered_from', 'parent_page_score', 'discovery_source_type']
PRIORITY_FIELDS = ['url_priority', 'source']


def aggregate_discoveries(table):
    """Two hash-aggregation passes; no score sorts or provenance joins.

    The caller partitions by shard. Min row indices preserve the first row,
    first nonempty anchor and earliest parent/priority among tied maxima.
    Decimal sums detect counter overflow rather than silently wrapping int64.
    """
    if pc.count_distinct(table['shard_id']).as_py() != 1:
        raise ValueError('discovery aggregation requires exactly one shard')
    for name in COUNTERS:
        if table[name].null_count:
            raise ValueError(f'null discovery counter: {name}')
    for name in ('parent_page_score', 'url_priority'):
        if pc.any(pc.invert(pc.fill_null(pc.is_finite(table[name]), True))).as_py():
            raise ValueError(f'non-finite discovery score: {name}')

    row = pa.array(range(table.num_rows), type=pa.int64())
    # Only projected columns participate in aggregation; full metadata stays
    # in the source until the final representative rows are selected.
    inputs = pa.table({
        'url': table['url'], '__row': row,
        '__anchor': pc.if_else(pc.equal(table['anchor_text'], ''), None, table['anchor_text']),
        'parent_page_score': table['parent_page_score'], 'url_priority': table['url_priority'],
        '__inputs': table['__inputs'],
        **{name:pc.cast(table[name], pa.decimal128(38, 0)) for name in COUNTERS[:2]},
    })
    grouped = inputs.group_by('url', use_threads=False).aggregate([
        ('__row', 'min'), ('__anchor', 'first'), ('parent_page_score', 'max'), ('url_priority', 'max'),
        *[(name, 'sum') for name in COUNTERS],
    ]).sort_by([('__row_min', 'ascending')])

    group_index = pc.index_in(table['url'], value_set=grouped['url'])
    candidates = {'url':table['url']}
    for name in ('parent_page_score', 'url_priority'):
        maximum = pc.take(grouped[name+'_max'], group_index)
        wins = pc.or_(pc.is_null(maximum), pc.fill_null(pc.equal(table[name], maximum), False))
        candidates[name] = pc.if_else(wins, row, None)
    winners = pa.table(candidates).group_by('url', use_threads=False).aggregate([
        ('parent_page_score','min'), ('url_priority','min'),
    ])
    winner_index = pc.index_in(grouped['url'], value_set=winners['url'])

    output = table.take(grouped['__row_min'])
    replacements = {name:pc.cast(grouped[name+'_sum'], pa.int64(), safe=True) for name in COUNTERS}
    replacements['anchor_text'] = pc.coalesce(grouped['__anchor_first'], output['anchor_text'])
    for score, fields in [('parent_page_score', PARENT_FIELDS), ('url_priority', PRIORITY_FIELDS)]:
        indices = pc.take(winners[score+'_min'], winner_index)
        selected = table.select(fields).take(indices)
        replacements.update({name:selected[name] for name in fields})
    for name, values in replacements.items():
        output = output.set_column(output.schema.get_field_index(name), name, values)
    return output
