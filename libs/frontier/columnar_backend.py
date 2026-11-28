"""Batch URL policy: parse syntax once, then evaluate columnar rule masks."""
from urllib.parse import unquote, unquote_plus, urlsplit, urlunsplit

import pyarrow as pa
import pyarrow.compute as pc

from . import trap_rules as rules
from . import url_normalize as normalize


FEATURE_SCHEMA = pa.schema([
    ('url',pa.string()), ('normalized_url',pa.string()), ('shadow_url',pa.string()),
    ('path',pa.string()), ('query',pa.string()), ('valid',pa.bool_()),
    ('segments',pa.int32()), ('long_url',pa.bool_()),
    ('big_page_path',pa.bool_()), ('future_path',pa.bool_()),
])
PARAMETER_SCHEMA = pa.schema([
    ('owner',pa.int32()), ('name',pa.string()), ('value',pa.string()), ('value_ci',pa.string()),
    ('number',pa.int64()), ('future',pa.bool_()),
])
SEGMENT_SCHEMA = pa.schema([('owner',pa.int32()),('segment',pa.string())])


def _parse(urls, cfg):
    """URL decoding is scalar; all rules share these parsed columns.

    Preserve urllib's percent decoding, Unicode and malformed-URL behavior.
    Parsing is bounded to at most 512 distinct URLs per call.
    """
    rows = {field.name:[] for field in FEATURE_SCHEMA}
    params = {field.name:[] for field in PARAMETER_SCHEMA}
    segments = {field.name:[] for field in SEGMENT_SCHEMA}
    approved = frozenset(name.lower() for name in cfg.normalize_params)
    selected = cfg.drop_rules if cfg.drop else frozenset()
    for owner, url in enumerate(urls.to_pylist()):
        new_url, shadow_url, path, query = url, None, '', ''
        valid = True
        try:
            parts = urlsplit(url) if url else urlsplit('')
        except ValueError:
            valid = False
        if valid and url:
            kept, shadow_kept = [], []
            for piece in parts.query.split('&') if parts.query else []:
                key, _, raw_value = piece.partition('=')
                name, value = unquote_plus(key).lower(), unquote_plus(raw_value)
                removed = cfg.normalize and name in approved
                if not removed:
                    kept.append(piece)
                    if piece and selected:
                        number = rules.parse_integer(value) if name in rules.PAGE_PARAMS or name in rules.OFFSET_PARAMS else None
                        date = rules._DATE_PARAM.match(value) if 'B_future_date' in selected and name in rules.FUTURE_DATE_PARAMS else None
                        params['owner'].append(owner)
                        params['name'].append(name)
                        params['value'].append(value)
                        params['value_ci'].append(value.translate(rules.CASE_INSENSITIVE_TRANSLATION))
                        params['number'].append(number)
                        params['future'].append(bool(date and int(date.group(1)) > rules.YEAR_MAX))
                if cfg.shadow_normalize:
                    drop = (rules.is_tracking_parameter(name) or rules.is_named_session_parameter(name)
                            or name in normalize.GENERIC_SESSION_PARAMS and bool(normalize._SESSION_TOKEN.match(value)))
                    if not drop:
                        shadow_kept.append(piece)
            path = parts.path
            if cfg.normalize and approved & normalize.PATH_SESSION_PARAMS:
                path = normalize._PATH_SESSION.sub(
                    lambda match:'' if match.group(1).lower() in approved else match.group(0), path)
            query = '&'.join(kept)
            if path != parts.path or query != parts.query:
                new_url = urlunsplit((parts.scheme,parts.netloc,path,query,parts.fragment))
            if cfg.shadow_normalize:
                full_path = normalize._PATH_SESSION.sub('',parts.path)
                full_query = '&'.join(shadow_kept)
                full = (url if full_path==parts.path and full_query==parts.query else
                        urlunsplit((parts.scheme,parts.netloc,full_path,full_query,parts.fragment)))
                if full != new_url:
                    shadow_url = full
        parts_list = [segment for segment in (path or '/').split('/') if segment] if valid else []
        if 'A_path_repeat' in selected:
            for segment in parts_list:
                if not (segment.isdigit() and len(segment)<=4):
                    segments['owner'].append(owner)
                    segments['segment'].append(segment)
        rows['url'].append(url)
        rows['normalized_url'].append(new_url)
        rows['shadow_url'].append(shadow_url)
        rows['path'].append(path.translate(rules.CASE_INSENSITIVE_TRANSLATION))
        rows['query'].append(query.translate(rules.CASE_INSENSITIVE_TRANSLATION))
        rows['valid'].append(valid)
        rows['segments'].append(len(parts_list))
        rows['long_url'].append(bool(url and 'B_long_url' in selected and len(unquote(new_url))>rules.MAX_URL_LEN))
        rows['big_page_path'].append(bool('B_big_page' in selected and any(
            int(value)>rules.MAX_PAGE for value in rules._PAGE_PATH.findall(path) if len(value)<12)))
        rows['future_path'].append(bool('B_future_date' in selected and any(
            int(value[0])>rules.YEAR_MAX for value in rules._DATE_PATH.findall(path))))
    return (pa.Table.from_pydict(rows,schema=FEATURE_SCHEMA),
            pa.Table.from_pydict(params,schema=PARAMETER_SCHEMA),
            pa.Table.from_pydict(segments,schema=SEGMENT_SCHEMA))


def _in(values, options):
    return pc.is_in(values,value_set=pa.array(sorted(options),type=pa.string()))


def _any_owner(row_ids, parents, mask):
    return pc.is_in(row_ids,value_set=pc.unique(pc.filter(parents,pc.fill_null(mask,False))))


def _at_least(row_ids, table, column, minimum, *, distinct=False):
    if distinct:
        table = table.group_by(['owner',column],use_threads=False).aggregate([])
    counts = table.group_by('owner',use_threads=False).aggregate([(column,'count')])
    return _any_owner(row_ids,counts['owner'],pc.greater_equal(counts[column+'_count'],minimum))


def _masks(rows, params, segments, selected):
    result = {}
    ids = pa.array(range(rows.num_rows),type=pa.int32())
    name, value, parent = params['name'],params['value'],params['owner']
    tracking = pc.or_(_in(name,rules.TRACKING_PARAMS),pc.starts_with(name,pattern='utm_'))
    emit = lambda mask:_any_owner(ids,parent,mask)
    if 'A_path_repeat' in selected:
        repeated = segments.group_by(['owner','segment'],use_threads=False).aggregate([('segment','count')])
        result['A_path_repeat'] = _any_owner(ids,repeated['owner'],pc.greater_equal(repeated['segment_count'],rules.PATH_REPEAT_MAX))
    if 'A_too_many_segments' in selected:
        result['A_too_many_segments'] = pc.greater(rows['segments'],rules.MAX_PATH_SEGMENTS)
    if 'A_session_id' in selected:
        token = pc.or_(pc.match_substring_regex(value,pattern=rules.COLUMNAR_SESSION_HEX_PATTERN,ignore_case=True),
                       pc.and_(pc.match_substring_regex(value,pattern=rules.COLUMNAR_SESSION_ALNUM_PATTERN,ignore_case=True),
                               pc.and_(pc.match_substring_regex(value,pattern='[0-9]'),pc.match_substring_regex(value,pattern='[a-z]',ignore_case=True))))
        result['A_session_id'] = pc.or_(emit(pc.or_(pc.or_(_in(name,rules.SESSION_PARAMS_STRICT),pc.starts_with(name,pattern='aspsessionid')),
                                                        pc.and_(_in(name,rules.SESSION_PARAMS_TOKEN),token))),
                                       pc.match_substring_regex(rows['path'],pattern=rules._PATH_SESSION.pattern,ignore_case=True))
    for rule, names, maximum in [('B_big_page',rules.PAGE_PARAMS,rules.MAX_PAGE),('B_big_offset',rules.OFFSET_PARAMS,rules.MAX_OFFSET)]:
        if rule in selected:
            mask = emit(pc.and_(_in(name,names),pc.greater(params['number'],maximum)))
            result[rule] = pc.or_(mask,rows['big_page_path']) if rule=='B_big_page' else mask
    if 'B_calendar' in selected:
        result['B_calendar'] = pc.or_(pc.match_substring_regex(rows['path'],pattern=rules._CAL_PLUGIN_PATH.pattern,ignore_case=True),
                                     emit(_in(name,rules._CAL_PLUGIN_PARAMS)))
    if 'B_future_date' in selected:
        result['B_future_date'] = pc.or_(rows['future_path'],emit(params['future']))
    if 'B_nested_redirect' in selected:
        nested = pc.match_substring_regex(params['value_ci'],pattern=rules._NESTED_URL.pattern,ignore_case=True)
        question = pc.match_substring_regex(value,pattern=r'\?|%3f',ignore_case=True)
        result['B_nested_redirect'] = pc.or_(emit(pc.and_(nested,question)),pc.greater_equal(
            pc.count_substring_regex(rows['query'],pattern=rules._NESTED_URL.pattern,ignore_case=True),2))
    if 'B_faceted' in selected:
        facets = pc.or_(_in(name,rules.FACET_PARAMS),pc.or_(pc.starts_with(name,pattern='filter['),
                        pc.or_(pc.starts_with(name,pattern='f['),pc.starts_with(name,pattern='facet'))))
        result['B_faceted'] = _at_least(ids,params.filter(facets),'name',rules.MIN_FACET_PARAMS,distinct=True)
    if 'B_tracking_params' in selected:
        result['B_tracking_params'] = emit(tracking)
    if 'B_many_params' in selected:
        result['B_many_params'] = _at_least(ids,params.filter(pc.invert(tracking)),'name',rules.MAX_PARAMS)
    if 'B_long_url' in selected:
        result['B_long_url'] = rows['long_url']
    if 'B_random_token' in selected:
        token = pc.or_(pc.match_substring_regex(value,pattern=rules.COLUMNAR_HEX_TOKEN_PATTERN,ignore_case=True),
                       pc.match_substring_regex(value,pattern=rules.COLUMNAR_BASE64_TOKEN_PATTERN))
        result['B_random_token'] = emit(pc.and_(pc.invert(tracking),token))
    result = {name:pc.and_(mask,rows['valid']) for name,mask in result.items()}
    if 'unparseable' in selected:
        result['unparseable'] = pc.invert(rows['valid'])
    return result


def evaluate(urls, cfg):
    """Return normalized URL, shadow URL and rule columns in input order."""
    names = rules.selected_rules(cfg)
    selected = frozenset(names)
    batches = []
    for start in range(0,len(urls),512):
        part = urls.slice(start,512)
        if not cfg.enabled:
            rows = pa.table({'url':part,'normalized_url':part,'shadow_url':pa.nulls(len(part),type=pa.string())})
            masks = {}
        else:
            rows, params, segments = _parse(part,cfg)
            masks = _masks(rows,params,segments,selected)
        output = rows.select(['url','normalized_url','shadow_url'])
        dropped = pa.repeat(False,len(part))
        for name in names:
            mask = pc.fill_null(masks[name],False)
            output = output.append_column(name,mask)
            dropped = pc.or_(dropped,mask)
        output = output.append_column('dropped',dropped)
        batches.append(output)
    schema = pa.schema([('url',pa.string()),('normalized_url',pa.string()),('shadow_url',pa.string()),
                        *[(name,pa.bool_()) for name in names],('dropped',pa.bool_())])
    return pa.concat_tables(batches) if batches else pa.Table.from_batches([],schema=schema)
