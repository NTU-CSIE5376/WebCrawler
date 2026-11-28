import random
import os
from pathlib import Path
import subprocess
import sys
from urllib.parse import quote

import pyarrow as pa
import pytest

from libs.frontier import URLPolicy, evaluate
from libs.frontier.trap_rules import ALL_RULES, match_rules
from libs.frontier.url_normalize import strip_named_params, strip_params


def corpus():
    urls = ['',None,'https://[broken','https://example.test/a/a/a/',
            'https://example.test/2022/02/02/','https://example.test/²/²/²/',
            'https://example.test/events/day/2028-01-01',
            'https://example.test/calendar/2028-01','https://example.test/calendar/2028-01?ical=1',
            'https://example.test/page/1234567890123/page/1001',
            'https://example.test/2030/01/01','https://example.test/'+'x/'*21,
            'https://example.test/search?q=a%20b+c&SRSLTID=x&&x=1',
            'https://example.test/a;jsessionid=A;phpsessid=B?PHPSESSID=C',
            'https://example.test/'+quote('中文'*100)]
    rng = random.Random(147)
    paths = ['/a','/a/a/a','/events/day/2030-01-01','/calendario/a/2028-01','/page/1001','/ſeite/1002','/a;jsessionid=abc']
    keys = ['srsltid','utm_source','filter[x]','sort','order','type','page','offset','ical','date','sid','x','', '%70age']
    values = ['', '1001','0001001','10001','2030-01-01','https://other.test/?q=1',
              'https%3A%2F%2Fother.test%2F%3Fx=1', 'https://a.test/https://b.test/',
              'a'*32,'a1'*20,'a'*32+'\n','ſ'*40,'²','１２３４','a%20b+c','%FF']
    for _ in range(1400):
        query = '&'.join(rng.choice(keys)+'='+quote(rng.choice(values),safe='/%') for _ in range(rng.randrange(9)))
        urls.append('https://example.test'+rng.choice(paths)+('?' +query if query else ''))
    return urls


@pytest.mark.parametrize('changes', [
    {}, {'drop_rules':frozenset((*ALL_RULES,'unparseable'))}, {'drop':False},
    {'normalize':False}, {'shadow_normalize':False}, {'enabled':False},
    {'normalize_params':frozenset({'SRSLTID','jsessionid','phpsessid'})},
])
def test_columnar_policy_matches_reference_normalization_and_every_rule(changes):
    cfg = URLPolicy(**changes)
    urls = corpus()
    actual = evaluate(pa.array(urls,type=pa.string()),cfg,backend='pyarrow').to_pylist()
    assert actual == evaluate(iter(urls),cfg,backend='python')
    for url, result in zip(urls,actual):
        normalized, shadow, hits = url,None,[]
        if cfg.enabled and url:
            normalized = strip_named_params(url,cfg.normalize_params) if cfg.normalize else url
            full = strip_params(url,generic_session_tokens=True) if cfg.shadow_normalize else normalized
            shadow = full if full!=normalized else None
            hits = [name for name in match_rules(normalized) if name in cfg.drop_rules] if cfg.drop else []
        assert result['normalized_url']==normalized, url
        assert result['shadow_url']==shadow, url
        assert [name for name in (*ALL_RULES,'unparseable') if result.get(name,False)]==hits, url
        assert result['dropped']==bool(hits), url


@pytest.mark.parametrize('urls', [[],[None,'','https://example.test/a?srsltid=x']])
def test_public_backends_have_identical_decision_fields_and_order(urls):
    expected=evaluate(iter(urls))
    chunks=[pa.array(urls[:1],type=pa.string()),pa.array(urls[1:],type=pa.string())]
    actual=evaluate(pa.chunked_array(chunks,type=pa.string()),backend='pyarrow')
    assert actual.to_pylist()==expected
    assert actual.schema.names==['url','normalized_url','shadow_url',
                                 'A_path_repeat','A_too_many_segments','B_calendar',
                                 'B_nested_redirect','B_faceted','dropped']


def test_python_backend_does_not_import_columnar_dependencies():
    code='''import importlib.abc,sys
class BlockColumnar(importlib.abc.MetaPathFinder):
    def find_spec(self, fullname, path=None, target=None):
        if fullname.split('.')[0] in {'pyarrow','numpy','pandas'}:
            raise ImportError('Python backend attempted a columnar import: '+fullname)
sys.meta_path.insert(0,BlockColumnar())
from libs.frontier import URLPolicy,evaluate
result=evaluate(['https://example.test/a?srsltid=x'],URLPolicy())
assert result[0]['normalized_url']=='https://example.test/a'
assert not any(name.split('.')[0] in {'pyarrow','numpy','pandas'} for name in sys.modules)
try:
    evaluate([],backend='pyarrow')
except ImportError as exc:
    assert 'Python backend attempted a columnar import' in str(exc)
else:
    raise AssertionError('PyArrow selection did not import its dependency')
'''
    repo=Path(__file__).resolve().parents[3]
    result=subprocess.run([sys.executable,'-c',code],cwd=repo,
                          env={**os.environ,'PYTHONPATH':str(repo)},capture_output=True,text=True)
    assert result.returncode==0,result.stderr




def test_default_backend_matches_explicit_python_selection():
    urls=['https://example.test/a?srsltid=x','https://example.test/x/x/x/']
    assert evaluate(urls)==evaluate(urls,backend='python')


@pytest.mark.parametrize('backend',['arrow','columnar','unknown',None])
def test_invalid_backend_is_rejected_before_consuming_input(backend):
    def unread():
        raise AssertionError('invalid backend consumed input')
        yield
    with pytest.raises(ValueError,match="backend must be 'python' or 'pyarrow'"):
        evaluate(unread(),backend=backend)


def test_existing_scalar_callers_need_no_backend_parameter():
    url='https://example.test/a?srsltid=x&utm_source=y&id=1'
    assert strip_named_params(url,frozenset({'srsltid'}))=='https://example.test/a?utm_source=y&id=1'
    assert strip_params(url)=='https://example.test/a?id=1'
    assert match_rules('https://example.test/x/x/x/')==['A_path_repeat']
