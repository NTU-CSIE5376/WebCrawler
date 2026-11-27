"""The pinned w3lib reproduces the stored url_state keys of tests/fixtures/url_keys.json (#166)."""
import json
from pathlib import Path

import pytest
from w3lib.url import canonicalize_url

FIXTURE = Path(__file__).resolve().parents[3] / "tests" / "fixtures" / "url_keys.json"
VECTORS = json.loads(FIXTURE.read_text(encoding="utf-8"))["vectors"]


@pytest.mark.parametrize("vector", VECTORS, ids=[v["url"] for v in VECTORS])
def test_canonicalize_url_gives_the_stored_key(vector):
    assert canonicalize_url(vector["url"]) == vector["key"]
