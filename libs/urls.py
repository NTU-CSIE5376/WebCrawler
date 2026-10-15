"""Shared validation and canonicalization for crawl URL submissions."""
from urllib.parse import urlsplit

import validators
from w3lib.url import canonicalize_url


MAX_URL_LEN = 2500


def validate_url(url: object) -> str:
    """Return a canonical absolute HTTP(S) URL or raise ValueError.

    This is string validation only: no DNS lookup or HTTP request is made.
    The length limit applies before and after encoding/canonicalization.
    """
    try:
        if not isinstance(url, str) or not url or len(url) > MAX_URL_LEN:
            raise ValueError()
        # The library owns URL syntax, hostname, port, and IP validation.
        if validators.url(
            url, simple_host=True, strict_query=False, rfc_1034=True,
            validate_scheme=lambda scheme: scheme in {"http", "https"},
        ) is not True:
            raise ValueError()
        # Submission policy: forbid credentials and raw controls even where a
        # permissive URL parser would strip or accept them.
        if any(ord(char) < 32 or ord(char) == 127 for char in url):
            raise ValueError()
        if urlsplit(url).username is not None:
            raise ValueError()
        canonical = canonicalize_url(url)
        if len(canonical) > MAX_URL_LEN:
            raise ValueError()
    except (ValueError, UnicodeError):
        raise ValueError("Invalid HTTP(S) URL") from None
    return canonical
