"""Shared validation and canonicalization for crawl URL submissions."""
from ipaddress import ip_address
from urllib.parse import urlsplit

import validators
from w3lib.url import canonicalize_url


MAX_URL_LEN = 2500


def validate_url(url: object, *, max_length: int = MAX_URL_LEN) -> str:
    """Return a canonical absolute HTTP(S) URL or raise ValueError.

    This is string validation only: no DNS lookup or HTTP request is made.
    IP literals must be global; localhost and single-label hosts are rejected.
    The length limit applies before and after encoding/canonicalization.
    """
    try:
        if not isinstance(url, str) or not url or len(url) > max_length:
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
        parsed = urlsplit(url)
        if parsed.username is not None:
            raise ValueError()
        host = (parsed.hostname or "").rstrip(".")
        try:
            address = ip_address(host)
        except ValueError:
            if "." not in host or host == "localhost" or host.endswith(".localhost"):
                raise ValueError()
        else:
            if not address.is_global:
                raise ValueError()
        canonical = canonicalize_url(url)
        if len(canonical) > max_length:
            raise ValueError()
    except (ValueError, UnicodeError):
        raise ValueError("Invalid HTTP(S) URL") from None
    return canonical
