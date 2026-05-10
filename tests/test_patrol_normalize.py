from __future__ import annotations

import unittest

from libs.patrol.normalize import normalize_parent_url


class NormalizeParentUrlTest(unittest.TestCase):
    def test_force_https(self):
        self.assertEqual(
            normalize_parent_url("http://example.com/foo"),
            "https://example.com/foo",
        )

    def test_strip_www_prefix(self):
        self.assertEqual(
            normalize_parent_url("https://www.example.com/foo"),
            "https://example.com/foo",
        )

    def test_lowercase_host(self):
        self.assertEqual(
            normalize_parent_url("https://Example.COM/foo"),
            "https://example.com/foo",
        )

    def test_strip_default_https_port(self):
        self.assertEqual(
            normalize_parent_url("https://example.com:443/foo"),
            "https://example.com/foo",
        )

    def test_strip_default_http_port_after_scheme_upgrade(self):
        # http:80 → after scheme upgrade to https, the literal :80 is no
        # longer the default port, so it survives. This is the conservative
        # behavior — we only strip the default for the *resulting* scheme.
        self.assertEqual(
            normalize_parent_url("http://example.com:80/foo"),
            "https://example.com:80/foo",
        )

    def test_keep_nondefault_port(self):
        self.assertEqual(
            normalize_parent_url("https://example.com:8080/foo"),
            "https://example.com:8080/foo",
        )

    def test_strip_trailing_slash(self):
        self.assertEqual(
            normalize_parent_url("https://example.com/foo/"),
            "https://example.com/foo",
        )

    def test_keep_root_slash(self):
        self.assertEqual(
            normalize_parent_url("https://example.com/"),
            "https://example.com/",
        )

    def test_empty_path_becomes_root(self):
        self.assertEqual(
            normalize_parent_url("https://example.com"),
            "https://example.com/",
        )

    def test_strip_fragment(self):
        self.assertEqual(
            normalize_parent_url("https://example.com/foo#section"),
            "https://example.com/foo",
        )

    def test_drop_utm_params(self):
        self.assertEqual(
            normalize_parent_url(
                "https://example.com/foo?utm_source=fb&utm_medium=cpc"
            ),
            "https://example.com/foo",
        )

    def test_drop_fbclid_gclid(self):
        self.assertEqual(
            normalize_parent_url(
                "https://example.com/foo?fbclid=ABC&gclid=DEF"
            ),
            "https://example.com/foo",
        )

    def test_keep_content_query_params(self):
        # `id` and `page` look generic but can be real content keys; we
        # never drop them.
        self.assertEqual(
            normalize_parent_url("https://example.com/article?id=123&page=2"),
            "https://example.com/article?id=123&page=2",
        )

    def test_mixed_query_drops_tracking_keeps_content(self):
        self.assertEqual(
            normalize_parent_url(
                "https://example.com/article?id=123&utm_source=fb&page=2"
            ),
            "https://example.com/article?id=123&page=2",
        )

    def test_three_redirect_variants_share_one_key(self):
        variants = [
            "http://example.com/scoreboard",
            "https://www.example.com/scoreboard",
            "https://example.com/scoreboard/",
            "https://example.com/scoreboard?utm_source=fb",
        ]
        keys = {normalize_parent_url(u) for u in variants}
        self.assertEqual(
            keys,
            {"https://example.com/scoreboard"},
            f"variants did not collapse to one key: {keys}",
        )

    def test_empty_input_returns_empty(self):
        self.assertEqual(normalize_parent_url(""), "")

    def test_no_host_passes_through(self):
        # Relative URLs and pathological inputs should not be silently
        # rewritten into something fetchable; callers filter these out.
        self.assertEqual(normalize_parent_url("/path/only"), "/path/only")

    def test_strip_whitespace_around_url(self):
        self.assertEqual(
            normalize_parent_url("  https://example.com/foo  "),
            "https://example.com/foo",
        )


if __name__ == "__main__":
    unittest.main()
