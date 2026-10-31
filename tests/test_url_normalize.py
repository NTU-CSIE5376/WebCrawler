import unittest

from libs.frontier.url_normalize import strip_named_params, strip_params


class StripNamedParamsTest(unittest.TestCase):
    NAMES = frozenset({"srsltid", "jsessionid", "phpsessid"})

    def test_only_named_params_removed(self):
        self.assertEqual(
            strip_named_params("https://a.com/p?gclid=1&id=5&SRSLTID=x&utm_source=y", self.NAMES),
            "https://a.com/p?gclid=1&id=5&utm_source=y",
        )
        self.assertEqual(
            strip_named_params("https://a.com/f?PHPSESSID=abc&t=1&oscsid=z", self.NAMES),
            "https://a.com/f?t=1&oscsid=z",
        )

    def test_path_session_only_when_named(self):
        url = "https://a.com/x.do;jsessionid=36ED?flag=3"
        self.assertEqual(strip_named_params(url, self.NAMES), "https://a.com/x.do?flag=3")
        self.assertEqual(strip_named_params(url, frozenset({"srsltid"})), url)
        self.assertEqual(
            strip_named_params("https://a.com/x;phpsessid=1;jsessionid=2", frozenset({"jsessionid"})),
            "https://a.com/x;phpsessid=1",
        )

    def test_unchanged_returns_same_string(self):
        url = "https://a.com/p?q=srsltid&xsrsltid=1"
        self.assertIs(strip_named_params(url, self.NAMES), url)


class StripParamsTest(unittest.TestCase):
    def test_tracking_params_are_removed_rest_kept_in_order(self):
        self.assertEqual(
            strip_params("https://a.com/p?gclid=abc&id=5&utm_source=x&b=2"),
            "https://a.com/p?id=5&b=2",
        )
        self.assertEqual(strip_params("https://a.com/p?srsltid=AfmB"), "https://a.com/p")

    def test_named_session_in_path_and_query(self):
        self.assertEqual(
            strip_params("https://a.com/x.do;jsessionid=36ED17384C1E?flag=3&id=4"),
            "https://a.com/x.do?flag=3&id=4",
        )
        self.assertEqual(
            strip_params("https://a.com/f?PHPSESSID=abc123&t=1"), "https://a.com/f?t=1"
        )

    def test_generic_session_names_kept_by_default(self):
        # Golden URL where `s` is a search id, not a session.
        url = ("https://www.saskatchewan.ca/government/municipal-administration/"
               "municipal-directory?s=0e1332395ff946feb30e1799aa560841")
        self.assertEqual(strip_params(url), url)

    def test_generic_session_tokens_opt_in_only_for_32_hex(self):
        phpbb = "http://f.com/memberlist.php?mode=leaders&sid=81bf41edc0d03848ae79fdb6a6941cc8"
        self.assertEqual(
            strip_params(phpbb, generic_session_tokens=True),
            "http://f.com/memberlist.php?mode=leaders",
        )
        for content_id in ("https://cpbl.com.tw/xmdoc/cont?sid=0P182512677623119312",
                           "https://parks.gov.taipei/p.php?sid=154"):
            self.assertEqual(strip_params(content_id, generic_session_tokens=True), content_id)

    def test_original_encoding_preserved(self):
        url = "https://a.com/search?q=a%20b+c&sort=new"
        self.assertEqual(strip_params(url), url)
        self.assertEqual(
            strip_params("https://a.com/search?q=a%20b&utm_medium=x"),
            "https://a.com/search?q=a%20b",
        )

    def test_classes_can_be_disabled(self):
        url = "https://a.com/p?utm_source=x;jsessionid=1"
        self.assertEqual(strip_params(url, tracking=False, named_sessions=False), url)


if __name__ == "__main__":
    unittest.main()
