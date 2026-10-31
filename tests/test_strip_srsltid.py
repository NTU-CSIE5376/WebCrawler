from __future__ import annotations

import unittest

from libs.urls import strip_srsltid

TOKEN = "AfmBOoqfCkB7Gr36L8FzV4ncwpCatQZRYNOjavRk_Qp1ZbjcmArzMmZw"


class StripSrsltidTest(unittest.TestCase):
    def test_only_param_drops_question_mark(self):
        self.assertEqual(
            strip_srsltid(f"https://ew.com/story-12174222?srsltid={TOKEN}"),
            "https://ew.com/story-12174222",
        )

    def test_last_param_keeps_others_byte_for_byte(self):
        self.assertEqual(
            strip_srsltid(f"https://a.com/s?term=b27%20supplement&sort=relevance&srsltid={TOKEN}"),
            "https://a.com/s?term=b27%20supplement&sort=relevance",
        )

    def test_middle_param(self):
        self.assertEqual(
            strip_srsltid(f"https://a.com/s?b=2&srsltid={TOKEN}&a=1"),
            "https://a.com/s?b=2&a=1",
        )

    def test_key_case_insensitive(self):
        self.assertEqual(strip_srsltid("https://a.com/?SrsLtId=x"), "https://a.com/")

    def test_fragment_kept(self):
        self.assertEqual(
            strip_srsltid("https://a.com/p?srsltid=x#reviews"),
            "https://a.com/p#reviews",
        )

    def test_unchanged_when_absent(self):
        for url in (
            "https://a.com/p?id=1",
            "https://a.com/srsltid/page",
            "https://a.com/p?xsrsltid=1&srsltidx=2",
            "https://a.com/p?q=srsltid",
            "/goto?url=relative",
        ):
            self.assertEqual(strip_srsltid(url), url)

    def test_idempotent(self):
        once = strip_srsltid(f"https://a.com/p?id=1&srsltid={TOKEN}")
        self.assertEqual(strip_srsltid(once), once)


if __name__ == "__main__":
    unittest.main()
