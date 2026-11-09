from __future__ import annotations

import sys
import unittest
from pathlib import Path
from unittest.mock import MagicMock, patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "scripts"))

# Ensure imports don't fail in environments without psycopg2/yaml
if "psycopg2" not in sys.modules:
    sys.modules["psycopg2"] = MagicMock()
if "yaml" not in sys.modules:
    sys.modules["yaml"] = MagicMock()

import golden_inject  # noqa: E402


class GoldenInjectCommitIntervalTest(unittest.TestCase):
    @patch("golden_inject.psycopg2.connect")
    @patch("golden_inject.fetch_injectable_batch_ids")
    @patch("golden_inject.fetch_urls_by_batches")
    @patch("golden_inject.load_sharding_config")
    @patch("golden_inject.ensure_domain")
    @patch("golden_inject.inject_url")
    def test_periodic_commits(
        self,
        mock_inject_url,
        mock_ensure_domain,
        mock_load_sharding_config,
        mock_fetch_urls,
        mock_fetch_batches,
        mock_connect,
    ):
        mock_metric_conn = MagicMock()
        mock_crawler_conn = MagicMock()
        mock_connect.side_effect = [mock_metric_conn, mock_crawler_conn]

        mock_fetch_batches.return_value = [1]
        mock_load_sharding_config.return_value = ({}, set())
        mock_ensure_domain.return_value = (1, 0.8)

        # 5 URLs, commit_interval = 2 -> commits after 2, 4, and final loop end
        mock_fetch_urls.return_value = [
            {"url": f"https://example.com/{i}", "query_id": i, "batch_id": 1, "gain": 0.9}
            for i in range(1, 6)
        ]
        mock_inject_url.return_value = golden_inject.INSERTED

        test_args = ["golden_inject.py", "--commit-interval", "2"]
        with patch.object(sys, "argv", test_args):
            golden_inject.main()

        # Should commit at count 2, count 4, and at loop end (total 3 commits)
        self.assertEqual(mock_crawler_conn.commit.call_count, 3)

    @patch("golden_inject.psycopg2.connect")
    @patch("golden_inject.fetch_injectable_batch_ids")
    @patch("golden_inject.fetch_urls_by_batches")
    @patch("golden_inject.load_sharding_config")
    @patch("golden_inject.inject_url")
    def test_dry_run_does_not_commit(
        self,
        mock_inject_url,
        mock_load_sharding_config,
        mock_fetch_urls,
        mock_fetch_batches,
        mock_connect,
    ):
        mock_metric_conn = MagicMock()
        mock_crawler_conn = MagicMock()
        mock_connect.side_effect = [mock_metric_conn, mock_crawler_conn]

        mock_fetch_batches.return_value = [1]
        mock_load_sharding_config.return_value = ({}, set())
        mock_fetch_urls.return_value = [
            {"url": "https://example.com/1", "query_id": 1, "batch_id": 1, "gain": 0.9},
            {"url": "https://example.com/2", "query_id": 2, "batch_id": 1, "gain": 0.9},
        ]
        mock_inject_url.return_value = golden_inject.INSERTED

        test_args = ["golden_inject.py", "--dry-run", "--commit-interval", "1"]
        with patch.object(sys, "argv", test_args):
            golden_inject.main()

        self.assertEqual(mock_crawler_conn.commit.call_count, 0)


if __name__ == "__main__":
    unittest.main()
