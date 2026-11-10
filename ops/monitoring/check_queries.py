"""Export the exact generated PromQL as recording rules for promtool validation.

No webhook is read and no HTTP requests are made. Used by monitoring CI.
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path

from render_alerting import PROMETHEUS_UID, build_provisioning


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    config = json.loads(args.config.read_text())
    doc = build_provisioning(config)["webcrawler-rules.json"]
    rules = [
        {
            "record": "monitoring_check_" + rule["uid"].replace("-", "_"),
            "expr": rule["data"][0]["model"]["expr"],
        }
        for group in doc["groups"]
        for rule in group["rules"]
        if rule["data"][0]["datasourceUid"] == PROMETHEUS_UID
    ]
    # JSON is also valid YAML, so promtool can consume this without a dependency.
    with args.output.open("x") as stream:
        json.dump(
            {"groups": [{"name": "monitoring-query-checks", "rules": rules}]}, stream
        )


if __name__ == "__main__":
    main()
