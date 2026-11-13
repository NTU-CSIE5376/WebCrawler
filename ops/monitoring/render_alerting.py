"""Render opt-in Grafana 11 provisioning; never call Grafana or Discord.

Default output has paused rules. To enable later, supply DISCORD_WEBHOOK_URL,
--enable, explicit --enable-rule UIDs and --routing-confirmed after adding the project=webcrawler child route
in Grafana. This tool deliberately never provisions the existing policy tree.
Generated contact-point files contain a secret: keep the output directory private.
"""

from __future__ import annotations

import argparse
import json
import os
import re
import sys
from pathlib import Path
from urllib.parse import quote, urlsplit

PROMETHEUS_UID = "crawler-prometheus"
LOKI_UID = "loki"
CONTACT_POINT = "webcrawler-discord"
INGEST_CONTACT_POINT = "webcrawler-ingestor-discord"
INGEST_ERROR_QUERY = '{service="ingestor",event="ingest.batch_error"}'
PIPELINE_ERROR_QUERY = '{event=~"route.db_error|route.domain_error|extract.record_error|stats.process_error"}'
GRAFANA_URL = "http://172.16.191.2:3000"


def explore_logs_url(expression: str, start="now-6h") -> str:
    panes = {
        "logs": {
            "datasource": LOKI_UID,
            "queries": [
                {
                    "refId": "A",
                    "expr": expression,
                    "queryType": "range",
                    "datasource": {"type": "loki", "uid": LOKI_UID},
                }
            ],
            "range": {"from": start, "to": "now"},
        }
    }
    return (
        GRAFANA_URL
        + "/explore?schemaVersion=1&panes="
        + quote(json.dumps(panes, separators=(",", ":")), safe="")
        + "&orgId=1"
    )


_PANES_FORMAT = (
    json.dumps(
        {
            "logs": {
                "datasource": LOKI_UID,
                "queries": [
                    {
                        "refId": "A",
                        "expr": "__QUERY__",
                        "queryType": "range",
                        "datasource": {"type": "loki", "uid": LOKI_UID},
                    }
                ],
                "range": {"from": "__FROM__", "to": "now"},
            }
        },
        separators=(",", ":"),
    )
    .replace('"__QUERY__"', "%q")
    .replace("__FROM__", "%d")
)
_DISCORD_LOG_LINK = (
    "{{ if and (not $linked) $alert.Annotations.logs_query }}"
    "{{ $panes := printf "
    + json.dumps(_PANES_FORMAT)
    + " $alert.Annotations.logs_query (($alert.StartsAt.Add -3600000000000).UnixMilli) }}"
    "[錯誤日誌（含 worker 與 DB 錯誤）]("
    + GRAFANA_URL
    + "/explore?schemaVersion=1&panes={{ $panes | urlquery }}&orgId=1)\n"
    "{{ $linked = true }}{{ end }}"
)
DISCORD_MESSAGE = (
    "{{ .Status }} | {{ len .Alerts.Firing }} firing / "
    "{{ len .Alerts.Resolved }} resolved\n"
    "{{ $linked := false }}{{ range $i, $alert := .Alerts }}{{ if lt $i 6 }}"
    "• {{ $alert.Annotations.summary }}"
    "{{ with $alert.Labels.host }} | {{ . }}{{ end }}"
    "{{ with $alert.Labels.mountpoint }} {{ . }}{{ end }}"
    "{{ with $alert.Labels.stage }} | {{ . }}{{ end }}"
    "{{ with $alert.Labels.worker }} (worker {{ . }}){{ end }}\n"
    + _DISCORD_LOG_LINK
    + "{{ end }}{{ end }}"
    '{{ if and $linked (eq .Status "resolved") }}近期未新增錯誤，不代表先前失敗資料已補回。\n{{ end }}'
    "最多列出 6 項；告警狀態：http://172.16.191.2:3000/alerting/list"
)
INGEST_DISCORD_MESSAGE = (
    "🚨 Ingestor 批次入庫失敗（近一小時）\n"
    + DISCORD_MESSAGE
    + "\n沒有新錯誤不代表先前失敗批次已補回。\n"
    "[Ingestor 告警規則](" + GRAFANA_URL + "/alerting/grafana/wc-ingest-error/view)"
)


def validate_webhook(value: str) -> str:
    """Reject mistakes without including the secret in an exception or log."""
    try:
        parsed = urlsplit(value)
        valid = (
            parsed.scheme == "https"
            and parsed.hostname in {"discord.com", "discordapp.com"}
            and parsed.port in {None, 443}
            and parsed.username is None
            and parsed.password is None
            and not parsed.query
            and not parsed.fragment
            and re.fullmatch(r"/api/webhooks/[0-9]+/[A-Za-z0-9_.-]{20,}", parsed.path)
        )
    except ValueError:
        valid = False
    if not valid:
        raise ValueError("DISCORD_WEBHOOK_URL is not a supported Discord webhook URL")
    return value


def _query(expression: str, datasource: str) -> dict:
    kind = "prometheus" if datasource == PROMETHEUS_UID else "loki"
    return {
        "refId": "A",
        "queryType": "instant" if kind == "loki" else "",
        "relativeTimeRange": {"from": 3600, "to": 0},
        "datasourceUid": datasource,
        "model": {
            "datasource": {"type": kind, "uid": datasource},
            "expr": expression,
            "instant": True,
            "range": False,
            "queryType": "instant",
            "refId": "A",
            "intervalMs": 1000,
            "maxDataPoints": 43200,
        },
    }


def _rule(
    uid: str,
    title: str,
    expression: str,
    *,
    datasource: str = PROMETHEUS_UID,
    threshold: float = 0,
    pending: str = "0s",
    severity: str = "warning",
    service: str = "monitoring",
    description: str = "",
    paused: bool = True,
    labels: dict | None = None,
    error_state: str = "KeepLast",
) -> dict:
    return {
        "uid": f"wc-{uid}",
        "title": title,
        "condition": "C",
        "data": [
            _query(expression, datasource),
            {
                "refId": "B",
                "datasourceUid": "__expr__",
                "relativeTimeRange": {"from": 0, "to": 0},
                "model": {
                    "refId": "B",
                    "type": "reduce",
                    "expression": "A",
                    "reducer": "last",
                    "settings": {},
                    "datasource": {"type": "__expr__", "uid": "__expr__"},
                },
            },
            {
                "refId": "C",
                "datasourceUid": "__expr__",
                "relativeTimeRange": {"from": 0, "to": 0},
                "model": {
                    "refId": "C",
                    "type": "threshold",
                    "expression": "B",
                    "datasource": {"type": "__expr__", "uid": "__expr__"},
                    "conditions": [
                        {
                            "evaluator": {"type": "gt", "params": [threshold]},
                            "operator": {"type": "and"},
                            "query": {"params": ["C"]},
                            "reducer": {"type": "last", "params": []},
                            "type": "query",
                        }
                    ],
                },
            },
        ],
        # Missing telemetry remains a blind spot. Query errors are reported by
        # dedicated monitoring rules instead of inventing business failures.
        "noDataState": "Alerting",
        "execErrState": error_state,
        "for": pending,
        "isPaused": paused,
        "labels": {
            "project": "webcrawler",
            "severity": severity,
            "service": service,
            **(labels or {}),
        },
        "annotations": {
            "summary": title,
            "description": description,
            "runbook": "ops/monitoring/README.md",
        },
    }


def build_provisioning(
    config: dict,
    *,
    enabled: bool = False,
    webhook: str | None = None,
    routing_confirmed: bool = False,
    enabled_rule_uids: list[str] | None = None,
) -> dict[str, dict]:
    """Return independent provisioning documents; no API/notification side effects."""
    if not isinstance(config, dict):
        raise TypeError("config must be an object")
    if webhook:
        validate_webhook(webhook)
    if enabled and (not webhook or not routing_confirmed):
        raise ValueError(
            "Enabling requires DISCORD_WEBHOOK_URL and --routing-confirmed"
        )
    if enabled and (
        not isinstance(enabled_rule_uids, list)
        or not enabled_rule_uids
        or not all(isinstance(uid, str) for uid in enabled_rule_uids)
    ):
        raise ValueError("Enabling requires an explicit --enable-rule allowlist")
    if not enabled and enabled_rule_uids:
        raise ValueError("--enable-rule also requires --enable")
    org_id = config.get("org_id", 1)
    if isinstance(org_id, bool) or not isinstance(org_id, int) or org_id < 1:
        raise ValueError("org_id must be a positive integer")
    workers = config.get("expected_crawlers", [])
    if not isinstance(workers, list) or not workers:
        raise ValueError(
            "expected_crawlers must be a nonempty explicit worker allowlist"
        )
    normalized = []
    for worker in workers:
        if not isinstance(worker, str) or not re.fullmatch(r"[0-9]{1,2}", worker):
            raise ValueError(
                "expected_crawlers entries must be one or two digit strings"
            )
        normalized.append(str(int(worker)))
    if len(set(normalized)) != len(normalized):
        raise ValueError("expected_crawlers contains duplicate worker IDs")

    rules = []

    def add(uid: str, title: str, expression: str, **kwargs) -> None:
        rules.append(_rule(uid, title, expression, paused=True, **kwargs))

    for uid, title, datasource in (
        ("loki-query-health", "Loki 監控查詢失敗", LOKI_UID),
        (
            "prometheus-query-health",
            "Prometheus 監控查詢失敗",
            PROMETHEUS_UID,
        ),
    ):
        add(
            uid,
            title,
            "vector(0)",
            datasource=datasource,
            severity="critical",
            service="monitoring",
            error_state="Alerting",
            description="監控資料來源的基本查詢失敗，部分業務狀態可能無法更新；這不代表 crawler 或其他業務程序已停止。先確認監控服務與查詢錯誤。",
        )

    add(
        "target-down",
        "監控目標無法連線",
        'up{job=~"crawler-node|crawler-pipeline|crawler-probe"} == bool 0',
        pending="2m",
        severity="critical",
        description="Prometheus 連續兩分鐘無法抓取此目標。確認主機、網路、exporter；不要自動重啟 crawler。",
    )
    add(
        "probe-failed",
        "服務探測失敗",
        'probe_success{job="crawler-probe"} == bool 0',
        pending="2m",
        severity="critical",
        service="probe",
        description="指定的 TCP／HTTP 探測連續失敗；TCP 成功只代表連線可建立，API healthz 也只代表存活。",
    )
    filesystem = '{job="crawler-node",mountpoint=~"/|/data",fstype!~"tmpfs|overlay|rootfs|squashfs|devtmpfs"}'
    disk_used = f"100 * (1 - node_filesystem_avail_bytes{filesystem} / node_filesystem_size_bytes{filesystem})"
    add(
        "disk-warning",
        "磁碟可用容量不足 15%",
        disk_used,
        threshold=85,
        pending="10m",
        service="host",
        description="先確認是哪個掛載點成長。不要自行刪 IPC、DB 或結果；85% 是初始值，需依容量與成長速度調整。",
    )
    add(
        "disk-critical",
        "磁碟可用容量不足 8%",
        disk_used,
        threshold=92,
        pending="5m",
        severity="critical",
        service="host",
        description="可能讓 DB 或 IPC 無法寫入；確認可用容量與成長速度，安排擴容或明確核准的清理。",
    )
    available = f"node_filesystem_avail_bytes{filesystem}"
    size = f"node_filesystem_size_bytes{filesystem}"
    forecast = (
        f"(((predict_linear({available}[6h], 86400) < bool 0) "
        f"* ({available} / {size} < bool 0.30)) "
        f"and present_over_time({available}[5m] offset 2h)) or vector(0)"
    )
    # Empty while the two-hour history gate warms up is intentionally zero.
    # Exporter availability is checked independently, not hidden here.
    add(
        "disk-forecast",
        "磁碟依近期成長速度可能在一天內耗盡",
        forecast,
        pending="15m",
        service="host",
        description="以最多六小時容量趨勢推估，至少要有兩小時前的樣本且剩餘小於 30%。線性推估可能受清理或突發流量影響，請核實成長來源與容量。",
    )
    inode_used = f"100 * (1 - node_filesystem_files_free{filesystem} / node_filesystem_files{filesystem})"
    add(
        "inode-critical",
        "磁碟 inode 可用數不足 5%",
        inode_used,
        threshold=95,
        pending="5m",
        severity="critical",
        service="host",
        description="即使還有磁碟空間，也可能因檔案數耗盡而無法寫檔；確認小檔案來源。",
    )
    add(
        "memory-warning",
        "主機可用記憶體不足 5%",
        '100 * (1 - node_memory_MemAvailable_bytes{job="crawler-node"} / node_memory_MemTotal_bytes{job="crawler-node"})',
        threshold=95,
        pending="15m",
        service="host",
        description="使用 MemAvailable，不因歷史 swap 用量高而告警。確認記憶體壓力、OOM 與工作量。",
    )
    add(
        "host-oom",
        "主機發生 OOM Kill",
        'increase(node_vmstat_oom_kill{job="crawler-node"}[5m]) or vector(0)',
        severity="critical",
        service="host",
        description="核心近五分鐘曾因記憶體不足殺掉程序。確認被殺程序、資料完整性與工作量；告警解除不代表工作已補回，不要自動重啟。",
    )
    add(
        "pipeline-scan",
        "管線監控無法讀取完整狀態",
        'crawler_pipeline_scan_success{job="crawler-pipeline"} == bool 0',
        pending="2m",
        service="pipeline",
        description="掃描失敗、狀態缺漏或掃描預算耗盡。此時無法判斷積壓，不能把缺資料當成沒有工作。",
    )
    add(
        "pipeline-stale",
        "管線監控結果已超過三分鐘未更新",
        'time() - crawler_pipeline_collector_last_run_timestamp_seconds{job="crawler-pipeline"}',
        threshold=180,
        service="pipeline",
        description="HTTP 還能回應不代表背景採集仍在運作；確認採集器與檔案系統。",
    )
    add(
        "pipeline-lag",
        "可處理的管線資料已等待超過一小時",
        'crawler_pipeline_oldest_ready_lag_seconds{job="crawler-pipeline"}',
        threshold=3600,
        pending="10m",
        service="pipeline",
        description="已扣除 FolderReader 預設兩個 interval 的刻意等待。確認該 worker 的 DB、錯誤日誌與輸入量；進度前進不代表資料沒有遺失。",
    )
    add(
        "ingest-error",
        "Ingestor 有批次入庫失敗，可能遺失資料",
        'sum(count_over_time({service="ingestor",event="ingest.batch_error"}[1h])) or vector(0)',
        datasource=LOKI_UID,
        severity="critical",
        service="ingestor",
        description="近一小時出現 ingest.batch_error；首次仍立即告警。失敗可能被捕捉後繼續推進 progress。沒有新錯誤不代表資料已補回，需核對失敗批次。",
    )
    rules[-1]["annotations"].update(
        logs_query=INGEST_ERROR_QUERY, logs_url=explore_logs_url(INGEST_ERROR_QUERY)
    )
    add(
        "pipeline-errors",
        "管線出現處理錯誤",
        'sum by (service) (count_over_time({event=~"route.db_error|route.domain_error|extract.record_error|stats.process_error"}[5m])) or vector(0)',
        datasource=LOKI_UID,
        service="pipeline",
        description="檢查 Router／Extractor／Stats 錯誤與 stats/bad；後續有 folder_done 也不能證明失敗資料已恢復。",
    )
    rules[-1]["annotations"].update(
        logs_query=PIPELINE_ERROR_QUERY, logs_url=explore_logs_url(PIPELINE_ERROR_QUERY)
    )

    for worker in normalized:
        selector = (
            f'{{service="crawler",worker_id="{worker}",event="spider.heartbeat"}}'
        )
        add(
            f"heartbeat-{worker}",
            f"Crawler {worker.zfill(2)} 連續缺少心跳",
            f"absent_over_time({selector}[2m]) or vector(0)",
            datasource=LOKI_UID,
            pending="2m",
            severity="critical",
            service="crawler",
            labels={"worker": worker.zfill(2)},
            description="近兩分鐘沒有心跳，且此情況連續維持兩分鐘才觸發。指定的 crawler 或其日誌路徑可能失去心跳；先分辨程序停止、Loki 問題或採集遺失，不自動重啟有記憶體佇列的程序。",
        )
        completed = f'{{service="crawler",worker_id="{worker}",event="request.end"}} | json status="status" | status >= 200 | status < 300 | status != 202 | __error__=""'
        ready = f'{selector} | json scheduler_size="scheduler_size" | scheduler_size > 0 | __error__=""'
        expression = (
            f"((sum(count_over_time({completed} [5m])) or vector(0)) < bool 1) * "
            f"((sum(count_over_time({ready} [2m])) or vector(0)) > bool 0)"
        )
        add(
            f"no-output-{worker}",
            f"Crawler {worker.zfill(2)} 有排程工作卻五分鐘無成功 HTTP 回應",
            expression,
            datasource=LOKI_UID,
            service="crawler",
            labels={"worker": worker.zfill(2)},
            description="近兩分鐘 scheduler 有工作，但近五分鐘沒有 2xx（排除 202）回應。這是完成回應的代理指標，不等於 HTML 已保存／入庫；確認限速冷卻、下載器等待、CPU／網路與後端。",
        )

    worker_pattern = "|".join(normalized)
    completed = (
        f'{{service="crawler",worker_id=~"{worker_pattern}",event="request.end"}}'
        ' | json status="status" | status >= 200 | status < 300 | status != 202 | __error__=""'
    )
    current = f"(sum(rate({completed} [5m])) or vector(0))"
    baseline = f"(sum(rate({completed} [30m] offset 5m)) or vector(0))"
    ready = (
        f'(sum(count_over_time({{service="crawler",worker_id=~"{worker_pattern}",event="spider.heartbeat"}}'
        ' | json scheduler_size="scheduler_size" | scheduler_size > 0 | __error__="" [2m])) or vector(0))'
    )
    add(
        "output-drop",
        "Crawler 完成回應量降至近期基準的一半以下",
        f"({current} < bool (0.5 * {baseline})) * ({baseline} > bool 0) * ({ready} > bool 0)",
        datasource=LOKI_UID,
        pending="10m",
        service="crawler",
        description="近五分鐘完成 2xx（排除 202）速率低於前一段 30 分鐘的一半，且有 scheduler 工作。這是初始警戒值，不是 QPS 上限或持久化成功量；需用實際站點組合校準。",
    )

    golden = config.get("golden_inject", {})
    if not isinstance(golden, dict):
        raise TypeError("golden_inject must be an object")
    if type(golden.get("enabled", False)) is not bool:
        raise ValueError("golden_inject.enabled must be boolean")
    if golden.get("enabled", False):
        baseline = golden.get("baseline_skipped_count", 179)
        if isinstance(baseline, bool) or not isinstance(baseline, int) or baseline < 0:
            raise ValueError("baseline_skipped_count must be a nonnegative integer")
        add(
            "golden-read",
            "Golden inject 監控無法讀取狀態",
            'crawler_golden_scan_success{job="crawler-pipeline"} == bool 0',
            pending="2m",
            service="golden-inject",
            description="checkpoint 或 latest-run 無法讀取，無法確認排程狀況；請保留原始狀態檔，不要直接重建或清空。",
        )
        add(
            "golden-overdue",
            "本週 Golden inject 未在寬限時間內成功完成",
            'crawler_golden_schedule_overdue{job="crawler-pipeline"}',
            severity="critical",
            service="golden-inject",
            description="採集器依台灣時間週一 09:00 與設定的寬限時間判斷；確認 cron、鎖、DB 和完成狀態。",
        )
        add(
            "golden-stalled",
            "Golden inject 執行中但進度超時未更新",
            'crawler_golden_stalled{job="crawler-pipeline"}',
            severity="critical",
            service="golden-inject",
            description="尚未完成的 checkpoint 超過設定的 stall_seconds 未更新；確認程序與 DB，不要同時啟動另一份注入。",
        )
        add(
            "golden-skipped",
            "Golden inject 跳過數超過已知基準",
            'crawler_golden_skipped_count{job="crawler-pipeline"}',
            threshold=baseline,
            service="golden-inject",
            description=f"本次 skipped_count 高於設定基準 {baseline}。確認新增的非法 URL、來源格式與 normalization；已知基準不會單獨重複觸發此告警。",
        )

    if enabled:
        requested = set(enabled_rule_uids)
        if not requested <= {rule["uid"] for rule in rules}:
            raise ValueError("unknown enabled rule UID")
        for rule in rules:
            rule["isPaused"] = rule["uid"] not in requested

    # Only the expensive response scans are slowed down. Keep missing-worker and
    # data-loss checks responsive without re-running 30-minute scans every minute.
    grouped = {"fast": [], "symptoms": [], "trends": []}
    for rule in rules:
        if rule["uid"] == "wc-output-drop":
            tier = "trends"
        elif rule["uid"].startswith("wc-no-output-"):
            tier = "symptoms"
        else:
            tier = "fast"
        grouped[tier].append(rule)
    intervals = {"fast": "60s", "symptoms": "300s", "trends": "600s"}
    documents = {
        "webcrawler-rules.json": {
            "apiVersion": 1,
            "groups": [
                {
                    "orgId": org_id,
                    "name": f"webcrawler-{tier}",
                    "folder": "WebCrawler Alerts",
                    "interval": intervals[tier],
                    "rules": group_rules,
                }
                for tier, group_rules in grouped.items()
                if group_rules
            ],
        }
    }
    if webhook:
        documents["webcrawler-contactpoints.json"] = {
            "apiVersion": 1,
            "contactPoints": [
                {
                    "orgId": org_id,
                    "name": CONTACT_POINT,
                    "receivers": [
                        {
                            "uid": "wc-discord",
                            "type": "discord",
                            "disableResolveMessage": False,
                            "settings": {
                                "url": webhook,
                                "message": DISCORD_MESSAGE,
                            },
                        },
                    ],
                },
                {
                    "orgId": org_id,
                    "name": INGEST_CONTACT_POINT,
                    "receivers": [
                        {
                            "uid": "wc-ingest-discord",
                            "type": "discord",
                            "disableResolveMessage": True,
                            "settings": {
                                "url": webhook,
                                "message": INGEST_DISCORD_MESSAGE,
                            },
                        }
                    ],
                },
            ],
        }
    return documents


def write_documents(output: Path, documents: dict[str, dict]) -> None:
    """Create a new private directory; do not overwrite live files or follow links."""
    output.mkdir(mode=0o700, parents=False, exist_ok=False)
    for name, document in documents.items():
        descriptor = os.open(output / name, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
        with os.fdopen(descriptor, "w", encoding="utf-8") as stream:
            json.dump(document, stream, ensure_ascii=False, indent=2)
            stream.write("\n")


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", type=Path, required=True)
    parser.add_argument(
        "--output", type=Path, required=True, help="New private output directory"
    )
    parser.add_argument("--enable", action="store_true")
    parser.add_argument(
        "--enable-rule",
        action="append",
        default=[],
        help="Explicit rule UID to enable; repeat for each reviewed rule",
    )
    parser.add_argument(
        "--routing-confirmed",
        action="store_true",
        help="Confirm the existing Grafana policy has a project=webcrawler child route",
    )
    args = parser.parse_args(argv)
    try:
        config = json.loads(args.config.read_text(encoding="utf-8"))
        documents = build_provisioning(
            config,
            enabled=args.enable,
            webhook=os.environ.get("DISCORD_WEBHOOK_URL"),
            routing_confirmed=args.routing_confirmed,
            enabled_rule_uids=args.enable_rule,
        )
        write_documents(args.output, documents)
    except (OSError, ValueError, TypeError) as error:
        # Never format an exception containing file data, JSON or a webhook.
        print(
            f"Render failed ({type(error).__name__}); validate config, secret and new output path.",
            file=sys.stderr,
        )
        return 1
    rules = [
        rule
        for group in documents["webcrawler-rules.json"]["groups"]
        for rule in group["rules"]
    ]
    active = sum(not rule["isPaused"] for rule in rules)
    print(
        f"Rendered {len(documents)} provisioning file(s); {active}/{len(rules)} rules enabled."
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
