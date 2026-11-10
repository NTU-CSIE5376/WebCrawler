# WebCrawler 告警：第一版

這是一組**獨立、選擇性啟用**的監控工具。PR 合併不會啟動服務、改爬速或重啟 crawler。先 review，之後才另外安排部署與 Discord 測試。

沿用現有 **Grafana 11 + Loki 3**：新增獨立 Prometheus 收集主機、服務探測與管線狀態，讓 Grafana 評估告警並通知 Discord。這版不需要 Alertmanager，也不修改其他人的監控服務。

```text
.1/.2/.3/.4 node_exporter ──────┐
.2 唯讀 IPC 採集器 ────────────┼→ 獨立 Prometheus → 現有 Grafana 11 → Discord
HTTP/TCP 探測 ─────────────────┘                      ↑
現有 crawler / pipeline 日誌 → 現有 Loki ─────────────┘
```

## 會通知什麼

| 狀況 | 第一版判斷 | 注意事項 |
| --- | --- | --- |
| 主機或採集器連不上 | Prometheus scrape 連續失敗兩分鐘 | 指標斷線和程序故障需要再分辨 |
| DB／Loki 探測失敗 | 指定 TCP／HTTP 探測失敗兩分鐘 | DB TCP 成功只代表能連線，不代表 SQL 正常或 YB 有 quorum |
| 容量或 inode 不足 | 可用容量低於 15%／8%、inode 低於 5%，以及容量成長預測 | 僅 `/`、`/data`；門檻是起始設定，並非量測後的 SLO |
| 記憶體不足／OOM | MemAvailable 長期低於 5%、近期 OOM kill | 不因 swap 歷史占用高就發告警 |
| 特定 crawler 消失 | 每個預期 worker 超過兩分鐘沒有 `spider.heartbeat` | 單一 worker 死掉也能發現；Loki 壞掉也會觸發，需要看相關告警 |
| 有工作但停止產出 | scheduler 有工作，五分鐘沒有成功 HTTP 回應 | 使用 `request.end` 的 2xx，排除 202；這不是「HTML 已入庫」證明 |
| 整體完成量下降 | 有 scheduler 工作，完成回應速率低於近期基準一半，持續十分鐘 | 網站組合與冷卻也可能影響，要校準門檻 |
| Router／Ingestor／Extractor 積壓 | 最老的可處理資料已等超過一小時，持續十分鐘 | 已扣除每階段原本刻意等待的兩個 interval，預設共 20 分鐘 |
| 批次入庫錯誤 | `ingest.batch_error` 立即 critical | progress 仍可能前進，告警解除不代表失敗資料已補回 |
| 其他管線錯誤 | Router／Extractor／Stats 指定錯誤事件 | 應確認是否跳過資料，不能只看下一個 folder 成功 |
| Golden inject 漏跑／卡住 | 每週一 09:00 台北時間，允許 30 分鐘；未完成且 15 分鐘沒進度 | 以 checkpoint 判斷；已知 179 筆 skipped 是起始基準，新增跳過才提醒 |
| 監控本身失明 | 掃描失敗、缺少進度檔、超過掃描上限、背景採集過舊 | 這些不會被當成零積壓或健康 |

`request.start` 不是實際送出的 QPS，因此不拿它當成功產量。`request.end` 仍只是第一版代理指標：下一階段應增加「已保存的有效 HTML」與「已 commit 的筆數」。normal／API 的新增 source 標記也不是這版啟用的前提。

## 設定與保護

- `monitoring.example.json` 是明確的預期 worker 清單與進度路徑；部署時應比對 Supervisor **實際啟用**清單，不是把所有測試 container 都視為 production。worker 增減要同步改設定。
- 採集器只讀 `/data/ipc`，不連 DB、不讀 URL 內容、不修改 checkpoint、不自動修復或重啟服務。不掛 Docker socket。
- 每個 stage／worker 有目錄掃描數量上限；只讀日期／時間目錄及小型 JSON，超限會發出採集失敗，不能回報部分結果當成完整狀態。採集在背景定期執行，HTTP scrape 讀快取。
- 指標 label 僅使用有限的 stage、worker、host、service，不放 URL、domain 或 job ID。
- `compose.yml` 是獨立 project；`compose.node.yml` 是每台主機獨立 project。不要把它們當作正式 `docker-compose.yml` 的 override。
- 本 PR 不修改現有 Compose、Grafana datasource provisioning、Loki 或 production hotfix。`grafana-datasource.yml` 是待部署的新 datasource 檔，不會自動載入。
- 所有 Grafana 規則預設 **paused**。未提供 webhook、未確認 routing 時，generator 拒絕產生 enabled 規則。generator 不呼叫 Grafana／Discord，也不發測試通知。
- `runtime/` 不進 Git。生成目錄 0700、檔案 0600；contact point 檔含 webhook，勿貼到 PR、log 或聊天。Grafana 載入前要另以正確 UID 與唯讀權限安置檔案。

## 本機驗證（不部署、不發通知）

```bash
python -m pip install pytest==8.4.2 PyYAML==6.0.2
python -m pytest -q ops/monitoring/tests
mkdir -p ops/monitoring/runtime
cp ops/monitoring/monitoring.example.json ops/monitoring/runtime/monitoring.json
python ops/monitoring/render_alerting.py \
  --config ops/monitoring/runtime/monitoring.json \
  --output ops/monitoring/runtime/paused-review
```

若 shell 已設定 `DISCORD_WEBHOOK_URL`，generator 會把 contact point 一起寫入**私有輸出目錄**，但規則仍 paused；純 review 時先清除該環境變數。已存在的輸出目錄不覆寫，重做時請使用新的目錄。

採集器可獨立跑在測試 IPC 樹：

```bash
python ops/monitoring/pipeline_exporter.py \
  --config ops/monitoring/runtime/monitoring.json --listen 127.0.0.1 --port 9812
```

`/metrics` 即使採集失敗仍回傳錯誤指標；`/healthz` 只有快取新鮮且設定的採集全部成功才回 200。缺少 progress 檔會顯示失敗；新環境尚未產生檔案時應先確認管線啟動，不自行造假進度。

## 後續部署順序（本 PR 不執行）

1. 保存現有 Compose／override／外掛 hotfix 的檔案與 hash、Grafana datasource／contact point／policy 設定、現有容器與 PID。確認沒有別人同時操作。不要用整份 main 蓋 production。
2. 確認各 host 的 IP、9108 port、網路防火牆與磁碟掛載。只允許監控主機存取 node exporter。`HOST_METRICS_BIND` 必須填該機的私有 IP，不是 `0.0.0.0`。在四台主機以獨立 `compose.node.yml` 啟動 exporter。
3. 在 .2 使用獨立工作目錄，安置本目錄檔案與 `runtime/monitoring.json`，確認讀取 IPC 所需的 UID／GID、資料夾權限與 worker 清單。不要更動原本資料夾的 owner／mode 來配合監控。
4. 確認既有 Grafana Docker network 名稱，設定 `CRAWLER_DOCKER_NETWORK`、`MONITOR_UID`、`MONITOR_GID`。先執行 `docker compose -f compose.yml config --quiet`，再以這個**獨立檔案**啟動新增監控服務。它不會啟動／重建既有 crawler、DB 或 Grafana。
5. 檢查 Prometheus targets、採集錯誤、可用容量、lag 與 worker 心跳是否符合現場。Prometheus UI 綁 .2 loopback 9098；不要對外開放未認證的監控介面。先確認 target 清單與實際 production 相符，排除未部署服務。
6. 待 review 核准後，只新增 `crawler-prometheus` datasource 檔與 paused 告警檔到**既有** Grafana provisioning。先查是否已有相同 UID，避免覆蓋別人的設定。現有 Loki datasource 不改、預設 datasource 不改。reload／重啟 Grafana 前先確認不影響他人；不需重啟 crawler。
7. Discord 資訊提供後，用環境 secret 產生私有 contact point 檔，安置為 Grafana UID 可讀的唯讀檔。由現有 notification policy 加一條 `project=webcrawler` **child route** 指到 `webcrawler-discord`；保留其他路由。generator 刻意不產生 `policies`，避免整棵覆蓋。
8. 建議 group by `project,severity,service,host`，group wait 30s、group interval 5m、repeat 2h，讓同一服務多個 worker 合併通知。維護時用 silence。Grafana 11 沒有這裡需要的自訂根因抑制，Loki 或主機失聯仍可能造成多組關聯告警。
9. 經人工確認 routing 與 targets 後，`--enable --routing-confirmed` 才產生 enabled 檔。使用新的私有輸出目錄；替換前檢查差異。這個旗標是操作人員的確認，不會透過 API 驗證路由。
10. 在隔離測試目標上依序驗證：worker 心跳缺席、target down、虛擬容量不足、IPC ready／未 ready、batch error，以及 Discord 收到與解除通知。不要停 production 程序來測告警。最後再次比對 hotfix hash、PID 與爬量。

回滾只移除／暫停本 PR 新增的告警與監控 project；保留 Prometheus volume 供調查。不要對既有 production Compose 執行 `down` 或 `down -v`。

## 明確還沒完成的能力

- **外部 dead-man watchdog**：Grafana／Loki／Prometheus 同在 .2，整台斷電時無法自己傳 Discord。正式依賴此系統前，要用不同故障範圍的監測服務定期檢查其 heartbeat／health endpoint，並實測斷線通知；不能把本機正常視為這個風險已解決。
- **DB 真正 readiness**：TCP 只測連線。後續要加入低成本 SQL、YB quorum／鎖等待與交易錯誤；不要每分鐘掃完整網址表。
- **Offerer 補貨、API job／TTL、stats/bad、設定 drift**：這版提供 pipeline lag 與錯誤日誌，尚未提供這些專用告警。drift 應與「核准的已部署 manifest」比較，不能因 main 有尚未部署的 PR 就報錯。
- **持久化成功量與排程狀態**：需要明確 commit/save counter、可執行工作及冷卻資訊，才能區分合理限速與故障。第一版的輸出下降只發 warning。
- **通知傳遞本身**：仍需接上外部 watchdog 並定期實測 Discord；本 PR 尚未取得 webhook，也未做真實傳送測試。

Grafana 11 的 NoData／Error 會跳過 pending period；本版不把缺資料設成 OK，而是以明確時間視窗檢查 worker absence，並另外通知採集失明。

參考：[Grafana 11 Discord](https://grafana.com/docs/grafana/v11.0/alerting/configure-notifications/manage-contact-points/integrations/configure-discord/)、[Grafana 11 provisioning](https://grafana.com/docs/grafana/v11.0/alerting/set-up/provision-alerting-resources/file-provisioning/)、[node exporter 主機掛載](https://github.com/prometheus/node_exporter/tree/v1.9.1#docker)、[blackbox exporter](https://github.com/prometheus/blackbox_exporter/blob/v0.26.0/CONFIGURATION.md)。
