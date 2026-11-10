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

## 先控制成本，再決定要不要通知

規則分成三個 evaluation group，UID、門檻與查詢語意維持一致：

| Group | 頻率 | 內容 |
| --- | --- | --- |
| `fast` | 60 秒 | 主機／採集健康、心跳、錯誤、IPC lag、golden 狀態 |
| `symptoms` | 300 秒 | 各 worker 有工作卻沒有完成回應 |
| `trends` | 600 秒 | 整體產量相對歷史基準下降 |

Prometheus 每 60 秒抓一次資料，IPC 採集每 60 秒更新；DB probe 每台每分鐘只建立一次短暫 TCP 連線，不登入、不執行 SQL。這些依然有成本，不是零負擔。

**門檻大多是未校準的起始值，不是根據長期 production 數據算出的 SLO。** 心跳預設 5 秒、FolderReader 的兩倍 interval、weekly cron 和已知 skipped 179 有現場／程式依據；15%／8%、一小時 lag、50% 產量下降與其他寬限時間仍需觀察。每 10 分鐘評估一次趨勢也可能漏掉短暫下降，不能宣稱連續逐秒觀測。

2026-11-10 約 18:09 台北時間的唯讀單次量測：

- 在 .2 對 48 組 stage／worker 完整採集約 35ms、CPU 約 28ms、Python 最高 RSS 約 19MiB；48 組皆成功。
- 原本整體產量查詢約 4.47s，Loki 統計處理約 803MB／182 萬筆日誌。這不是實際磁碟讀取量，但證明不能每條規則都頻繁重掃。
- .1 `/data` 可用比例約 7.85%，已低於起始 critical 門檻；inode 使用約 0.022%。本 PR 未部署，因此不會自動通知這個現況。

這些只是快照，不是持續效能驗證，也不能證明新頻率對 crawler 沒影響。要依下列觀察流程再決定啟用哪些規則。

## 真正不通知的觀察模式

`paused` 的 Grafana 規則完全不評估，因此不能用來驗證誤報。本 PR 另有 `shadow.py`，直接使用**同一份 generator 的查詢、門檻、pending 與 group 頻率**，只 GET Prometheus／Loki、寫入自己的觀察紀錄。它不呼叫 Grafana、不讀 Discord secret，沒有通知程式，也不更動既有 notification policy。

預設只開 `fast`。重查詢需明確加 `symptoms,trends`；第一次 symptoms／trends 分別延後 5／10 分鐘，不在啟動瞬間全部執行。一次只做一個查詢，拒絕同一輸出目錄多個 observer；同一輪相同查詢共用結果。每輪有 20 秒預算，超出就記錄 skipped／unknown，不能當正常；單查詢有 5 秒 server／socket timeout。預算是在查詢之間檢查，**不是後端 CPU 或整輪耗時的硬上限**。

```bash
# 只是操作範例：本 PR 不執行 production 部署。
# 先確認基礎監控已部署，再選擇啟動觀察 profile，仍不啟用 Grafana 告警。
docker compose -f compose.yml --profile shadow up -d crawler-shadow

# 觀察採集成本穩定後，才逐組增加查詢。
SHADOW_GROUPS=fast,symptoms,trends docker compose -f compose.yml --profile shadow up -d crawler-shadow

# 報告只讀本機 state，不發查詢／通知。
docker compose -f compose.yml --profile shadow exec -T crawler-shadow \
  python -B /app/shadow.py --report --output-dir /state/shadow

# 停止觀察只停新增的 observer，不動 crawler／ingestor／DB。
docker compose -f compose.yml --profile shadow stop crawler-shadow
```

獨立 CLI 用法：

```bash
python ops/monitoring/shadow.py \
  --config ops/monitoring/runtime/monitoring.json \
  --prometheus-url http://127.0.0.1:9098 --loki-url http://127.0.0.1:3100 \
  --output-dir ops/monitoring/runtime/shadow --groups fast
python ops/monitoring/shadow.py --report --output-dir ops/monitoring/runtime/shadow
```

`--once` 只執行當下到期的工作，仍遵守 warm-up，不能用啟動時的一次執行宣稱已驗證 trends。預設 `fast` 觀察也不能宣稱覆蓋全部規則。

觀察紀錄包括各規則觸發次數、狀態切換、查詢錯誤、漏評次數、耗時，以及 Loki 回傳的處理 bytes／lines。報告會列出 active groups／實際規則數、每條規則的觀察起點與資料新鮮度；停止 observer 後讀舊檔會標為 stale，最後已知狀態不代表當下健康。更動規則或重新加入停止觀察的 group 會開始新的該規則統計期間，不把不同版本混算。狀態與 journal 為 0600、目錄為 0700，journal 輪替保留最多三份、各最多 10MiB；累計統計保留在小型 state。只寫自己的 named volume，不寫 IPC 或 DB。shadow container 上限 0.25 CPU／128MiB；**此限制管不到既有 Loki 因查詢新增的工作量**。

這是 threshold／pending 的觀察工具，不是 Grafana 的完整複製：missing series 的解除時機可能不同，報告的 firing episodes **不是 Discord 通知數**；分組、重送與通知成功都還需正式接線後實測。

建議先比對 30～60 分鐘的基準，再從 `fast` 逐類加入；完整候選組觀察 24～48 小時。每次檢查 crawler 產量、IPC lag、Loki CPU／記憶體／query latency、磁碟等待與 observer 的錯誤／skip。若出現持續惡化或重查詢大量漏評，先停止該組並調整，不能為了告警拖慢業務。24～48 小時結束**不會自動啟用 Discord**。

## 通知合併與降噪

`notification-route.example.json` 是待人工加入既有 policy 的 **child route 片段**，不是可以直接匯入整棵 policy 的 provisioning 檔。generator 不會自動套用它。

- 只匹配 `project=webcrawler`，保留其他人的 root receiver／路由。
- 嚴重告警依 `project,severity,service` 合併，不以 worker 或 alertname 拆組；等 30 秒合併、更新至少間隔 5 分鐘、未變化每 2 小時重送。
- warning 全部依 `project,severity` 合併；首次等 1 小時、更新至少間隔 1 小時、未變化每 24 小時重送。若在 group wait 內恢復，可能不發通知。
- Discord message 只列最多 6 項簡短摘要及 Grafana 入口，避免一次大量 worker 錯誤塞滿訊息。
- 已知事件用**有期限**的 silence；不是永久忽略容量不足。

這是合併與節流，**不是每小時最多 N 則的硬上限**；多個 critical service group、恢復或反覆觸發仍可能產生多則。shadow 先用於辨識誤報與抖動，實際訊息數／傳送成功仍需接線測試。初次啟用建議只開容量、失聯、OOM、入庫批次失敗，其他規則等觀察結果合理再開。

## 設定與保護

- `monitoring.example.json` 是明確的預期 worker 清單與進度路徑；部署時應比對 Supervisor **實際啟用**清單，不是把所有測試 container 都視為 production。worker 增減要同步改設定。
- 採集器只讀 `/data/ipc`，不連 DB、不讀 URL 內容、不修改 checkpoint、不自動修復或重啟服務。不掛 Docker socket。
- 每個 stage／worker 有目錄掃描數量上限；只讀日期／時間目錄及小型 JSON，超限會發出採集失敗，不能回報部分結果當成完整狀態。採集在背景定期執行，HTTP scrape 讀快取。
- 指標 label 僅使用有限的 stage、worker、host、service，不放 URL、domain 或 job ID。
- `compose.yml` 是獨立 project；`compose.node.yml` 是每台主機獨立 project。不要把它們當作正式 `docker-compose.yml` 的 override。
- 本 PR 不修改現有 Compose、Grafana datasource provisioning、Loki 或 production hotfix。`grafana-datasource.yml` 是待部署的新 datasource 檔，不會自動載入。
- 所有 Grafana 規則預設 **paused**。未提供 webhook、未確認 routing，或未逐項提供 `--enable-rule UID` 時，generator 拒絕產生 enabled 規則；其餘規則繼續 paused。generator 不呼叫 Grafana／Discord，也不發測試通知。
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
8. 先依觀察模式分組試跑並檢查成本與誤報。之後才參考 `notification-route.example.json` 加入 child route，確認 grouping 不含 worker／alertname。Grafana 11 沒有這裡需要的自訂根因抑制，Loki 或主機失聯仍可能造成多組關聯告警。
9. 經人工確認 routing、targets 與觀察結果後，以 `--enable --routing-confirmed --enable-rule wc-ingest-error` 等明確 UID 清單產生檔案；可重複 `--enable-rule`。只有列出的規則解除 paused，其餘維持暫停，CLI 會印出 enabled 數量。使用新的私有輸出目錄；替換前檢查差異。這個旗標是操作人員的確認，不會透過 API 驗證路由。
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
