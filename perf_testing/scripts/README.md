# DB效能測試腳本

對應報告:`notes/2026-10-08-merge-table-perf-retest.md`、`notes/2026-10-11-weekly-report.md`(這兩份報告在`notes/`,已被`.gitignore`排除,不在這個repo的版控範圍內)
原始結果數字:`../data/db_comparison_summary.csv`(同樣被`.gitignore`排除,只留在本機)

只保留平常常測的upsert查詢,依資料庫分資料夾。每份都是**單個worker**的腳本,實際測試時開多個worker同時跑(見下方「怎麼跑」)。

## 方法論(所有測試共同遵守)

1. **持久連線**:每個worker用`docker exec -i postgres psql <connstr> < worker.sql`,一個worker從頭到尾只開一個連線,不是每次query重新連線。可以用`SELECT pg_backend_pid();`在同一個session裡跑幾次確認pid不變,來驗證連線真的沒斷。
2. **安全**:全部用`BEGIN; ... ROLLBACK;`包裹,不留下實際寫入。
3. **並發**:多個worker腳本同時用`&`背景執行+`wait`,模擬正式環境多個連線同時打DB的狀況。
4. **隨機domain_id**:每筆INSERT的`domain_id`都是隨機生成,避免always命中同一個tablet/shard造成不公平的加速。

## 目錄結構

```
postgresql/
  upsert_worker.sql              baseline(文字SQL,固定shard table)

yugabytedb/
  upsert_worker.sql              baseline(文字SQL,合併表)
  upsert_worker_concurrency30.sql   並發度ablation(30 workers版本的其中一個)
  upsert_worker_concurrency50.sql   並發度ablation(50 workers版本的其中一個)
  upsert_worker_batched20.sql       batched insert(一個INSERT塞20筆)

cockroachdb/
  upsert_worker_prepared.sql        用PREPARE/EXECUTE參數化查詢(⚠️CockroachDB一定要這樣測,見下方說明)
  upsert_worker_concurrency50.sql   並發度ablation(PREPARE/EXECUTE版本)
  upsert_worker_batched20.sql       batched insert(文字SQL,一個INSERT塞20筆)
```

## 為什麼CockroachDB要特別用PREPARE/EXECUTE

CockroachDB的query plan cache預設是開啟的,但**只有PREPARE過的查詢、或字面值完全相同的查詢才會命中**。如果像PostgreSQL/YugabyteDB那樣直接把值刻進SQL文字裡(每次domain_id/url都不同),CockroachDB會把每一句都當成新查詢,每次重新跑一次cost-based optimizer,throughput會少將近30%、p99延遲會差將近60%。PostgreSQL/YugabyteDB不受這個影響,文字SQL跟參數化查詢測出來的數字幾乎一樣。

`upsert_worker_prepared.sql`開頭會先`PREPARE ins_stmt (...) AS INSERT ... VALUES ($1,$2,...) ON CONFLICT ...;`,之後每筆迭代用`EXECUTE ins_stmt('url', domid, now(), ...);`帶實際值執行,而不是把值寫進SQL文字裡——這對應到正式環境SQLAlchemy/psycopg2走參數化查詢時,底層實際在做的事。

## 怎麼重新產生多個worker(30/50個)

並發ablation測試用到的30或50個worker檔案,每個都只是「換一個目標shard table + 換一組隨機domain_id/url」,結構跟對應的sample完全一樣。重新產生的方式(Python範例,YugabyteDB版本)：

```python
import random
template = '''BEGIN;
\\timing on
INSERT INTO url_state_current (url, domain_id, last_fetch_ok, last_content_update, last_modified, num_fetch_ok_90d, num_fetch_fail_90d, num_content_update_90d, num_consecutive_fail, last_fail_reason, content_hash, should_crawl, title, hreflang_count, has_json_ld, etag, cache_control, is_redirect, redirect_hop_count, robots_bits)
VALUES ('https://perftest.example.com/w{i}', {domid}, now(), now(), now(), 1, 0, 1, 0, NULL, 'abc123hash', FALSE, 'Test Page Title', 2, TRUE, 'etag123', 'no-cache', FALSE, 0, 1)
ON CONFLICT (domain_id, url) DO UPDATE SET
  last_fetch_ok = COALESCE(EXCLUDED.last_fetch_ok, url_state_current.last_fetch_ok),
  num_fetch_ok_90d = url_state_current.num_fetch_ok_90d + EXCLUDED.num_fetch_ok_90d;
\\timing off
ROLLBACK;'''
N = 20000
for w in range(1, 31):  # 改成 51 就是50-worker版本
    lines = [template.format(i=f'{w}_{i}', domid=random.randint(1, 999999999)) for i in range(N)]
    with open(f'worker_{w}.sql', 'w') as f:
        f.write('\n'.join(lines))
```

CockroachDB版本結構一樣,只是在檔案最前面加一次`PREPARE`,迴圈裡改成`EXECUTE ins_stmt(...)`。

## 怎麼跑

```bash
# N個worker同時跑,背景執行
for w in $(seq 1 N); do
  docker exec -i postgres psql <connstr> < worker_${w}.sql > log_${w}.txt 2>&1 &
done
wait
```

連線字串範例：
- PostgreSQL:`psql -h <A機IP> -p 5432 -U <user> -d crawlerdb`
- YugabyteDB:`psql -h <A機IP> -p 5434 -U yugabyte -d crawlerdb`
- CockroachDB:`psql "postgresql://root@<A機IP>:26257/crawlerdb?sslmode=disable"`

## 怎麼分析log算出throughput/latency

每個log檔案裡`\timing on`之後的每個statement都會印一行`Time: X.XXX ms`。因為腳本結構是`BEGIN; INSERT; ROLLBACK;`三個statement都在`\timing on`範圍內,所以每輪迭代會印3行`Time:`,**要取每3行裡的第2行**(對應INSERT)才是真正的寫入延遲,不要把BEGIN/ROLLBACK的時間也算進去。

```python
import re, statistics
times = []
with open('all_logs.txt') as f:
    for line in f:
        m = re.match(r'^Time: ([0-9.]+) ms', line)
        if m:
            times.append(float(m.group(1)))
inserts = times[1::3]  # 取每組3筆裡的第2筆
inserts.sort()
n = len(inserts)
print('mean:', statistics.mean(inserts))
print('p95:', inserts[int(n*0.95)])
```

throughput = 總迭代次數 / 整批worker全部跑完的總耗時(秒)。
