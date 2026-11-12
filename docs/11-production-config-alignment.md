# Approved production configuration

Keep the production values in these tracked files. No crawler, API, GC or scheduling behavior is changed by this alignment.

| Item | Source | Approved value |
| --- | --- | --- |
| API database | containers/api/config/config.yml | Existing passwordless Yugabyte172.16.191.2:5433/crawlerdb |
| Site limits | containers/crawler/config/site_policies.yml | Existing38hosts;Wikipedia shared crawler concurrency7,QPS15,target8 |
| Dispatch | crawler settings and Compose | Existing20percent weighted API share |
| Capacity | containers/crawler/supervisord.conf | Worker15:384/150MB; others256/100MB (#87) |
| Discovery scoring | control.yaml and Compose | V2,V1fallback,steering enabled,16workers |
| Sitemap patrol | sitemap.yaml | Batch1000,delay0.5s,loop60s |
| Cleanup | script defaults and ops/production/cron.d/crawl-result-cleanup | Consumed min2h; unread forced deletion disabled |

The model environment settings in Compose match the existing production settings; artifact files remain unchanged. Do not start the legacy PostgreSQL service or recreate databases during this alignment. Deploy only named pipeline services, preserving current dependency images and data mounts.

Before restarting crawler workers, preserve their memory-only normal requests. Use the existing per-domain IPC input format to return preserved work, and restore existing Retry-After runtime state before releasing inputs. API unfinished work already uses persisted result indices. These are operator steps; no startup recovery feature is added.

Do not interrupt the separately managed router7 catch-up operation. Services whose effective settings already match these values do not need a restart. Supervisor reread alone does not update cached group definitions; verify the affected crawler group when applying its15/7 capacity placement.

Retire old private Compose overrides after saving them. A retained file alias must resolve to the tracked configuration, not carry its own policy values.

Wikipedia concurrency reserves two additional requests for the existing sitemap processes. It does not establish an aggregate system QPS guarantee; see#145.
