# Scheduler-Crawler Architecture Documentation

This documentation set describes the architecture implemented in this repository, covering:

- Container layout and process topology
- End-to-end data flow across queueing, crawling, routing, ingesting, feature extraction, and aggregation
- SQL schema design (including 256-way shard table families)
- Docker/Compose runtime settings and operational implications

## Documents

1. [01-system-architecture.md](./01-system-architecture.md)
2. [02-container-runtime-design.md](./02-container-runtime-design.md)
3. [03-data-flow-and-ipc.md](./03-data-flow-and-ipc.md)
4. [04-sql-schema-design.md](./04-sql-schema-design.md)
5. [05-docker-settings-and-deployment.md](./05-docker-settings-and-deployment.md)
6. [06-maintenance-scripts.md](./06-maintenance-scripts.md)
