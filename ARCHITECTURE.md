# Architecture

The service's data path is:

```text
crawl -> raw archive to MinIO -> Kafka -> consumer -> Postgres -> API
```

## Collection and publishing

This repository includes selected source collectors, the shared collector harness and publishing helpers. Crawlers request public source data and parse it into dataset rows. Date guards reject responses for another date. Completeness guards check row counts where the source has returned usable data. The refusal guard identifies a source refusal and raises a stop signal; the harness stops the remaining dates when it receives that signal.

`ingestion.publish.publish_crawl` first writes the raw response as JSON through `ingestion.minio_io`. Archive keys include the dataset, caller-provided key parts and a UTC timestamp. The MinIO bucket must exist before a run.

After archiving, `publish_crawl` sends Kafka messages through `ingestion.kafka_io`. Each message contains the dataset, parsed rows, archive object key, source URL, fetch timestamp and batch metadata. At most 800 rows are sent in each message. Even an empty row list produces a message. Archive writes and Kafka publication are separate operations, not a shared transaction; a failed publication can leave an archived response without its message.

The collector harness selects dates, checks the trading calendar when required, checks configured row-count floors and returns a nonzero exit code for failures. Telegram delivery is optional and does not affect collection results. Source-specific collectors retain their own parsing and request behavior.

## Database and API boundary

The Kafka consumer that normalizes rows and writes dataset tables is not included. The consumer helper is included, but it is not an ingestion worker. A database-writing consumer should commit its Kafka offsets only after the corresponding Postgres transaction succeeds.

Postgres dataset tables, migrations, API handlers and account management belong to the separate API service and are not included. The Compose stack starts Postgres so the included trading-calendar helper can query it. `schema.sql` creates only `taiwan_stock_trading_date`; official dates must be loaded separately before calendar-based collection. Historical completeness lookups can query dataset tables supplied by the API service and retain their configured absolute floor when that lookup fails.

The scripts under `scripts/` include a second submission path: they POST rows to ingestion endpoints on a separately running API service. This repository does not implement those endpoints. These scripts therefore need that service even though the Kafka-based crawler example does not.

## Local services

`docker-compose.yml` supplies Postgres 16, Kafka 3.8.0 and MinIO. Published ports bind to `127.0.0.1`. Postgres and MinIO use named volumes. Kafka is a single local broker with automatically created topics and no configured persistent volume. Redis is not required by this subset and is not started.

Configuration is environment-driven through `api.config`. The trading-calendar helper also reads the exported `CFD_DB_URL` directly. Defaults point to local services and development credentials. No external service is contacted merely by importing the collector modules.
