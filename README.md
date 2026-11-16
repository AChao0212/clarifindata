# clarifindata collection pipeline

This repository contains a runnable collection and publishing subset of [clarifindata](https://clarifindata.com), a free, not-for-profit Taiwan market-data API. Every account receives the same data and a limit of 3,000 requests per hour. The service collects data from public web pages and open data published by TWSE, TPEx, TAIFEX, MOPS, the central bank and other sources. This repository shows how selected collectors parse responses, archive raw payloads and publish rows; it does not contain the hosted API service.

## Files

```text
api/
  __init__.py                 Package marker.
  config.py                   Local environment settings.
ingestion/
  __init__.py                 Package marker.
  publish.py                  Archive responses and publish row batches.
  kafka_io.py                 Kafka producer and consumer helpers.
  minio_io.py                 JSON archive writer.
  collector.py                Date selection, row checks and optional alerts.
  completeness_guard.py       Minimum captured-row checks.
  date_guard.py               Source-response date validation.
  trading_calendar.py         Postgres trading-day lookups.
  waf_guard.py                Source-refusal detection and stop signals.
  crawlers/
    __init__.py               Package marker.
    bot_exchange_rate.py      Bank of Taiwan exchange rates.
    twse_day_trading.py       TWSE day-trading statistics.
    twse_delisting.py         TWSE delisting records.
    twse_fundamentals.py      TWSE company fundamentals.
    twse_news.py              TWSE company announcements.
    twse_per.py               TWSE valuation ratios.
    twse_total_inst.py        TWSE institutional trading totals.
    twse_weighted_index.py    TWSE weighted-index history.
scripts/
  macro_extra.py              Additional public macroeconomic series.
  mops_bulk_financials.py     MOPS bulk financial statements.
  mops_direct.py              MOPS quarterly financial statements.
  mops_dividend_worker.py     MOPS dividend records.
  mopsfin_quarterly.py        Quarterly financial tables from MOPS.
  taifex_contract_info.py     TAIFEX contract specifications.
schema.sql                   Minimal trading-calendar table.
docker-compose.yml           Local Postgres, Kafka and MinIO services.
requirements.txt             Python dependencies for this subset.
.gitignore                   Local environment and Python output exclusions.
ARCHITECTURE.md              Data flow and repository boundaries.
LICENSE                      MIT software licence.
README.md                    Setup and source information.
```

## Run one crawler

Use Python 3.11 or newer and Docker Compose. All published service ports bind to the loopback interface. Credentials below are for local development only.

```sh
docker compose up -d
python3 -m venv .venv
. .venv/bin/activate
pip install -r requirements.txt

export CFD_DB_URL=postgresql://clarifindata:dev@localhost:5432/clarifindata
export KAFKA_BOOTSTRAP_SERVERS=localhost:9092
export KAFKA_TOPIC_RAW=raw
export MINIO_ENDPOINT=localhost:9000
export MINIO_ACCESS_KEY=minioadmin
export MINIO_SECRET_KEY=minioadmin
export MINIO_BUCKET_RAW=raw
export MINIO_SECURE=false
export LOG_LEVEL=INFO
```

Wait for Kafka and MinIO to report readiness in `docker compose logs`. Create the raw archive bucket once:

```sh
python - <<'PY'
from api.config import settings
from ingestion.minio_io import client

bucket = settings().minio_bucket_raw
storage = client()
if not storage.bucket_exists(bucket):
    storage.make_bucket(bucket)
PY

python -m ingestion.crawlers.twse_per --date 2026-11-13 --force --no-alert
```

This command fetches valuation data for the supplied date, archives its response in MinIO and publishes parsed rows to Kafka. It needs access to the public source. A source refusal or a collection error can make the run fail. Inspect the error before making another request.

`--force` skips the local trading-calendar lookup for an explicitly supplied date. Check that the date is an official trading day before using it. It does not disable source-refusal checks. Without this flag, dated collectors need official calendar rows in `taiwan_stock_trading_date`. The Compose init creates only that table, without assumed weekday flags or sample market data. Schema initialization runs when the Postgres volume is first created.

Settings can also be read from a local `.env` file. The trading-calendar helper reads `CFD_DB_URL` from the process environment, so export that variable when using calendar-based date selection. `DATABASE_URL` is an alternative setting for completeness lookups; keep both database addresses consistent if using it.

Telegram alerts are optional. The harness reads `BOT_TOKEN` and `BOT_CHAT_ID` from `~/.secret/telegram.txt` at runtime if that file exists. `--no-alert` or `CFD_NO_ALERT=1` suppresses alerts without suppressing collection failures.

Some scripts submit rows to a separately running API rather than Kafka. Their default address is `http://localhost:8000`; they read an administrative bearer token from `~/.secret/cfd_admin_token`, or accept a token argument where provided. The dividend script also needs a company-code file. Their ingestion endpoints, the Kafka-to-Postgres consumer, dataset schemas and the API server are not included. See each script's command-line help and [ARCHITECTURE.md](ARCHITECTURE.md) before running it.

## Sources and use

The collectors in this repository use public pages and open data from TWSE, TPEx, MOPS, TAIFEX, the Bank of Taiwan and FRED. Each collector keeps its source URL in the code, and published messages carry it where the source provides one.

clarifindata has no licence agreement with these institutions. The collected data is unofficial. Source data remains subject to its rights holders' terms and is not licensed by this repository's software licence. Rights holders can request removal at [support@clarifindata.com](mailto:support@clarifindata.com); requests are handled within 7 days.

For hosted API access, see the [documentation](https://docs.clarifindata.com) and the [Python SDK](https://github.com/AChao0212/clarifindata-python).

## Licence

The code is provided under the [MIT licence](LICENSE).
