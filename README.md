# Crypto Supervision Chain

The goal is to provide a basic example of how to **collect**, **transform**, **store** and **visualise** crypto‑market data using only open‑source tools.  The implementation here is intentionally simple and intended for educational purposes; it is _not_ production ready.  Nevertheless, it demonstrates how the building blocks fit together and lays the foundation for a more complete system.

## Architecture

The stack consists of the following components:

| Layer              | Purpose                                           | Tools                |
|--------------------|----------------------------------------------------|----------------------|
| **Collect**        | Poll price data from an exchange API and expose Prometheus metrics | [`collector/app.py`](collector/app.py) (Python + CCXT) |
| **Store**          | Persist price history in a relational database     | PostgreSQL           |
| **Metrics**        | Scrape application metrics                         | Prometheus           |
| **Visualise**      | Build dashboards and alerts                       | Grafana              |
| **Logs (optional)**| Aggregate and explore logs                         | Loki + Promtail      |

All services are orchestrated via `docker-compose`.  The collector writes price snapshots to Postgres, exposes metrics on port `8000` and is scraped by Prometheus every 15 seconds.  Grafana is pre‑configured with data sources for Prometheus and Postgres (user: `admin`/`admin`).  Loki and Promtail are included as optional log aggregation components.

## Quick start

> **Prerequisites:** Docker ≥ 20.10 and Docker Compose ≥ 1.29, with a Linux host (to allow Promtail to read container logs).

Clone this repository and start the stack:

```bash
git clone https://github.com/<your‑username>/crypto-supervision.git
cd crypto-supervision
docker compose up -d
```

Open Grafana at <http://localhost:3000> (login `admin`/`admin`) to explore the dashboards.  Prometheus is available at <http://localhost:9090> and Postgres at `localhost:5432` (database `crypto`).

## Collector overview

The Python collector (see [`collector/app.py`](collector/app.py)) uses the [CCXT](https://github.com/ccxt/ccxt) library to fetch the latest ticker price for a trading pair (default: `BTC/USDT`).  It writes each observation into the `prices` table of the Postgres database and exposes metrics via [Prometheus Client](https://github.com/prometheus/client_python).  Metrics include:

* `crypto_price{symbol="BTC/USDT"}` – the latest fetched price
* `api_latency_seconds` – time taken to fetch a ticker from the exchange
* `collector_errors_total` – total number of exceptions encountered
* `last_update_timestamp` – Unix timestamp of the last successful update

Environment variables allow you to change the trading pair (`SYMBOL`), the polling interval (`POLL_INTERVAL`) and the database connection (`PGHOST`, `PGUSER`, `PGPASSWORD`, `PGDATABASE`).

## Database schema

An initialisation script in [`postgres/init.sql`](postgres/init.sql) creates a single table:

```sql
CREATE TABLE prices (
    id SERIAL PRIMARY KEY,
    symbol TEXT NOT NULL,
    price NUMERIC,
    ts TIMESTAMP NOT NULL
);

CREATE INDEX idx_prices_ts ON prices(ts);
```

This table stores each price snapshot along with a timestamp.  The index on the timestamp accelerates time‑series queries.

## Configuration

Configuration files for Prometheus, Loki and Promtail live under their respective directories.  Grafana provisioning is provided under `grafana/provisioning` and automatically adds Postgres and Prometheus as data sources on first start.

## Limitations and next steps

This repository is a starting point.  A production‑grade crypto observability platform would need to address many additional concerns, including:

* Handling multiple trading pairs and multiple exchanges concurrently
* Using WebSocket streams for lower latency and higher data quality
* Performing enrichment and feature engineering within a streaming pipeline (e.g. Kafka + Flink)
* Implementing alerting rules in Prometheus or Grafana Alerting
* Adding dashboards for market and operational metrics
* Hardening the collector with retries, backoff and better error handling

Feel free to extend this project according to the ideas and recommendations from the original conversation.

## License

This project is licensed under the MIT License.  See [LICENSE](LICENSE) for details.
