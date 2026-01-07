import time
import os
import ccxt
import psycopg2
from prometheus_client import start_http_server, Gauge, Counter

"""
A minimal crypto price collector.

The collector fetches the latest price for a given trading pair from a supported
exchange (Binance by default), stores the price and timestamp in PostgreSQL and
exposes Prometheus compatible metrics on port 8000.

This implementation is intentionally simple: it polls every 15 seconds and
does not handle all of the edge cases described in the design document
(multiple assets, WebSockets, enrichment, error backoff, etc.), but it
implements the core loop end‑to‑end.

Environment variables can be used to configure the database connection and
the trading symbol.  See below for defaults.
"""

# Configuration from environment variables with sensible defaults.
PGHOST = os.getenv("PGHOST", "postgres")
PGUSER = os.getenv("PGUSER", "postgres")
PGPASSWORD = os.getenv("PGPASSWORD", "postgres")
PGDATABASE = os.getenv("PGDATABASE", "crypto")
SYMBOL = os.getenv("SYMBOL", "BTC/USDT")
POLL_INTERVAL = float(os.getenv("POLL_INTERVAL", "15"))

# Prometheus metrics.
price_gauge = Gauge(
    "crypto_price",
    "Latest price for the configured symbol",
    ["symbol"],
)
api_latency = Gauge(
    "api_latency_seconds",
    "Latency of the API request in seconds",
)
collector_errors = Counter(
    "collector_errors_total",
    "Number of errors encountered by the collector",
)
last_update_timestamp = Gauge(
    "last_update_timestamp",
    "Timestamp of the last successful update (Unix time)",
)


def get_db_connection():
    """Create a new PostgreSQL connection."""
    return psycopg2.connect(
        host=PGHOST,
        user=PGUSER,
        password=PGPASSWORD,
        dbname=PGDATABASE,
    )


def fetch_price(exchange, symbol: str) -> float:
    """
    Fetch the latest ticker price for the provided symbol.

    Returns the price as a float.  If an exception occurs the error counter
    is incremented and None is returned.
    """
    try:
        start = time.time()
        ticker = exchange.fetch_ticker(symbol)
        latency = time.time() - start
        api_latency.set(latency)
        return float(ticker["last"])
    except Exception:
        collector_errors.inc()
        return None


def main():
    # Start the Prometheus metrics HTTP server.
    start_http_server(8000)

    # Initialise exchange.
    exchange = ccxt.binance()

    # Prepare database connection outside of the loop.
    conn = get_db_connection()
    cur = conn.cursor()

    # Infinite polling loop.
    while True:
        price = fetch_price(exchange, SYMBOL)
        if price is not None:
            # Record metric.
            price_gauge.labels(symbol=SYMBOL).set(price)
            last_update_timestamp.set_to_current_time()
            # Insert into database.
            try:
                cur.execute(
                    "INSERT INTO prices(symbol, price, ts) VALUES (%s, %s, NOW())",
                    (SYMBOL.replace("/", ""), price),
                )
                conn.commit()
            except Exception:
                collector_errors.inc()
                # Attempt to reconnect if the connection fails.
                conn.rollback()
                try:
                    conn.close()
                except Exception:
                    pass
                conn = get_db_connection()
                cur = conn.cursor()
        # Sleep until next poll.
        time.sleep(POLL_INTERVAL)


if __name__ == "__main__":
    main()
