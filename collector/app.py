import os
import time
import json
import signal
import logging
from datetime import datetime, timezone
from typing import Dict, Any, List, Tuple

import ccxt
import psycopg2
from psycopg2.extras import execute_values
from prometheus_client import start_http_server, Gauge, Counter, Histogram

# ----------------------------
# Logging
# ----------------------------
logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO"),
    format="%(asctime)s %(levelname)s %(message)s",
)
log = logging.getLogger("collector")

# ----------------------------
# Prometheus metrics
# ----------------------------
SCRAPE_LATENCY = Histogram(
    "collector_scrape_latency_seconds",
    "Time spent fetching market data from exchanges",
    ["exchange"],
)
DB_WRITE_LATENCY = Histogram(
    "collector_db_write_latency_seconds",
    "Time spent writing data to PostgreSQL",
)
API_ERRORS = Counter(
    "collector_api_errors_total",
    "Total number of API errors",
    ["exchange", "symbol", "error_type"],
)
DB_ERRORS = Counter(
    "collector_db_errors_total",
    "Total number of DB errors",
    ["error_type"],
)
LAST_SUCCESS_TS = Gauge(
    "collector_last_success_timestamp",
    "Unix timestamp of last successful full iteration",
)
LAST_SYMBOL_TS = Gauge(
    "collector_last_symbol_timestamp",
    "Unix timestamp when symbol was last updated",
    ["exchange", "symbol"],
)
PRICE_LAST = Gauge("crypto_price_last", "Last traded price", ["exchange", "symbol"])
PRICE_BID = Gauge("crypto_price_bid", "Best bid price", ["exchange", "symbol"])
PRICE_ASK = Gauge("crypto_price_ask", "Best ask price", ["exchange", "symbol"])
SPREAD_ABS = Gauge("crypto_spread_abs", "Absolute spread (ask-bid)", ["exchange", "symbol"])
SPREAD_PCT = Gauge("crypto_spread_pct", "Spread percentage (ask-bid)/mid", ["exchange", "symbol"])


# ----------------------------
# Config
# ----------------------------
def getenv_required(name: str) -> str:
    v = os.getenv(name)
    if not v:
        raise RuntimeError(f"Missing required env var: {name}")
    return v


EXCHANGES = [x.strip().lower() for x in os.getenv("EXCHANGES", "binance").split(",") if x.strip()]
SYMBOLS = [s.strip().upper() for s in os.getenv("SYMBOLS", "BTC/USDT,ETH/USDT").split(",") if s.strip()]
POLL_INTERVAL_SECONDS = float(os.getenv("POLL_INTERVAL_SECONDS", "15"))

# CCXT options
ENABLE_RATE_LIMIT = os.getenv("CCXT_ENABLE_RATE_LIMIT", "true").lower() == "true"
REQUEST_TIMEOUT_MS = int(os.getenv("CCXT_TIMEOUT_MS", "10000"))

# DB
DB_HOST = getenv_required("DB_HOST")
DB_PORT = int(os.getenv("DB_PORT", "5432"))
DB_NAME = getenv_required("DB_NAME")
DB_USER = getenv_required("DB_USER")
DB_PASSWORD = getenv_required("DB_PASSWORD")

# Metrics server
METRICS_PORT = int(os.getenv("METRICS_PORT", "8000"))

# Graceful stop
RUNNING = True


def handle_stop(sig, frame):
    global RUNNING
    RUNNING = False
    log.warning("Received stop signal, shutting down...")


signal.signal(signal.SIGINT, handle_stop)
signal.signal(signal.SIGTERM, handle_stop)


# ----------------------------
# DB helpers
# ----------------------------
def db_connect():
    return psycopg2.connect(
        host=DB_HOST,
        port=DB_PORT,
        dbname=DB_NAME,
        user=DB_USER,
        password=DB_PASSWORD,
        connect_timeout=10,
        application_name="crypto-collector",
    )


def db_init_check(conn):
    with conn.cursor() as cur:
        cur.execute("SELECT 1;")
    conn.commit()


# ----------------------------
# Exchange helpers
# ----------------------------
def make_exchange(name: str):
    if not hasattr(ccxt, name):
        raise ValueError(f"Unknown exchange in ccxt: {name}")
    klass = getattr(ccxt, name)
    ex = klass(
        {
            "enableRateLimit": ENABLE_RATE_LIMIT,
            "timeout": REQUEST_TIMEOUT_MS,
        }
    )
    return ex


def safe_float(x) -> float:
    try:
        if x is None:
            return float("nan")
        return float(x)
    except Exception:
        return float("nan")


def compute_spread(bid: float, ask: float) -> Tuple[float, float]:
    if bid != bid or ask != ask:  # NaN check
        return float("nan"), float("nan")
    spread_abs = ask - bid
    mid = (ask + bid) / 2.0 if (ask + bid) != 0 else float("nan")
    spread_pct = (spread_abs / mid) if mid == mid and mid != 0 else float("nan")
    return spread_abs, spread_pct


# ----------------------------
# Main loop
# ----------------------------
def fetch_all(exchanges: Dict[str, Any], symbols: List[str]) -> List[Dict[str, Any]]:
    """
    Returns list of rows:
    {
      exchange, symbol, ts_utc, last, bid, ask, spread_abs, spread_pct, raw_json
    }
    """
    rows = []
    now = datetime.now(timezone.utc)

    for ex_name, ex in exchanges.items():
        with SCRAPE_LATENCY.labels(exchange=ex_name).time():
            # Some exchanges require markets loaded for symbol validation
            try:
                ex.load_markets()
            except Exception as e:
                API_ERRORS.labels(exchange=ex_name, symbol="*", error_type="load_markets").inc()
                log.warning("load_markets failed for %s: %s", ex_name, e)

            for sym in symbols:
                try:
                    ticker = ex.fetch_ticker(sym)
                    last = safe_float(ticker.get("last"))
                    bid = safe_float(ticker.get("bid"))
                    ask = safe_float(ticker.get("ask"))
                    spread_abs, spread_pct = compute_spread(bid, ask)

                    # Metrics
                    if last == last:
                        PRICE_LAST.labels(exchange=ex_name, symbol=sym).set(last)
                    if bid == bid:
                        PRICE_BID.labels(exchange=ex_name, symbol=sym).set(bid)
                    if ask == ask:
                        PRICE_ASK.labels(exchange=ex_name, symbol=sym).set(ask)
                    if spread_abs == spread_abs:
                        SPREAD_ABS.labels(exchange=ex_name, symbol=sym).set(spread_abs)
                    if spread_pct == spread_pct:
                        SPREAD_PCT.labels(exchange=ex_name, symbol=sym).set(spread_pct)

                    ts_unix = int(now.timestamp())
                    LAST_SYMBOL_TS.labels(exchange=ex_name, symbol=sym).set(ts_unix)

                    rows.append(
                        {
                            "exchange": ex_name,
                            "symbol": sym,
                            "ts_utc": now,
                            "last": last,
                            "bid": bid,
                            "ask": ask,
                            "spread_abs": spread_abs,
                            "spread_pct": spread_pct,
                            "raw_json": json.dumps(ticker, default=str),
                        }
                    )
                except ccxt.RateLimitExceeded:
                    API_ERRORS.labels(exchange=ex_name, symbol=sym, error_type="rate_limit").inc()
                    log.warning("[%s] rate limit exceeded on %s", ex_name, sym)
                except ccxt.NetworkError as e:
                    API_ERRORS.labels(exchange=ex_name, symbol=sym, error_type="network").inc()
                    log.warning("[%s] network error on %s: %s", ex_name, sym, e)
                except ccxt.ExchangeError as e:
                    API_ERRORS.labels(exchange=ex_name, symbol=sym, error_type="exchange").inc()
                    log.warning("[%s] exchange error on %s: %s", ex_name, sym, e)
                except Exception as e:
                    API_ERRORS.labels(exchange=ex_name, symbol=sym, error_type="unknown").inc()
                    log.exception("[%s] unexpected error on %s: %s", ex_name, sym, e)

    return rows


def db_insert_rows(conn, rows: List[Dict[str, Any]]):
    if not rows:
        return
    values = [
        (
            r["exchange"],
            r["symbol"],
            r["ts_utc"],
            r["last"],
            r["bid"],
            r["ask"],
            r["spread_abs"],
            r["spread_pct"],
            r["raw_json"],
        )
        for r in rows
    ]
    query = """
        INSERT INTO market_ticks
            (exchange, symbol, ts_utc, last, bid, ask, spread_abs, spread_pct, raw_json)
        VALUES %s
    """
    with DB_WRITE_LATENCY.time():
        with conn.cursor() as cur:
            execute_values(cur, query, values, page_size=200)
        conn.commit()


def main():
    log.info("Starting crypto collector with exchanges=%s symbols=%s interval=%ss",
             EXCHANGES, SYMBOLS, POLL_INTERVAL_SECONDS)

    # Start metrics endpoint
    start_http_server(METRICS_PORT)
    log.info("Prometheus metrics exposed on :%d/metrics", METRICS_PORT)

    # Init exchanges
    exchanges = {}
    for name in EXCHANGES:
        try:
            exchanges[name] = make_exchange(name)
        except Exception as e:
            log.error("Failed to init exchange %s: %s", name, e)

    if not exchanges:
        raise RuntimeError("No valid exchanges initialized. Check EXCHANGES env var.")

    # DB connect (retry loop)
    conn = None
    while RUNNING and conn is None:
        try:
            conn = db_connect()
            db_init_check(conn)
            log.info("Connected to PostgreSQL at %s:%d/%s", DB_HOST, DB_PORT, DB_NAME)
        except Exception as e:
            DB_ERRORS.labels(error_type="connect").inc()
            log.warning("DB connection failed, retrying in 3s: %s", e)
            time.sleep(3)

    # Main loop
    while RUNNING:
        loop_start = time.time()
        try:
            rows = fetch_all(exchanges, SYMBOLS)
            db_insert_rows(conn, rows)
            LAST_SUCCESS_TS.set(int(time.time()))
            log.info("Iteration OK: inserted %d rows", len(rows))
        except Exception as e:
            DB_ERRORS.labels(error_type="write_or_loop").inc()
            log.exception("Loop error: %s", e)
            # if DB died, try reconnect
            try:
                conn.close()
            except Exception:
                pass
            conn = None
            while RUNNING and conn is None:
                try:
                    conn = db_connect()
                    db_init_check(conn)
                    log.info("Reconnected to DB.")
                except Exception as e2:
                    DB_ERRORS.labels(error_type="reconnect").inc()
                    log.warning("DB reconnect failed, retrying in 3s: %s", e2)
                    time.sleep(3)

        elapsed = time.time() - loop_start
        sleep_for = max(0.0, POLL_INTERVAL_SECONDS - elapsed)
        time.sleep(sleep_for)

    # Cleanup
    try:
        if conn:
            conn.close()
    except Exception:
        pass
    log.info("Collector stopped.")


if __name__ == "__main__":
    main()
