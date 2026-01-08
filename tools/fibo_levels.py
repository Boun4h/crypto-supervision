import os
import psycopg2
from dataclasses import dataclass
from typing import Dict

# Ratios for Fibonacci retracement (0 to 1)
RATIOS = {
    "0.0%": 0.0,
    "23.6%": 0.236,
    "38.2%": 0.382,
    "50.0%": 0.5,
    "61.8%": 0.618,
    "78.6%": 0.786,
    "100.0%": 1.0,
}

@dataclass
class Swing:
    low: float
    high: float

def get_db_connection():
    """Return a psycopg2 connection using environment variables."""
    conn = psycopg2.connect(
        host=os.getenv("DB_HOST", os.getenv("PGHOST", "localhost")),
        port=int(os.getenv("DB_PORT", os.getenv("PGPORT", "5432"))),
        dbname=os.getenv("DB_NAME", os.getenv("PGDATABASE", "crypto")),
        user=os.getenv("DB_USER", os.getenv("PGUSER", "crypto")),
        password=os.getenv("DB_PASSWORD", os.getenv("PGPASSWORD", "crypto")),
    )
    return conn

def get_swing(symbol: str, lookback_hours: int) -> Swing:
    """
    Compute the lowest and highest price for a symbol over a lookback window.

    Args:
        symbol (str): Trading pair like "BTC/USDT".
        lookback_hours (int): How many hours back to look.

    Returns:
        Swing: dataclass with low and high prices.
    """
    conn = get_db_connection()
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT
                    MIN(last)::float8 AS low,
                    MAX(last)::float8 AS high
                FROM market_ticks
                WHERE symbol = %s
                  AND ts_utc >= NOW() - (%s || ' hours')::interval;
                """,
                (symbol, lookback_hours),
            )
            row = cur.fetchone()
            if not row or row[0] is None or row[1] is None:
                raise RuntimeError(
                    f"No data found for symbol={symbol} in the last {lookback_hours}h"
                )
            return Swing(low=row[0], high=row[1])
    finally:
        conn.close()

def fib_levels(swing: Swing) -> Dict[str, float]:
    """
    Compute Fibonacci retracement levels from a swing.

    Args:
        swing (Swing): low and high.

    Returns:
        dict: mapping ratio label to price level.
    """
    low, high = swing.low, swing.high
    diff = high - low
    return {label: high - diff * ratio for label, ratio in RATIOS.items()}

def main():
    symbol = os.getenv("SYMBOL", "BTC/USDT")
    lookback = int(os.getenv("LOOKBACK_HOURS", "24"))
    swing = get_swing(symbol, lookback)
    levels = fib_levels(swing)
    print(f"Symbol: {symbol}")
    print(f"Lookback: {lookback}h")
    print(f"Low: {swing.low:.2f}  High: {swing.high:.2f}")
    for label, value in sorted(levels.items(), key=lambda x: RATIOS[x[0]]):
        print(f"{label:<6}: {value:.2f}")

if __name__ == "__main__":
    main()
