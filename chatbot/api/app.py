import os
import re
from datetime import datetime, timezone, timedelta
from typing import Optional, Dict, Any

import psycopg2
from fastapi import FastAPI
from pydantic import BaseModel

DB_HOST = os.getenv("DB_HOST", "postgres")
DB_PORT = int(os.getenv("DB_PORT", "5432"))
DB_NAME = os.getenv("DB_NAME", "crypto")
DB_USER = os.getenv("DB_USER", "crypto")
DB_PASSWORD = os.getenv("DB_PASSWORD", "crypto")

DEFAULT_SYMBOL = os.getenv("DEFAULT_SYMBOL", "BTC/USDT")
DEFAULT_LOOKBACK_HOURS = int(os.getenv("DEFAULT_LOOKBACK_HOURS", "6"))

app = FastAPI(title="Crypto Chat API", version="1.0.0")


class ChatRequest(BaseModel):
    message: str
    symbol: Optional[str] = None
    lookback_hours: Optional[int] = None


def get_conn():
    return psycopg2.connect(
        host=DB_HOST, port=DB_PORT, dbname=DB_NAME, user=DB_USER, password=DB_PASSWORD
    )


def fib_levels(low: float, high: float) -> Dict[str, float]:
    # Retracements from high down to low (common approach after impulse up)
    return {
        "0.0%": high,
        "23.6%": high - (high - low) * 0.236,
        "38.2%": high - (high - low) * 0.382,
        "50.0%": high - (high - low) * 0.5,
        "61.8%": high - (high - low) * 0.618,
        "78.6%": high - (high - low) * 0.786,
        "100.0%": low,
    }


def parse_lookback(msg: str) -> Optional[int]:
    # accepts "6h", "24h", "6 hours", "24 hours"
    m = re.search(r"(\d+)\s*(h|hours?)\b", msg.lower())
    if m:
        return int(m.group(1))
    return None


def parse_symbol(msg: str) -> Optional[str]:
    # very simple: find patterns like BTC/USDT
    m = re.search(r"\b([A-Z0-9]{2,10}/[A-Z0-9]{2,10})\b", msg.upper())
    if m:
        return m.group(1)
    return None


def fetch_last_price(symbol: str) -> Optional[float]:
    sql = """
      SELECT last::float8
      FROM market_ticks
      WHERE symbol = %s
      ORDER BY ts_utc DESC
      LIMIT 1
    """
    with get_conn() as conn, conn.cursor() as cur:
        cur.execute(sql, (symbol,))
        row = cur.fetchone()
        return float(row[0]) if row else None


def fetch_swing(symbol: str, lookback_hours: int) -> Optional[Dict[str, Any]]:
    sql = """
      SELECT MIN(last::float8) AS low,
             MAX(last::float8) AS high,
             COUNT(*)::int     AS n
      FROM market_ticks
      WHERE symbol = %s
        AND ts_utc >= NOW() - (%s || ' hours')::interval
    """
    with get_conn() as conn, conn.cursor() as cur:
        cur.execute(sql, (symbol, lookback_hours))
        row = cur.fetchone()
        if not row or row[2] == 0 or row[0] is None or row[1] is None:
            return None
        return {"low": float(row[0]), "high": float(row[1]), "n": int(row[2])}


@app.get("/health")
def health():
    return {"ok": True}


@app.post("/chat")
def chat(req: ChatRequest):
    msg = req.message.strip()
    symbol = req.symbol or parse_symbol(msg) or DEFAULT_SYMBOL
    lookback = req.lookback_hours or parse_lookback(msg) or DEFAULT_LOOKBACK_HOURS

    msg_l = msg.lower()

    # intents
    if "help" in msg_l or msg_l in {"?", "aide"}:
        return {
            "reply": (
                "Commandes:\n"
                "- 'price BTC/USDT'\n"
                "- 'fib 6h BTC/USDT' ou 'fib 24h'\n"
                "- 'status'\n"
            )
        }

    if "status" in msg_l:
        last = fetch_last_price(symbol)
        return {"reply": f"Status OK. Symbol={symbol}. Dernier prix: {last}"}

    if "price" in msg_l or "prix" in msg_l:
        last = fetch_last_price(symbol)
        if last is None:
            return {"reply": f"Pas de donnée pour {symbol}."}
        return {"reply": f"Dernier prix {symbol}: {last:.6f}"}

    if "fib" in msg_l or "fibo" in msg_l or "fibonacci" in msg_l:
        swing = fetch_swing(symbol, lookback)
        if not swing:
            return {"reply": f"Pas assez de données pour {symbol} sur {lookback}h."}
        levels = fib_levels(swing["low"], swing["high"])
        lines = [f"{k}: {v:.6f}" for k, v in levels.items()]
        return {
            "reply": (
                f"Fib {symbol} sur {lookback}h (n={swing['n']})\n"
                f"Low={swing['low']:.6f} High={swing['high']:.6f}\n" +
                "\n".join(lines)
            )
        }

    # default
    last = fetch_last_price(symbol)
    if last is None:
        return {"reply": f"Je n'ai pas compris. Essaie 'help'. (Pas de donnée pour {symbol})"}
    return {"reply": f"Je peux donner 'price' ou 'fib'. Dernier prix {symbol}: {last:.6f}"}
