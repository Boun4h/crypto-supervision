import asyncio, json, time, os
import websockets
from services.common.redis_streams import get_redis, RAW_STREAM
from services.common.models import RawTick

POLONIEX_WS = "wss://ws.poloniex.com/ws/public"  # à adapter si futures
POLONIEX_SYMBOLS = [s.strip() for s in os.getenv("POLONIEX_SYMBOLS","" ).split(",") if s.strip()]

async def main():
    r = get_redis()

    # Exemple de subscribe (à ajuster au protocole exact spot/futures)
    sub = {
        "event": "subscribe",
        "channel": ["tickers"],
        "symbols": POLONIEX_SYMBOLS
    }

    while True:
        try:
            async with websockets.connect(POLONIEX_WS, ping_interval=20, ping_timeout=20) as ws:
                await ws.send(json.dumps(sub))
                async for msg in ws:
                    data = json.loads(msg)
                    # Selon payload réel : extraire symbol + price
                    sym = data.get("symbol") or data.get("s")
                    price = data.get("price") or data.get("p")
                    if sym is None or price is None:
                        continue
                    tick = RawTick(exchange="poloniex", symbol_raw=str(sym), ts=time.time(), price=float(price))
                    await r.xadd(RAW_STREAM, tick.model_dump())
        except Exception:
            await asyncio.sleep(2)

if __name__ == "__main__":
    asyncio.run(main())
