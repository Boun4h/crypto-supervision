import asyncio
import json
import time
import os
import websockets
from services.common.redis_streams import get_redis, RAW_STREAM
from services.common.models import RawTick

BINANCE_WS = "wss://stream.binance.com:9443/ws/!miniTicker@arr"

async def main():
    r = get_redis()
    while True:
        try:
            async with websockets.connect(BINANCE_WS, ping_interval=20, ping_timeout=20) as ws:
                async for msg in ws:
                    data = json.loads(msg)  # list of tickers
                    ts = time.time()
                    pipe = r.pipeline()
                    for t in data:
                        sym = t.get("s")  # e.g. BTCUSDT
                        price = float(t.get("c"))
                        tick = RawTick(exchange="binance", symbol_raw=sym, ts=ts, price=price)
                        pipe.xadd(RAW_STREAM, tick.model_dump())
                    await pipe.execute()
        except Exception as e:
            # backoff
            await asyncio.sleep(2)

if __name__ == "__main__":
    asyncio.run(main())
