import asyncio, json, time, os
import websockets
from services.common.redis_streams import get_redis, RAW_STREAM
from services.common.models import RawTick

KRAKEN_WS = "wss://ws.kraken.com"  # ou v2 selon ton choix
KRAKEN_SYMBOLS = [s.strip() for s in os.getenv("KRAKEN_SYMBOLS","" ).split(",") if s.strip()]

async def main():
    r = get_redis()
    sub = {
        "event": "subscribe",
        "pair": KRAKEN_SYMBOLS,
        "subscription": {"name": "ticker"}
    }

    while True:
        try:
            async with websockets.connect(KRAKEN_WS, ping_interval=20, ping_timeout=20) as ws:
                await ws.send(json.dumps(sub))
                async for msg in ws:
                    data = json.loads(msg)
                    # Kraken envoie aussi des "event": "heartbeat"/"subscriptionStatus"
                    if isinstance(data, dict):
                        continue
                    # Format typique v1: [channelId, {...}, "ticker", "XBT/USD"]
                    if isinstance(data, list) and len(data) >= 4:
                        pair = data[-1]
                        payload = data[1]
                        # last trade close price c[0] souvent
                        c = payload.get("c")
                        if not c:
                            continue
                        price = float(c[0])
                        tick = RawTick(exchange="kraken", symbol_raw=pair, ts=time.time(), price=price)
                        await r.xadd(RAW_STREAM, tick.model_dump())
        except Exception:
            await asyncio.sleep(2)

if __name__ == "__main__":
    asyncio.run(main())
