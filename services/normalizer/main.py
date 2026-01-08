import asyncio, time, os
from services.common.redis_streams import get_redis, RAW_STREAM, NORM_STREAM, RAW_GROUP
from services.common.symbol_map import load_symbol_map
from services.common.models import RawTick, NormTick
from services.common.timebuf import TimeBuffer

THROTTLE_MS = int(os.getenv("THROTTLE_MS", "250"))

def should_emit(last_emit_ts: float, ts: float) -> bool:
    return (ts - last_emit_ts) * 1000.0 >= THROTTLE_MS

async def ensure_group(r):
    try:
        await r.xgroup_create(RAW_STREAM, RAW_GROUP, id="0-0", mkstream=True)
    except Exception:
        pass

async def main():
    r = get_redis()
    await ensure_group(r)

    canonical_symbols, maps = load_symbol_map()
    # state by (exchange, canonical_symbol)
    buffers = {}
    last_emit = {}

    consumer = f"normalizer-{os.getpid()}"

    while True:
        resp = await r.xreadgroup(
            groupname=RAW_GROUP,
            consumername=consumer,
            streams={RAW_STREAM: ">"},
            count=200,
            block=1000
        )
        if not resp:
            continue

        for _stream, messages in resp:
            for msg_id, fields in messages:
                try:
                    raw = RawTick(**fields)
                    cmap = maps.get(raw.exchange, {})
                    symbol = cmap.get(raw.symbol_raw)
                    if not symbol:
                        await r.xack(RAW_STREAM, RAW_GROUP, msg_id)
                        continue

                    key = (raw.exchange, symbol)
                    buf = buffers.get(key)
                    if not buf:
                        buf = TimeBuffer()
                        buffers[key] = buf

                    buf.add(raw.ts, raw.price)

                    le = last_emit.get(key, 0.0)
                    if not should_emit(le, raw.ts):
                        await r.xack(RAW_STREAM, RAW_GROUP, msg_id)
                        continue

                    p10 = buf.get_price_ago(raw.ts, 10.0)
                    p60 = buf.get_price_ago(raw.ts, 60.0)

                    tick = NormTick(exchange=raw.exchange, symbol=symbol, ts=raw.ts, price=raw.price)

                    if p10 is not None and p10 != 0:
                        tick.delta_10s = raw.price - p10
                        tick.pct_10s = (tick.delta_10s / p10) * 100.0

                    if p60 is not None and p60 != 0:
                        tick.delta_1m = raw.price - p60
                        tick.pct_1m = (tick.delta_1m / p60) * 100.0

                    await r.xadd(NORM_STREAM, tick.model_dump())
                    last_emit[key] = raw.ts

                    await r.xack(RAW_STREAM, RAW_GROUP, msg_id)
                except Exception:
                    # ack anyway to avoid blocking the stream
                    await r.xack(RAW_STREAM, RAW_GROUP, msg_id)

if __name__ == "__main__":
    asyncio.run(main())
