import os, asyncio, datetime
import asyncpg
from services.common.redis_streams import get_redis, NORM_STREAM, NORM_GROUP

PG_DSN = os.getenv("PG_DSN", "postgresql://crypto:crypto@postgres:5432/crypto")

async def ensure_schema(conn):
    sql_path = "/app/services/writer/schema.sql"
    with open(sql_path, "r", encoding="utf-8") as f:
        await conn.execute(f.read())

async def ensure_group(r):
    try:
        await r.xgroup_create(NORM_STREAM, NORM_GROUP, id="0-0", mkstream=True)
    except Exception:
        pass

async def main():
    r = get_redis()
    await ensure_group(r)
    conn = await asyncpg.connect(PG_DSN)
    await ensure_schema(conn)
    consumer = f"writer-{os.getpid()}"
    while True:
        resp = await r.xreadgroup(
            groupname=NORM_GROUP,
            consumername=consumer,
            streams={NORM_STREAM: ">"},
            count=500,
            block=1000
        )
        if not resp:
            continue

        rows = []
        ids = []
        for _stream, messages in resp:
            for msg_id, f in messages:
                ids.append(msg_id)
                ts = datetime.datetime.fromtimestamp(float(f["ts"]), tz=datetime.timezone.utc)
                rows.append((
                    ts, f["exchange"], f["symbol"], float(f["price"]),
                    f.get("delta_10s"), f.get("pct_10s"),
                    f.get("delta_1m"), f.get("pct_1m"),
                ))

        if rows:
            await conn.executemany(
                """INSERT INTO ticks(ts, exchange, symbol, price, delta_10s, pct_10s, delta_1m, pct_1m)
                   VALUES($1,$2,$3,$4,$5,$6,$7,$8)""",
                rows
            )
            await r.xack(NORM_STREAM, NORM_GROUP, *ids)

if __name__ == "__main__":
    asyncio.run(main())
