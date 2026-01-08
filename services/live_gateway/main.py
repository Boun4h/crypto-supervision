import os, asyncio
from fastapi import FastAPI, WebSocket, WebSocketDisconnect
import uvicorn
from services.common.redis_streams import get_redis, NORM_STREAM, NORM_GROUP

app = FastAPI()

async def ensure_group(r):
    try:
        await r.xgroup_create(NORM_STREAM, NORM_GROUP, id="0-0", mkstream=True)
    except Exception:
        pass

@app.on_event("startup")
async def startup():
    r = get_redis()
    await ensure_group(r)

@app.websocket("/ws")
async def ws_endpoint(ws: WebSocket):
    await ws.accept()
    r = get_redis()
    consumer = f"gw-{os.getpid()}"

    try:
        while True:
            resp = await r.xreadgroup(
                groupname=NORM_GROUP,
                consumername=consumer,
                streams={NORM_STREAM: ">"},
                count=200,
                block=1000
            )
            if not resp:
                continue

            # push batch
            batch = []
            ids = []
            for _stream, messages in resp:
                for msg_id, fields in messages:
                    batch.append(fields)
                    ids.append(msg_id)

            if batch:
                await ws.send_json(batch)
                # ack
                await r.xack(NORM_STREAM, NORM_GROUP, *ids)

    except WebSocketDisconnect:
        return
    except Exception:
        return

if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8000)
