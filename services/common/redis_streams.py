import os
import redis.asyncio as redis


def get_redis():
    return redis.from_url(os.getenv("REDIS_URL", "redis://redis:6379/0"), decode_responses=True)


RAW_STREAM = os.getenv("RAW_STREAM", "ticks_raw")
NORM_STREAM = os.getenv("NORM_STREAM", "ticks_norm")
RAW_GROUP = os.getenv("RAW_GROUP", "raw_consumers")
NORM_GROUP = os.getenv("NORM_GROUP", "norm_consumers")
