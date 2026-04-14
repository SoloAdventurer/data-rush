import redis.asyncio as aioredis
import os

redis = aioredis.from_url(os.getenv("REDIS_URL"), decode_responses=True)
