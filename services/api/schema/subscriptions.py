import strawberry
import asyncio
from typing import AsyncGenerator
from db.redis import redis

@strawberry.type
class Subscription:
    @strawberry.subscription
    async def leaderboard_updated(self) -> AsyncGenerator[str, None]:
        pubsub = redis.pubsub()
        await pubsub.subscribe("leaderboard:updates")
        try:
            async for message in pubsub.listen():
                if message["type"] == "message":
                    yield message["data"]
        finally:
            await pubsub.unsubscribe("leaderboard:updates")