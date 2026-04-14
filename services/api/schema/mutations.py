import strawberry
from schema.types import Player
from db.mongo import players as players_col
from db.redis import redis
import uuid

@strawberry.type
class Mutation:
    @strawberry.mutation
    async def create_player(self, username: str) -> Player:
        player_id = str(uuid.uuid4())
        doc = {"_id": player_id, "username": username, "total_score": 0}
        await players_col.insert_one(doc)
        # Cache in Redis
        await redis.hset(f"player:{player_id}", mapping={"username": username, "score": 0})
        await redis.expire(f"player:{player_id}", 3600)
        return Player(id=player_id, username=username, total_score=0)