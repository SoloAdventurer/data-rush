from db.redis import redis
import uuid

async def create_room(host_player_id: str) -> str:
    room_id = str(uuid.uuid4())[:8]
    await redis.hset(f"room:{room_id}", mapping={
        "host": host_player_id,
        "status": "waiting",
        "player_count": 1
    })
    await redis.expire(f"room:{room_id}", 7200)
    return room_id

async def join_room(room_id: str, player_id: str) -> bool:
    key = f"room:{room_id}"
    exists = await redis.exists(key)
    if not exists:
        return False
    await redis.hincrby(key, "player_count", 1)
    await redis.sadd(f"room:{room_id}:players", player_id)
    return True