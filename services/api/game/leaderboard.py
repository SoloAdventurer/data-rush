from db.redis import redis

LEADERBOARD_KEY = "leaderboard:global"

async def add_score(player_id: str, score: float):
    await redis.zadd(LEADERBOARD_KEY, {player_id: score})

async def get_top(n: int = 10):
    return await redis.zrevrange(LEADERBOARD_KEY, 0, n - 1, withscores=True)

async def get_rank(player_id: str):
    return await redis.zrevrank(LEADERBOARD_KEY, player_id)