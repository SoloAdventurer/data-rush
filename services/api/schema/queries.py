import strawberry
from typing import List
from schema.types import LeaderboardEntry, Player
from game.leaderboard import get_top
from db.mongo import players as players_col

@strawberry.type
class Query:
    @strawberry.field
    async def leaderboard(self, top: int = 10) -> List[LeaderboardEntry]:
        entries = await get_top(top)
        return [
            LeaderboardEntry(player_id=pid, score=score, rank=i+1)
            for i, (pid, score) in enumerate(entries)
        ]

    @strawberry.field
    async def player(self, player_id: str) -> Player:
        doc = await players_col.find_one({"_id": player_id})
        if not doc:
            raise ValueError("Player not found")
        return Player(id=doc["_id"], username=doc["username"], total_score=doc.get("total_score", 0))