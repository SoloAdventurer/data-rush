import strawberry
from typing import Optional

@strawberry.type 
class Player:
    id: str
    username: str
    total_score: int

@strawberry.type
class LeaderboardEntry:
    player_id: str
    rank: int
    score: int

@strawberry.type
class Leaderboard:
    entries: list[LeaderboardEntry]
    total_players: int

@strawberry.type
class GameRoom: 
    room_id: str
    status: str
    player_count: int
