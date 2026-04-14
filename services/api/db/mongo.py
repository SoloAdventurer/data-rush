from motor.motor_asyncio import AsyncIOMotorClient
import os

client = AsyncIOMotorClient(os.getenv("MONGO_URI"))
db = client.datarush

players = db["players"]
questions = db["questions"]
games = db["games"]
