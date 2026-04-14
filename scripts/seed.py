import asyncio
from motor.motor_asyncio import AsyncIOMotorClient
import os
from dotenv import load_dotenv

load_dotenv()

questions = [
    {"question": "What does CPU stand for?", "answer": "Central Processing Unit", "category": "tech", "difficulty": "easy"},
    {"question": "What is the time complexity of binary search?", "answer": "O(log n)", "category": "cs", "difficulty": "medium"},
    {"question": "What year was Python created?", "answer": "1991", "category": "tech", "difficulty": "easy"},
    {"question": "What does ACID stand for in databases?", "answer": "Atomicity Consistency Isolation Durability", "category": "databases", "difficulty": "hard"},
    {"question": "What is a Kafka topic?", "answer": "A category or feed to which records are published", "category": "big data", "difficulty": "medium"},
]

async def seed():
    client = AsyncIOMotorClient(os.getenv("MONGO_URI", "mongodb://localhost:27017/datarush"))
    db = client.datarush
    await db.questions.delete_many({})
    await db.questions.insert_many(questions)
    print(f"✅ Seeded {len(questions)} questions")
    client.close()

asyncio.run(seed())