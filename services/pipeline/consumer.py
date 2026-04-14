import asyncio, json, os
from aiokafka import AIOKafkaConsumer
from motor.motor_asyncio import AsyncIOMotorClient

client = AsyncIOMotorClient(os.getenv("MONGO_URI"))
analytics = client.datarush["analytics"]

async def consume():
    consumer = AIOKafkaConsumer(
        "game-events",
        bootstrap_servers=os.getenv("KAFKA_BROKER"),
        group_id="pipeline-group"
    )
    await consumer.start()
    try:
        async for msg in consumer:
            event = json.loads(msg.value)
            print(f"[pipeline] received: {event}")
            # Aggregate: upsert category stats per hour
            await analytics.update_one(
                {"player_id": event.get("player_id")},
                {"$inc": {"total_score": event.get("score_delta", 0),
                          "answers": 1}},
                upsert=True
            )
    finally:
        await consumer.stop()

if __name__ == "__main__":
    asyncio.run(consume())