import asyncio, json, os
from aiokafka import AIOKafkaProducer
from datetime import datetime

TOPIC = "game-events"

async def emit_event(event: dict):
    producer = AIOKafkaProducer(bootstrap_servers=os.getenv("KAFKA_BROKER"))
    await producer.start()
    try:
        payload = json.dumps({**event, "timestamp": datetime.utcnow().isoformat()})
        await producer.send_and_wait(TOPIC, payload.encode())
    finally:
        await producer.stop()

# Example: emit answer event
if __name__ == "__main__":
    asyncio.run(emit_event({
        "type": "answer",
        "player_id": "player_1",
        "question_id": "q_42",
        "correct": True,
        "response_time_ms": 1200,
        "score_delta": 100
    }))