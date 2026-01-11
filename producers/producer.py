from kafka import KafkaProducer
import json, random, time
from datetime import datetime, timezone

producer = KafkaProducer(
    bootstrap_servers="localhost:9092",
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

while True:
    event = {
        "event_time": datetime.now(timezone.utc).isoformat(),
        "player_id": f"player_{random.randint(1,1000)}",
        "match_id": f"match_{random.randint(1,50)}",
        "event_type": random.choice(["join","leave","kill","purchase"]),
        "map": random.choice(["arena","desert","city"]),
        "player_level": random.randint(1,50),
        "region": random.choice(["EU","MENA","ASIA"])
    }

    producer.send("game-events", event)
    print("Sent:", event)
    time.sleep(1)
