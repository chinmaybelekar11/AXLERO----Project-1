from confluent_kafka import Producer
import json
import time
from datetime import datetime

producer = Producer({
    "bootstrap.servers": "localhost:9092"
})

topic = "truck-telemetry"

for i in range(100):
    data = {
        "truck_id": f"TRUCK-{i % 10 + 1:03d}",
        "temperature": 20 + (i % 15),
        "timestamp": datetime.now().isoformat()
    }

    producer.produce(
        topic,
        key=data["truck_id"],
        value=json.dumps(data)
    )

producer.flush()

print("100 events sent successfully!")