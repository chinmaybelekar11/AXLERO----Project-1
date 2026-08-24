from kafka import KafkaProducer
import json
import random
import time
from datetime import datetime

# Create producer
producer = KafkaProducer(
    bootstrap_servers=['localhost:9092'],
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

TRUCK_IDS = [f"TRUCK_{i:03d}" for i in range(1, 51)]

if __name__ == "__main__":
    print("🚚 Starting Truck Telemetry Producer...")
    print("Sending data every 1 second...\n")
    
    try:
        while True:
            truck_id = random.choice(TRUCK_IDS)
            temperature = random.uniform(60, 100)
            
            message = {
                'truck_id': truck_id,
                'temperature': round(temperature, 2),
                'timestamp': datetime.now().isoformat()
            }
            
            producer.send('truck-telemetry', value=message)
            print(f"✅ {truck_id}: {temperature:.2f}°C")
            time.sleep(1)
            
    except KeyboardInterrupt:
        print("\n✋ Producer stopped")
        producer.close()