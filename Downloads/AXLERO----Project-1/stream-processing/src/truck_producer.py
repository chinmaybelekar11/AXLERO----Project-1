from kafka import KafkaProducer
import json
import time
import random

producer = KafkaProducer(
    bootstrap_servers=['localhost:9092'],
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

print("=" * 60)
print("🚚 TRUCK PRODUCER - STARTED")
print("=" * 60)
print("\nGenerating data from 50 trucks...\n")

try:
    while True:
        for truck_id in range(1, 51):
            truck_name = f"TRUCK_{truck_id:03d}"
            temperature = round(random.uniform(60, 100), 1)
            timestamp = time.strftime('%Y-%m-%dT%H:%M:%S')
            
            message = {
                'truck_id': truck_name,
                'temperature': temperature,
                'timestamp': timestamp
            }
            
            producer.send('truck-telemetry', value=message)
            print(f"Sent: {truck_name} = {temperature}°C")
        
        time.sleep(1)


except KeyboardInterrupt:
    print("\n" + "=" * 60)
    print("Producer stopped")
    print("=" * 60)
    producer.close()