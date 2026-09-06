from confluent_kafka import Consumer, Producer
import json
from datetime import datetime
import time

consumer = Consumer({
    'bootstrap.servers': 'localhost:9092',
    'group.id': 'processor-group',
    'auto.offset.reset': 'earliest'
})

producer = Producer({'bootstrap.servers': 'localhost:9092'})

consumer.subscribe(['truck-telemetry'])

temps = {}

print("=" * 60)
print("⚙️  STREAM PROCESSOR - STARTED")
print("=" * 60)
print("\nProcessing truck telemetry data...\n")

try:
    while True:
        msg = consumer.poll(timeout=1.0)
        
        if msg is None:
            continue
        
        if msg.error():
            continue
        

        try:
            data = json.loads(msg.value().decode('utf-8'))
            truck = data['truck_id']
            temp = float(data['temperature'])
            
            # Filter: only temps > 60
            if temp > 60:
                # Window: keep 300 readings
                if truck not in temps:
                    temps[truck] = []
                
                temps[truck].append(temp)
                
                if len(temps[truck]) > 300:
                    temps[truck].pop(0)
                
                # Calculate rolling average
                avg = sum(temps[truck]) / len(temps[truck])
                
                output = {
                    'truck_id': truck,
                    'current_temp': round(temp, 2),
                    'rolling_avg_5min': round(avg, 2),
                    'reading_count': len(temps[truck]),
                    'timestamp': datetime.now().isoformat()
                }
                
                producer.produce('truck-averages', 
                               value=json.dumps(output).encode())
                
                print(f"✅ {truck}: {temp}°C | Avg: {avg:.2f}°C")
        

        except Exception as e:
            pass

except KeyboardInterrupt:
    print("\n" + "=" * 60)
    print("Processor stopped")
    print("=" * 60)
    consumer.close()