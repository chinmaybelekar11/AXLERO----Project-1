from confluent_kafka import Consumer, Producer
import json
from datetime import datetime

# Consumer config
consumer = Consumer({
    'bootstrap.servers': 'localhost:9092',
    'group.id': 'processor-group',
    'auto.offset.reset': 'earliest'
})

# Producer config
producer = Producer({'bootstrap.servers': 'localhost:9092'})

consumer.subscribe(['truck-telemetry'])

temps = {}

print("🎯 Stream Processor Started...")
print("Processing truck-telemetry data...\n")

try:
    while True:
        msg = consumer.poll(timeout=1.0)
        
        if msg is None:
            continue
        
        if msg.error():
            print(f"Error: {msg.error()}")
            continue
        
        try:
            data = json.loads(msg.value().decode('utf-8'))
            truck = data['truck_id']
            temp = float(data['temperature'])
            
            # Filter: only temps > 60
            if temp > 60:
                # Store temps
                if truck not in temps:
                    temps[truck] = []
                
                temps[truck].append(temp)
                
                # Keep last 300 readings
                if len(temps[truck]) > 300:
                    temps[truck].pop(0)
                
                # Calculate rolling average
                avg = sum(temps[truck]) / len(temps[truck])
                
                # Create output
                output = {
                    'truck_id': truck,
                    'current_temp': round(temp, 2),
                    'rolling_avg_5min': round(avg, 2),
                    'reading_count': len(temps[truck]),
                    'timestamp': datetime.now().isoformat()
                }
                
                # Send to output topic
                producer.produce('truck-averages', 
                               value=json.dumps(output).encode('utf-8'))
                producer.flush()
                
                print(f"✅ {truck}: {temp}°C | Avg: {avg:.2f}°C | Count: {len(temps[truck])}")
        
        except Exception as e:
            print(f"Error processing: {e}")

except KeyboardInterrupt:
    print("\n✋ Processor stopped")

finally:
    consumer.close()