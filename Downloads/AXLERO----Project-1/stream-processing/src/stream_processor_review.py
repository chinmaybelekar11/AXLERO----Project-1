from confluent_kafka import Consumer, Producer
import json
from datetime import datetime
import time

consumer = Consumer({
    'bootstrap.servers': 'localhost:9092',
    'group.id': 'processor-review-group',
    'auto.offset.reset': 'earliest'
})

producer = Producer({'bootstrap.servers': 'localhost:9092'})

consumer.subscribe(['truck-telemetry'])

temps = {}
message_count = 0
start_time = time.time()

print("=" * 60)
print("🎯 MID-PROJECT REVIEW - SPEED TEST")
print("=" * 60)
print("\nTesting how fast the processor can handle data...\n")

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
            
            if temp > 60:
                if truck not in temps:
                    temps[truck] = []
                
                temps[truck].append(temp)
                
                if len(temps[truck]) > 300:
                    temps[truck].pop(0)
                
                avg = sum(temps[truck]) / len(temps[truck])
                
                output = {
                    'truck_id': truck,
                    'current_temp': round(temp, 2),
                    'rolling_avg_5min': round(avg, 2),
                    'reading_count': len(temps[truck]),
                    'timestamp': datetime.now().isoformat()
                }
                
                producer.produce('truck-averages', 
                               value=json.dumps(output).encode('utf-8'))
                
                message_count += 1
                
                if message_count % 1000 == 0:

                    elapsed = time.time() - start_time
                    if elapsed > 0:
                        speed = message_count / elapsed
                        print(f"✅ {message_count:,} messages processed")
                        print(f"   Speed: {speed:,.0f} messages/second")
                        print(f"   Time: {elapsed:.1f}s")
                        print()
        
        except Exception as e:
            pass

except KeyboardInterrupt:
    elapsed = time.time() - start_time
    if elapsed > 0:
        speed = message_count / elapsed
    else:
        speed = 0
    
    print("\n" + "=" * 60)
    print("📊 FINAL TEST RESULTS")
    print("=" * 60)
    print(f"✅ Total Messages: {message_count:,}")
    print(f"✅ Time Taken: {elapsed:.2f} seconds")
    print(f"✅ Speed: {speed:,.0f} messages/second")
    print(f"✅ Trucks Tracked: {len(temps)}")
    print("=" * 60)

finally:
    consumer.close()