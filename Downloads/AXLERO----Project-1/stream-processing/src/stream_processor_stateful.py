from confluent_kafka import Consumer, Producer
import json
import sqlite3
from datetime import datetime

# Create database
conn = sqlite3.connect("truck_state.db")
cursor = conn.cursor()

# Create table
cursor.execute('''
    CREATE TABLE IF NOT EXISTS truck_state (
        truck_id TEXT PRIMARY KEY,
        temperatures TEXT
    )
''')
conn.commit()

# Setup Kafka
consumer = Consumer({
    'bootstrap.servers': 'localhost:9092',
    'group.id': 'stateful-processor-group',
    'auto.offset.reset': 'earliest'
})

producer = Producer({'bootstrap.servers': 'localhost:9092'})
consumer.subscribe(['truck-telemetry'])

def load_state_from_disk():
    """Load old data from database"""
    temps = {}
    cursor.execute("SELECT truck_id, temperatures FROM truck_state")
    
    for row in cursor.fetchall():
        truck_id = row[0]
        temperatures = json.loads(row[1])
        temps[truck_id] = temperatures
    
    return temps

def save_state_to_disk(truck, temperatures):
    """Save data to database and Kafka"""
    # Save to database
    cursor.execute(
        "INSERT OR REPLACE INTO truck_state (truck_id, temperatures) VALUES (?, ?)",
        (truck, json.dumps(temperatures))
    )
    conn.commit()
    
    # Backup to Kafka
    value = json.dumps(temperatures).encode()
    producer.produce('truck-state-changelog', value=value)

print("=" * 60)
print("⚙️  WEEK 3 - STATEFUL PROCESSOR")
print("=" * 60)
print("\nLoading old data from disk...\n")

# Load on startup
temps = load_state_from_disk()
print(f"✅ Recovered {len(temps)} trucks from disk\n")

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
                # Get or create state
                if truck not in temps:
                    temps[truck] = []
                
                temps[truck].append(temp)
                
                # Keep only 300 readings
                if len(temps[truck]) > 300:
                    temps[truck].pop(0)
                
                # Calculate average
                avg = sum(temps[truck]) / len(temps[truck])
                
                # SAVE TO DISK
                save_state_to_disk(truck, temps[truck])
                
                # Send output
                output = {
                    'truck_id': truck,
                    'current_temp': round(temp, 2),
                    'rolling_avg_5min': round(avg, 2),
                    'reading_count': len(temps[truck]),
                    'timestamp': datetime.now().isoformat()
                }
                
                producer.produce('truck-averages', 
                               value=json.dumps(output).encode())
                
                print(f"✅ {truck}: {temp}°C | Avg: {avg:.2f}°C | Saved to disk")
        
        except Exception as e:
            pass

except KeyboardInterrupt:
    print("\n" + "=" * 60)
    print("Processor stopped")
    print("=" * 60)
    consumer.close()
    conn.close()