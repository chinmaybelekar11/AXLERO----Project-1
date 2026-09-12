from confluent_kafka import Consumer, Producer
import json
import sqlite3
from datetime import datetime
from prometheus_client import Counter, Gauge, start_http_server
import time

# Start Prometheus HTTP server on port 8000
start_http_server(8000)

# Define metrics
messages_processed = Counter(
    'messages_processed_total',
    'Total messages processed'
)

messages_per_second = Gauge(
    'messages_per_second',
    'Messages processed per second'
)

trucks_tracked = Gauge(
    'trucks_tracked',
    'Number of trucks being tracked'
)

errors_total = Counter(
    'errors_total',
    'Total errors encountered'
)

state_saves = Counter(
    'state_saves_total',
    'Total times state saved to disk'
)

# Create database
conn = sqlite3.connect("truck_state.db")
cursor = conn.cursor()

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
    'group.id': 'metrics-processor-group',
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
    try:
        cursor.execute(
            "INSERT OR REPLACE INTO truck_state (truck_id, temperatures) VALUES (?, ?)",
            (truck, json.dumps(temperatures))
        )
        conn.commit()
        
        # Track metric
        state_saves.inc()
        
        # Backup to Kafka
        value = json.dumps(temperatures).encode()
        producer.produce('truck-state-changelog', value=value)
    
    except Exception as e:
        errors_total.inc()

print("=" * 60)
print("📊 WEEK 4 - PROCESSOR WITH PROMETHEUS METRICS")
print("=" * 60)
print("\nLoading old data from disk...")

# Load on startup
temps = load_state_from_disk()
print(f"✅ Recovered {len(temps)} trucks from disk\n")

print("📊 Prometheus metrics available at: http://localhost:8000/metrics")
print("Processing messages...\n")

# Track messages per second
start_time = time.time()
message_count = 0

try:
    while True:
        msg = consumer.poll(timeout=1.0)
        
        if msg is None:
            continue
        
        if msg.error():
            errors_total.inc()
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
                
                # Track metrics
                messages_processed.inc()
                trucks_tracked.set(len(temps))
                
                # Calculate messages per second
                message_count += 1
                elapsed = time.time() - start_time
                if elapsed > 0:
                    msg_per_sec = message_count / elapsed
                    messages_per_second.set(msg_per_sec)
                
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
            errors_total.inc()

except KeyboardInterrupt:
    print("\n" + "=" * 60)
    print("Processor stopped")
    print("=" * 60)
    consumer.close()
    conn.close()