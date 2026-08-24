# Architecture - Week 1 & 2

## System Overview

Trucks (50)
↓
truck_producer.py

1 message/truck/second
60-100°C temperature
Total: 50 msg/sec
↓
Redpanda/Kafka
Topic: truck-telemetry
3 partitions
↓
stream_processor.py
Filter: temp > 60°C
Window: 300 readings
Calculate: rolling average
↓
Redpanda/Kafka
Topic: truck-averages
↓
Backend Team → Frontend Team → Dashboard

## Components

### Producer (truck_producer.py)
- Creates 50 truck IDs (TRUCK_001 to TRUCK_050)
- Generates random temperatures (60-100°C)
- Sends to Kafka every 1 second
- Runs continuously

### Stream Processor (stream_processor.py)
- Consumer: Reads from truck-telemetry
- Filter: Only keeps temps > 60°C
- State Store: In-memory dictionary
- Calculator: Computes rolling average
- Producer: Sends to truck-averages

### Testing (verify_math.py)
- Tests rolling average calculation
- Tests window size (300 readings)
- Tests temperature filter (>60°C)

## Data Processing Flow

Message arrives at truck-telemetry:
↓
Parse JSON
↓
Check temperature > 60?
NO → Discard message
YES → Continue
↓
Retrieve truck's history (from state store)
↓
Add new temperature to history
↓
Keep only last 300 readings
↓
Calculate rolling average
↓
Send to truck-averages topic


## Performance (Current)

| Metric | Value |
|--------|-------|
| Throughput | ~50 msg/sec (producer) |
| Processing | Real-time |
| Latency | <100ms |
| Data Loss | 0 messages |
| Trucks | 50 concurrent |
| Window | 300 readings (5 minutes) |

## Topics in Kafka

### truck-telemetry (INPUT)
- Receives raw truck data from producer
- 3 partitions (for parallel processing)
- 50 messages/second continuously

**Message Format**:
```json
{
  "truck_id": "TRUCK_001",
  "temperature": 85.5,
  "timestamp": "2026-08-22T21:00:00"
}
```

### truck-averages (OUTPUT)
- Stores processed results
- 1 partition
- Ready for backend consumption

**Message Format**:
```json
{
  "truck_id": "TRUCK_001",
  "current_temp": 85.5,
  "rolling_avg_5min": 86.2,
  "reading_count": 300,
  "timestamp": "2026-08-22T21:00:10"
}
```

## How Data Flows Through System

**Time: 21:00:00**

TRUCK_001 sends: temp=85.5°C
↓
Kafka receives in truck-telemetry
↓
Processor reads message
↓
Filter check: 85.5 > 60? YES ✅
↓
Store reading: TRUCK_001 = [85.5]
↓
Calculate average: (85.5) / 1 = 85.5°C
↓
Send to truck-averages:
{
"truck_id": "TRUCK_001",
"current_temp": 85.5,
"rolling_avg_5min": 85.5,
"reading_count": 1
}


## Week 1 vs Week 2

### Week 1: Kafka Setup
- Set up Redpanda (Kafka)
- Created topics
- Built producer
- Verified data flow

### Week 2: Stream Processing
- Built stream processor
- Implemented filter logic
- Implemented window logic
- Implemented calculation logic
- Verified math with tests

