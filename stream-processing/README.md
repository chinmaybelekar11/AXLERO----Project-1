# Stream Processing Module - Week 1 & 2

## Status
- ✅ Week 1: Kafka Setup - COMPLETE
- ✅ Week 2: Stream Processing - COMPLETE


## What's Inside

### Week 1: Kafka Setup
- **truck_producer.py**: Generates IoT data from 50 trucks
  - Temperature: 60-100°C
  - Rate: 1 message/second per truck
  - Total: 50 messages/second

### Week 2: Stream Processing
- **stream_processor.py**: Main stream processor
  - Reads from: truck-telemetry (Kafka topic)
  - Operations:
    - Filter: temps > 60°C only
    - Window: 300 readings (5-minute window)
    - Calculate: rolling average temperature
  - Outputs to: truck-averages (Kafka topic)

- **verify_math.py**: Math verification tests
  - Tests rolling average calculation
  - Verifies window management
  - Validates filter logic

## Quick Start

### Prerequisites
```bash
pip install -r requirements.txt
```

### 1. Start Redpanda (Kafka)
```bash
docker run -d --name redpanda -p 9092:9092 redpandadata/redpanda:latest redpanda start --overprovisioned --smp 1 --memory 1G --reserve-memory 0M --node-id 0 --advertise-kafka-addr localhost:9092
```

### 2. Create Topics
```bash
docker exec redpanda rpk topic create truck-telemetry --partitions 3
docker exec redpanda rpk topic create truck-averages --partitions 1
```

### 3. Run Producer (Terminal 1)
```bash
python src/truck_producer.py
```

### 4. Run Processor (Terminal 2)
```bash
python src/stream_processor.py
```

### 5. View Results (Terminal 3)
```bash
docker exec redpanda rpk topic consume truck-averages --from-beginning
```

## How It Works

50 Trucks
↓
Producer (truck_producer.py)
↓
Kafka Topic: truck-telemetry
(50 messages/second)
↓
Stream Processor (stream_processor.py)
├─ Filter (temp > 60°C)
├─ Window (300 readings)
└─ Calculate (rolling average)
↓
Kafka Topic: truck-averages
(Processed results)


---> ##Data Format

### Input (truck-telemetry)
```json
{
  "truck_id": "TRUCK_001",
  "temperature": 85.5,
  "timestamp": "2026-08-22T21:00:00"
}
```

### Output (truck-averages)
```json
{
  "truck_id": "TRUCK_001",
  "current_temp": 85.5,
  "rolling_avg_5min": 86.2,
  "reading_count": 300,
  "timestamp": "2026-08-22T21:00:10"
}
```

## Testing

Run math verification:
```bash
python tests/verify_math.py
```

Expected output:

Test 1: Simple Average Calculation - PASS
Test 2: 5-Minute Window (300 readings) - PASS
Test 3: Temperature Filter (>60°C) - PASS
✅ ALL MATH TESTS PASSED


--->  ## Files

stream-processing/
├── src/
│ ├── truck_producer.py # IoT data generator
│ └── stream_processor.py # Stream processor
├── tests/
│ └── verify_math.py # Math verification tests
├── requirements.txt # Python dependencies
└── README.md # This file



**Created by**: Karthikeya 
**Date**: August 2026  
**Project**: AXLERO SOLUTIONS - PROJECT 1(Stream Forge)