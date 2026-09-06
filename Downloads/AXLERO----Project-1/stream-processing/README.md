\# Stream Processing Module - Week 1 \& 2



\## Status

\- ✅ Week 1: Kafka Setup - COMPLETE

\- ✅ Week 2: Stream Processing - COMPLETE

\- ✅ Mid-Project Review: ALL TESTS PASSED



\## Overview



Real-time stream processor for IoT truck telemetry data using Apache Kafka.



\*\*Key Metrics\*\*:

\- 50 trucks monitored

\- 50 messages/second throughput

\- 5-minute rolling average (300 readings)

\- 100% math accuracy verified

\- Zero data loss



\## Quick Start



\### Prerequisites

```bash

pip install -r requirements.txt

```



\### 1. Start Kafka

```bash

docker run -d --name redpanda -p 9092:9092 redpandadata/redpanda:latest redpanda start --overprovisioned --smp 1 --memory 1G --reserve-memory 0M --node-id 0 --advertise-kafka-addr localhost:9092

```





\### 2. Create Topics

```bash

docker exec redpanda rpk topic create truck-telemetry --partitions 3

docker exec redpanda rpk topic create truck-averages --partitions 1

```



\### 3. Run Producer (Terminal 1)

```bash

python src/truck\_producer.py

```



\### 4. Run Processor (Terminal 2)

```bash

python src/stream\_processor.py

```



\### 5. View Results (Terminal 3)

```bash

docker exec redpanda rpk topic consume truck-averages -n 10

```



\## Files



\- \*\*src/truck\_producer.py\*\* - IoT data generator (50 trucks)

\- \*\*src/stream\_processor.py\*\* - Stream processor (filter, window, calculate)

\- \*\*src/stream\_processor\_review.py\*\* - Speed testing code

\- \*\*tests/verify\_math.py\*\* - Math verification tests

\- \*\*requirements.txt\*\* - Python dependencies



\## Architecture



50 Trucks

↓

Producer (truck\_producer.py)

↓

Kafka: truck-telemetry (50 msg/sec)

↓

Processor (stream\_processor.py)

├─ Filter (temp > 60°C)

├─ Window (300 readings)

└─ Calculate (rolling average)

↓

Kafka: truck-averages

↓

Backend API → Frontend Dashboard





\## Testing



Run math verification:

```bash

python tests/verify\_math.py

```



Expected output:



✅ Test 1: Rolling Average - PASS

✅ Test 2: Window Size - PASS

✅ Test 3: Filter Logic - PASS

✅ ALL TESTS PASSED





\## Mid-Project Review Results



✅ Speed Test: 10,000 messages (zero loss)

✅ Math Test: 100% accuracy

✅ Integration Test: All working



