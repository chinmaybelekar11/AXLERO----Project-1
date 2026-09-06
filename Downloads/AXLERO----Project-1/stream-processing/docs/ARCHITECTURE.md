\# Architecture - Week 1 \& 2



\## System Overview



Trucks (50)

↓

truck\_producer.py



1 message/truck/second

60-100°C temperature

Total: 50 msg/sec

↓

Redpanda/Kafka

Topic: truck-telemetry

3 partitions

↓

stream\_processor.py

Filter: temp > 60°C

Window: 300 readings

Calculate: rolling average

↓

Redpanda/Kafka

Topic: truck-averages

↓

Backend Team → Frontend → Dashboard



\## Components



\### Producer (truck\_producer.py)

\- Creates 50 truck IDs (TRUCK\_001 to TRUCK\_050)

\- Generates random temperatures (60-100°C)

\- Sends 1 message/truck/second

\- Total: 50 messages/second to Kafka



\### Stream Processor (stream\_processor.py)

\- Consumer: Reads from truck-telemetry

\- Filter: Only keeps temps > 60°C

\- State: In-memory dictionary

\- Calculator: Computes rolling average (300 readings)

\- Producer: Sends to truck-averages



\### Testing (verify\_math.py)

\- Tests rolling average calculation

\- Tests window size (300 readings)

\- Tests temperature filter (>60°C)



\## Data Processing Flow



Message arrives at truck-telemetry:

↓

Parse JSON

↓

Check temperature > 60?

NO → Discard

YES → Continue

↓

Retrieve truck's history

↓

Add new temperature

↓

Keep only last 300 readings

↓

Calculate rolling average

↓

Send to truck-averages topic





\## Performance



| Metric | Value |

|--------|-------|

| Throughput | 50 msg/sec |

| Processing | Real-time |

| Latency | <100ms |

| Data Loss | 0% |

| Trucks | 50 concurrent |

| Window | 300 readings (5 min) |



\## Kafka Topics



\### truck-telemetry (INPUT)

\- Partitions: 3

\- Messages: 50/sec

\- Format: {"truck\_id", "temperature", "timestamp"}



\### truck-averages (OUTPUT)

\- Partitions: 1

\- Format: {"truck\_id", "current\_temp", "rolling\_avg\_5min", "reading\_count"}



\## Week 1 vs Week 2



\*\*Week 1\*\*: Kafka Setup

\- Redpanda running

\- Topics created

\- Producer working



\*\*Week 2\*\*: Stream Processing

\- Built processor

\- Filter logic

\- Window logic

\- Calculation logic

\- All tests passing

