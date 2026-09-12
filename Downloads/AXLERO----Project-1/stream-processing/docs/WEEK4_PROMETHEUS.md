Week 4: Prometheus Monitoring

## Overview

Added Prometheus metrics to monitor processor health in real-time.

## Metrics Tracked

### 1. Messages Processed Total
- **Metric**: `messages_processed_total`
- **Type**: Counter
- **What it tracks**: Total number of messages processed since startup
- **Example**: 10,000 messages

### 2. Messages Per Second
- **Metric**: `messages_per_second`
- **Type**: Gauge
- **What it tracks**: Current processing speed
- **Example**: 540 msg/sec

### 3. Trucks Tracked
- **Metric**: `trucks_tracked`
- **Type**: Gauge
- **What it tracks**: Number of active trucks
- **Example**: 50 trucks

### 4. Errors Total
- **Metric**: `errors_total`
- **Type**: Counter
- **What it tracks**: Total errors encountered
- **Example**: 0 errors

### 5. State Saves Total
- **Metric**: `state_saves_total`
- **Type**: Counter
- **What it tracks**: Times state saved to disk
- **Example**: 5,000 saves

## How to View Metrics

### Option 1: Browser
Open: `http://localhost:8000/metrics`

You'll see all metrics in Prometheus text format.

### Option 2: PowerShell
```powershell
Invoke-WebRequest http://localhost:8000/metrics
```

## Metrics Endpoint

- **URL**: `http://localhost:8000/metrics`
- **Port**: 8000
- **Format**: Prometheus text format
- **Refresh**: Every few seconds

## What Metrics Mean

**Counter**: Only goes UP (total count)
- messages_processed_total
- errors_total
- state_saves_total

**Gauge**: Can go UP or DOWN (current value)
- messages_per_second
- trucks_tracked

## Testing

1. Start processor: `python src\stream_processor_with_metrics.py`
2. Send messages
3. Open browser: `http://localhost:8000/metrics`
4. Watch metrics increase in real-time ✅
