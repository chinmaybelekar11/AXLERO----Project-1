from rocksdict import Rdict
import json
import time
from typing import Dict, Optional
from collections import defaultdict

class RocksDBStateManager:
    def __init__(self, db_path: str = "truck_state_db", window_size: int = 300):
        """
        Week 3: Real RocksDB State Manager with Recovery
        """
        self.db = Rdict(db_path)
        self.window_size = window_size  # 5 minutes = 300 seconds
        self.metrics = {
            "events_processed": 0,
            "trucks_tracked": 0,
            "recoveries": 0
        }
        print(f"[INIT] RocksDB opened at '{db_path}'")
        self._recover_state()

    def _get_window_start(self, timestamp: float) -> int:
        return int(timestamp // self.window_size) * self.window_size

    def _recover_state(self):
        """Simulate recovery: count how many trucks already exist in RocksDB"""
        count = 0
        for key in self.db.keys():
            count += 1
        self.metrics["trucks_tracked"] = count
        self.metrics["recoveries"] += 1
        print(f"[RECOVERY] Loaded state for {count} trucks from RocksDB")

    def add_event(self, truck_id: str, temperature: float, timestamp: float) -> bool:
        """
        Add temperature event and update rolling average in RocksDB
        """
        window_start = self._get_window_start(timestamp)
        key = f"{truck_id}:{window_start}"

        # Load existing state or create new
        if key in self.db:
            state = json.loads(self.db[key])
        else:
            state = {"sum": 0.0, "count": 0, "last_updated": timestamp}

        state["sum"] += temperature
        state["count"] += 1
        state["last_updated"] = timestamp

        # Save back to RocksDB
        self.db[key] = json.dumps(state)

        self.metrics["events_processed"] += 1
        return True

    def get_average(self, truck_id: str) -> Optional[float]:
        """Get the latest average for a truck"""
        latest_avg = None
        latest_ts = 0

        for key in self.db.keys():
            if key.startswith(f"{truck_id}:"):
                state = json.loads(self.db[key])
                if state["last_updated"] > latest_ts and state["count"] > 0:
                    latest_ts = state["last_updated"]
                    latest_avg = round(state["sum"] / state["count"], 2)

        return latest_avg

    def get_all_averages(self) -> Dict[str, float]:
        result = {}
        trucks = set()

        for key in self.db.keys():
            truck_id = key.split(":")[0]
            trucks.add(truck_id)

        for truck_id in trucks:
            avg = self.get_average(truck_id)
            if avg is not None:
                result[truck_id] = avg

        self.metrics["trucks_tracked"] = len(result)
        return result

    def get_metrics(self) -> Dict:
        """Week 4: Basic metrics"""
        return self.metrics.copy()

    def close(self):
        self.db.close()
        print("[CLOSE] RocksDB closed successfully")


# ==================== DEMO / TEST ====================
if __name__ == "__main__":
    print("=" * 50)
    print("  STREAM FORGE - RocksDB State Manager (Week 3)")
    print("=" * 50)

    manager = RocksDBStateManager()

    now = time.time()
    events = [
        ("TRUCK_001", 78.5, now - 100),
        ("TRUCK_001", 80.0, now - 50),
        ("TRUCK_001", 79.2, now - 10),
        ("TRUCK_002", 85.0, now - 80),
        ("TRUCK_002", 87.5, now - 20),
        ("TRUCK_003", 72.0, now - 30),
    ]

    print("\n[PROCESSING EVENTS]")
    for truck_id, temp, ts in events:
        manager.add_event(truck_id, temp, ts)
        avg = manager.get_average(truck_id)
        print(f"  {truck_id} | Temp: {temp}°C | Current Avg: {avg}°C")

    print("\n[ALL AVERAGES]")
    print(manager.get_all_averages())

    print("\n[METRICS - Week 4]")
    print(manager.get_metrics())

    manager.close()
    print("\nDemo completed successfully!")