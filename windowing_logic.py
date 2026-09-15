from collections import defaultdict
from typing import Dict, Optional
import time

class TruckWindowManager:
    def __init__(self, window_size_seconds: int = 300, allowed_lateness: int = 60):
        """
        window_size_seconds : 5 minutes = 300
        allowed_lateness    : how many seconds late an event can be
        """
        self.window_size = window_size_seconds
        self.allowed_lateness = allowed_lateness
        
        # {truck_id: {window_start_timestamp: {"sum": float, "count": int}}}
        self.state: Dict[str, Dict[int, Dict]] = defaultdict(dict)

    def _get_window_start(self, timestamp: float) -> int:
        """Get the starting timestamp of the 5-minute window"""
        return int(timestamp // self.window_size) * self.window_size

    def add_event(self, truck_id: str, temperature: float, timestamp: float) -> bool:
        """
        Add event to the correct window.
        Returns True if accepted, False if too late.
        """
        window_start = self._get_window_start(timestamp)
        current_time = time.time()

        # Reject if the event is too late
        if current_time - timestamp > self.window_size + self.allowed_lateness:
            print(f"Rejected late event for {truck_id}")
            return False

        if window_start not in self.state[truck_id]:
            self.state[truck_id][window_start] = {"sum": 0.0, "count": 0}

        self.state[truck_id][window_start]["sum"] += temperature
        self.state[truck_id][window_start]["count"] += 1
        return True

    def get_average(self, truck_id: str, timestamp: float = None) -> Optional[float]:
        """Get average of the latest window for a truck"""
        if truck_id not in self.state or not self.state[truck_id]:
            return None

        if timestamp is None:
            # Get the most recent window
            latest_window = max(self.state[truck_id].keys())
        else:
            latest_window = self._get_window_start(timestamp)

        data = self.state[truck_id].get(latest_window)
        if not data or data["count"] == 0:
            return None

        return round(data["sum"] / data["count"], 2)

    def get_all_averages(self) -> Dict[str, float]:
        """Return latest average for every truck"""
        result = {}
        for truck_id in self.state:
            avg = self.get_average(truck_id)
            if avg is not None:
                result[truck_id] = avg
        return result

    def cleanup_old_windows(self, current_timestamp: float):
        """Remove windows that are too old"""
        min_allowed = current_timestamp - (self.window_size * 2)

        for truck_id in list(self.state.keys()):
            for window_start in list(self.state[truck_id].keys()):
                if window_start < min_allowed:
                    del self.state[truck_id][window_start]
            
            if not self.state[truck_id]:
                del self.state[truck_id]


# ==================== TEST ====================
if __name__ == "__main__":
    manager = TruckWindowManager(window_size_seconds=300)

    print("=== Adding normal events ===")

    now = time.time()

    events = [
        ("TRUCK_101", 75.0, now - 120),   # 2 minutes ago
        ("TRUCK_101", 78.0, now - 60),    # 1 minute ago
        ("TRUCK_101", 80.0, now - 10),    # 10 seconds ago
        ("TRUCK_102", 70.0, now - 90),
        ("TRUCK_102", 72.5, now - 30),
    ]

    for truck_id, temp, ts in events:
        accepted = manager.add_event(truck_id, temp, ts)
        avg = manager.get_average(truck_id)
        print(f"{truck_id} | Temp: {temp} | Accepted: {accepted} | Avg: {avg}°C")

    print("\nAll averages:", manager.get_all_averages())