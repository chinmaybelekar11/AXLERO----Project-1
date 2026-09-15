from rocksdict import Rdict
import json

# 1. Create / open a RocksDB database
db = Rdict("truck_state_db")

# 2. Write some data (simulate truck state)
truck_id = "TRUCK_1042"
state = {
    "sum": 235.5,
    "count": 3,
    "last_updated": 1725123456.789
}

db[truck_id] = json.dumps(state)
print(f"Written state for {truck_id}")

# 3. Read the data back
raw = db[truck_id]
loaded_state = json.loads(raw)

print("Read from RocksDB:")
print(loaded_state)
print(f"Average temperature = {loaded_state['sum'] / loaded_state['count']:.2f}°C")

# 4. Update the state
loaded_state["sum"] += 80.0
loaded_state["count"] += 1
db[truck_id] = json.dumps(loaded_state)

print("\nUpdated state:")
print(json.loads(db[truck_id]))

# 5. Close the database
db.close()
print("\nDatabase closed successfully")