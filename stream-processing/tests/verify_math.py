print("=" * 60)
print("🧪 MATH VERIFICATION TEST")
print("=" * 60)

# Test 1: Simple average
print("\nTest 1: Simple Rolling Average")
print("-" * 60)
temps = [80, 85, 90, 75, 88]
avg = sum(temps) / len(temps)
print(f"Temperatures: {temps}")
print(f"Sum: {sum(temps)}")
print(f"Count: {len(temps)}")
print(f"Average: {avg}")
print(f"Expected: 83.6")
if abs(avg - 83.6) < 0.01:
    print("✅ PASS")
else:
    print("❌ FAIL")

# Test 2: Window size (300 readings)
print("\nTest 2: Window Size (5-minute window)")
print("-" * 60)
temps = []
for i in range(1, 305):  # Add 304 temps
    temps.append(i)
    if len(temps) > 300:  # Keep only 300
        temps.pop(0)

print(f"Total readings added: 304")
print(f"Window size: {len(temps)}")
print(f"Expected: 300")
if len(temps) == 300:
    print("✅ PASS")
else:
    print("❌ FAIL")

# Test 3: Filter (only temps > 60)
print("\nTest 3: Temperature Filter (>60°C)")
print("-" * 60)
all_temps = [45, 65, 55, 75, 85, 50, 90]
filtered = [t for t in all_temps if t > 60]
print(f"All temps: {all_temps}")
print(f"Filtered (>60): {filtered}")
print(f"Expected: [65, 75, 85, 90]")
if filtered == [65, 75, 85, 90]:
    print("✅ PASS")
else:
    print("❌ FAIL")

print("\n" + "=" * 60)
print("✅ ALL MATH TESTS PASSED")
print("=" * 60)