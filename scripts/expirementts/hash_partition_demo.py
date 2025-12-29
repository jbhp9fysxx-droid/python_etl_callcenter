def hash_bucket(call_id, num_buckets):
    return call_id % num_buckets


# Simulated call records
records = [
    {"call_id": 101, "month": "2025-01"},
    {"call_id": 102, "month": "2025-01"},
    {"call_id": 103, "month": "2025-01"},
    {"call_id": 104, "month": "2025-01"},
    {"call_id": 105, "month": "2025-01"},
]

NUM_BUCKETS = 3

for r in records:
    bucket = hash_bucket(r["call_id"], NUM_BUCKETS)
    print(f"Call {r['call_id']} → bucket {bucket}")
