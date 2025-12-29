import csv
import random


OUTPUT_FILE = "/Users/Vinay/python_training/python_etl_callcenter/src_files/call_center_raw_202501.csv"

VALID_STATUSES = ["COMPLETED", "DROPPED", "FAILED"]
INVALID_STATUSES = ["OPEN", "CLOSED", "UNKNOWN"]

def random_time(valid=True):
    if valid:
        return f"{random.randint(0,23):02}:{random.randint(0,59):02}:{random.randint(0,59):02}"
    else:
        return f"{random.randint(0,99)}:{random.randint(0,99)}"

with open(OUTPUT_FILE, "w", newline="") as f:
    writer = csv.writer(f)

    # Header
    writer.writerow([
        "call_id",
        "caller_id",
        "agent_id",
        "call_start_time",
        "call_end_time",
        "call_status"
    ])

    call_id = 1

    # 1️⃣ Valid records
    for _ in range(7000):
        writer.writerow([
            call_id,
            random.randint(100000, 999999),
            random.randint(1000, 9999),
            random_time(),
            random_time(),
            random.choice(VALID_STATUSES)
        ])
        call_id += 1

    # 2️⃣ Missing mandatory fields
    for _ in range(1000):
        writer.writerow([
            "",  # missing call_id
            random.randint(100000, 999999),
            random.randint(1000, 9999),
            random_time(),
            random_time(),
            random.choice(VALID_STATUSES)
        ])
        call_id += 1

    # 3️⃣ Invalid time format
    for _ in range(800):
        writer.writerow([
            call_id,
            random.randint(100000, 999999),
            random.randint(1000, 9999),
            random_time(valid=False),
            random_time(valid=False),
            random.choice(VALID_STATUSES)
        ])
        call_id += 1

    # 4️⃣ Non-numeric IDs
    for _ in range(600):
        writer.writerow([
            call_id,
            "ABC123",  # invalid caller_id
            "AGENTX",  # invalid agent_id
            random_time(),
            random_time(),
            random.choice(VALID_STATUSES)
        ])
        call_id += 1

    # 5️⃣ Invalid status
    for _ in range(400):
        writer.writerow([
            call_id,
            random.randint(100000, 999999),
            random.randint(1000, 9999),
            random_time(),
            random_time(),
            random.choice(INVALID_STATUSES)
        ])
        call_id += 1

    # 6️⃣ Malformed rows (missing column)
    for _ in range(200):
        writer.writerow([
            call_id,
            random.randint(100000, 999999),
            random.randint(1000, 9999),
            random_time()
            # missing end_time & status
        ])
        call_id += 1

print("Test data generated:", OUTPUT_FILE)
