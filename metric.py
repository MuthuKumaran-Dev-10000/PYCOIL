# main_scale.py — COIL scaling evaluation (1 → 50 rows)
# Measures how token savings scale with row count
# Across multiple JSON structures
# Verifies losslessness at every step

import json
from copy import deepcopy

from enc import encode
from dec import decode

# ---------------- TOKEN COUNT ----------------

try:
    import tiktoken
    ENC = tiktoken.encoding_for_model("gpt-4o-mini")
    def token_count(s): return len(ENC.encode(s))
    TOKENIZER = "gpt-4o-mini"
except Exception:
    def token_count(s): return max(1, (len(s) + 3) // 4)
    TOKENIZER = "approx"

# ---------------- METRICS ----------------

def stats(obj):
    text = json.dumps(obj, ensure_ascii=False)
    return {
        "tokens": token_count(text),
        "bytes": len(text.encode("utf-8")),
        "chars": len(text),
    }

# ---------------- ROW GENERATORS ----------------

def gen_sensor_rows(n):
    return [
        {"ts": f"2025-01-{i:02d}", "temp": 30 + (i % 5), "humidity": 55 + (i % 7)}
        for i in range(1, n + 1)
    ]

def gen_payment_rows(n):
    methods = ["UPI", "CARD"]
    statuses = ["SUCCESS", "FAILED"]
    return [
        {
            "id": f"TX{i}",
            "method": methods[i % 2],
            "status": statuses[i % 2],
            "amount": [199, 299, 499, 999][i % 4],
        }
        for i in range(1, n + 1)
    ]

def gen_metric_rows(n):
    return [
        {"minute": i, "requests": 100 + i * 5, "errors": i % 3}
        for i in range(1, n + 1)
    ]

def gen_order_rows(n):
    items = ["Book", "Pen", "Notebook"]
    return [
        {"order_id": f"O{i}", "item": items[i % 3], "qty": (i % 5) + 1, "price": [20, 99, 399][i % 3]}
        for i in range(1, n + 1)
    ]

def gen_user_rows(n):
    users = ["alice", "bob"]
    actions = ["login", "logout", "retry"]
    return [
        {"user": users[i % 2], "action": actions[i % 3], "success": i % 2 == 0}
        for i in range(1, n + 1)
    ]

# ---------------- DATASET TEMPLATES ----------------

DATASETS = [
    ("sensor_timeseries", lambda n: {
        "device": "iot-22",
        "location": "warehouse-7",
        "readings": gen_sensor_rows(n)
    }),

    ("payments", lambda n: {
        "service": "payment-gateway",
        "transactions": gen_payment_rows(n)
    }),

    ("metrics_only", lambda n: {
        "service": "auth",
        "metrics": gen_metric_rows(n)
    }),

    ("orders", lambda n: {
        "orders": gen_order_rows(n),
        "currency": "INR"
    }),

    ("user_activity", lambda n: {
        "users": gen_user_rows(n),
        "date": "2025-06-01"
    }),
]

# ---------------- PIPELINE ----------------

print("\n=== COIL SCALING STUDY (1 → 50 rows) ===")
print(f"Tokenizer : {TOKENIZER}")
print("-" * 60)

for name, builder in DATASETS:
    print(f"\nDATASET: {name}")
    print("rows | orig_tok | enc_tok | saving_% | lossless")
    print("-" * 60)

    for rows in range(1, 51):
        original = builder(rows)
        encoded = encode(deepcopy(original))
        decoded = decode(encoded)

        s_o = stats(original)
        s_e = stats(encoded)

        saving = (1 - s_e["tokens"] / s_o["tokens"]) * 100
        lossless = original == decoded

        print(
            f"{rows:>4} | "
            f"{s_o['tokens']:>8} | "
            f"{s_e['tokens']:>7} | "
            f"{saving:>8.2f} | "
            f"{'YES' if lossless else 'NO'}"
        )

print("\nScaling study complete.")
