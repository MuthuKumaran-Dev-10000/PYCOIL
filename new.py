# main.py — COIL multi-dataset end-to-end evaluation
# Tests 5 different JSON structures
# Measures tokens / chars / bytes
# Verifies strict losslessness

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
        "chars": len(text),
        "bytes": len(text.encode("utf-8")),
        "tokens": token_count(text),
    }

# ---------------- DATASETS ----------------

DATASETS = [

    # 1. Sensor time-series (structured)
    {
        "name": "sensor_timeseries",
        "data": {
            "device": "iot-22",
            "location": "warehouse-7",
            "readings": [
                {"ts": "2025-01-01", "temp": 34, "humidity": 60},
                {"ts": "2025-01-02", "temp": 35, "humidity": 58},
                {"ts": "2025-01-03", "temp": 33, "humidity": 61},
                {"ts": "2025-01-04", "temp": 36, "humidity": 59},
            ]
        }
    },

    # 2. Payment transactions (semi-structured)
    {
        "name": "payments",
        "data": {
            "service": "payment-gateway",
            "transactions": [
                {"id": "TX1", "method": "UPI", "status": "SUCCESS", "amount": 499},
                {"id": "TX2", "method": "CARD", "status": "FAILED", "amount": 1299},
                {"id": "TX3", "method": "UPI", "status": "SUCCESS", "amount": 249},
                {"id": "TX4", "method": "UPI", "status": "SUCCESS", "amount": 499},
            ]
        }
    },

    # 3. Log + metrics (mixed)
    {
        "name": "logs_with_metrics",
        "data": {
            "service": "auth",
            "logs": [
                "INFO login success",
                "WARN retry login",
                "ERROR invalid token",
                "INFO logout"
            ],
            "metrics": [
                {"minute": 1, "requests": 120, "errors": 2},
                {"minute": 2, "requests": 140, "errors": 1},
                {"minute": 3, "requests": 160, "errors": 0},
            ]
        }
    },

    # 4. E-commerce orders (nested + table)
    {
        "name": "orders",
        "data": {
            "orders": [
                {"order_id": "O1", "item": "Book", "qty": 2, "price": 399},
                {"order_id": "O2", "item": "Pen", "qty": 10, "price": 20},
                {"order_id": "O3", "item": "Notebook", "qty": 3, "price": 99},
            ],
            "currency": "INR"
        }
    },

    # 5. User activity audit (irregular but repetitive)
    {
        "name": "user_activity",
        "data": {
            "users": [
                {"user": "alice", "action": "login", "success": True},
                {"user": "bob", "action": "login", "success": False},
                {"user": "alice", "action": "logout", "success": True},
                {"user": "bob", "action": "retry", "success": False},
            ],
            "date": "2025-06-01"
        }
    }
]

# ---------------- PIPELINE ----------------

print("\n=== COIL MULTI-DATASET EVALUATION ===")
print(f"Tokenizer : {TOKENIZER}")
print("-" * 50)

for idx, ds in enumerate(DATASETS, 1):
    name = ds["name"]
    original = deepcopy(ds["data"])

    encoded = encode(original)
    decoded = decode(encoded)

    s_orig = stats(original)
    s_enc = stats(encoded)
    s_dec = stats(decoded)

    lossless = (original == decoded)

    print(f"\n[{idx}] DATASET : {name}")
    print(f"Lossless : {'YES' if lossless else 'NO'}")

    print("Original -> "
          f"chars={s_orig['chars']} "
          f"bytes={s_orig['bytes']} "
          f"tokens={s_orig['tokens']}")

    print("Encoded  -> "
          f"chars={s_enc['chars']} "
          f"bytes={s_enc['bytes']} "
          f"tokens={s_enc['tokens']}")

    print("Savings  -> "
          f"token%={(1 - s_enc['tokens']/s_orig['tokens']) * 100:.2f} "
          f"byte%={(1 - s_enc['bytes']/s_orig['bytes']) * 100:.2f}")

    # dump files
    with open(f"original_{idx}.json", "w", encoding="utf-8") as f:
        json.dump(original, f, indent=2, ensure_ascii=False)

    with open(f"encoded_{idx}.json", "w", encoding="utf-8") as f:
        json.dump(encoded, f, indent=2, ensure_ascii=False)

    with open(f"decoded_{idx}.json", "w", encoding="utf-8") as f:
        json.dump(decoded, f, indent=2, ensure_ascii=False)

print("\nDone. Files written: original_i.json, encoded_i.json, decoded_i.json")
