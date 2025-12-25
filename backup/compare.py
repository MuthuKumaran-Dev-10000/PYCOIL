# compare.py — COIL mixed-data stress test
# One large JSON with structured + unstructured + logs + sensors + transactions
# 100+ rows per table
# Measures token / byte / char savings
# Verifies strict losslessness

import json
from copy import deepcopy
from datetime import datetime, timedelta

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
        "tokens": token_count(text)
    }

# ---------------- DATA GENERATORS ----------------

def gen_sensor_data(n=120):
    base = datetime(2025, 1, 1)
    return [
        {
            "ts": (base + timedelta(minutes=i)).isoformat(),
            "temperature": 28 + (i % 6),
            "humidity": 55 + (i % 10),
            "pressure": 1010 + (i % 4)
        }
        for i in range(n)
    ]

def gen_transactions(n=120):
    methods = ["UPI", "CARD", "NETBANKING"]
    status = ["SUCCESS", "FAILED"]
    cities = ["Chennai", "Bangalore", "Hyderabad"]

    return [
        {
            "txn_id": f"TXN{i:05d}",
            "method": methods[i % 3],
            "status": status[i % 2],
            "amount": [199, 299, 499, 999][i % 4],
            "city": cities[i % 3],
            "timestamp": (datetime(2025, 3, 21) + timedelta(seconds=i*37)).isoformat()
        }
        for i in range(n)
    ]

def gen_metrics(n=120):
    return [
        {
            "minute": i,
            "requests": 1000 + i * 3,
            "errors": i % 5
        }
        for i in range(n)
    ]

def gen_logs():
    return [
        "INFO system boot",
        "INFO sensor connected",
        "WARN temperature spike detected",
        "INFO retrying sensor read",
        "ERROR transient network issue",
        "INFO recovered successfully",
    ] * 20  # intentionally repetitive free text

# ---------------- MIXED JSON ----------------

ORIGINAL_JSON = {
    "system": {
        "service": "smart-city-analytics",
        "region": "ap-south-1",
        "version": "v4.7.2",
        "deployed_at": "2025-03-01"
    },

    "sensors": {
        "device_id": "iot-sensor-cluster-17",
        "location": "traffic-junction-42",
        "readings": gen_sensor_data(120)
    },

    "transactions": {
        "provider": "city-pay",
        "currency": "INR",
        "records": gen_transactions(120)
    },

    "metrics": gen_metrics(120),

    "logs": gen_logs(),

    "summary": {
        "total_readings": 120,
        "total_transactions": 120,
        "error_rate": 0.04,
        "dominant_payment_method": "UPI"
    }
}

# ---------------- PIPELINE ----------------

print("\n=== COIL MIXED-DATA STRESS TEST ===")
print(f"Tokenizer : {TOKENIZER}")
print("-" * 60)

original = deepcopy(ORIGINAL_JSON)
encoded = encode(original)
decoded = decode(encoded)

s_orig = stats(original)
s_enc = stats(encoded)
s_dec = stats(decoded)

lossless = (original == decoded)

# ---------------- REPORT ----------------

print("Lossless decode :", "YES" if lossless else "NO")

print("\n--- ORIGINAL ---")
print(f"Chars  : {s_orig['chars']}")
print(f"Bytes  : {s_orig['bytes']}")
print(f"Tokens : {s_orig['tokens']}")

print("\n--- ENCODED (COIL) ---")
print(f"Chars  : {s_enc['chars']}")
print(f"Bytes  : {s_enc['bytes']}")
print(f"Tokens : {s_enc['tokens']}")

print("\n--- SAVINGS ---")
print(f"Token savings % : {(1 - s_enc['tokens']/s_orig['tokens']) * 100:.2f}")
print(f"Byte savings %  : {(1 - s_enc['bytes']/s_orig['bytes']) * 100:.2f}")

print("\n--- DECODED ---")
print(f"Chars  : {s_dec['chars']}")
print(f"Bytes  : {s_dec['bytes']}")
print(f"Tokens : {s_dec['tokens']}")

# ---------------- WRITE FILES ----------------

with open("mixed_original.json", "w", encoding="utf-8") as f:
    json.dump(original, f, indent=2, ensure_ascii=False)

with open("mixed_encoded.json", "w", encoding="utf-8") as f:
    json.dump(encoded, f, indent=2, ensure_ascii=False)

with open("mixed_decoded.json", "w", encoding="utf-8") as f:
    json.dump(decoded, f, indent=2, ensure_ascii=False)

print("\nFiles written:")
print("  mixed_original.json")
print("  mixed_encoded.json")
print("  mixed_decoded.json")
