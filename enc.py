# enc.py — COIL v2 Encoder
# Iterative token-optimal, nested COIL blocks
# Type info stored locally (NOT sent to LLM)

import json
from collections import Counter
from copy import deepcopy

ESC = "\\"
PAIR = ","
REC = "|"

TYPE_FILE = "coil_types.json"

TABLE_SEQ = 0
TYPE_REGISTRY = {}

# ---------------- TOKEN COUNT ----------------

try:
    import tiktoken
    ENC = tiktoken.encoding_for_model("gpt-4o-mini")
    def token_count(s): return len(ENC.encode(s))
except Exception:
    def token_count(s): return max(1, (len(s) + 3) // 4)

# ---------------- ESCAPE ----------------

def esc(v: str) -> str:
    return (
        v.replace(ESC, ESC + ESC)
         .replace(PAIR, ESC + PAIR)
         .replace(REC, ESC + REC)
         .replace(":", ESC + ":")
    )

# ---------------- DETECTION ----------------

def is_table(arr):
    return isinstance(arr, list) and len(arr) >= 2 and all(isinstance(x, dict) for x in arr)

def is_categorical_strings(arr):
    return (
        isinstance(arr, list)
        and len(arr) >= 2
        and all(isinstance(x, str) for x in arr)
        and len(set(arr)) <= len(arr) * 0.7
    )

def collect_keys(records):
    keys = set()
    for r in records:
        keys.update(r.keys())
    return sorted(keys)

# ---------------- ITERATIVE VMAP OPTIMIZER ----------------

def greedy_vmap(records, keys):
    flat_vals = []
    for r in records:
        for k in keys:
            v = r.get(k, "")
            if v is not None:
                flat_vals.append(str(v))

    freq = Counter(flat_vals)
    candidates = sorted(
        [v for v, c in freq.items() if c >= 2],
        key=lambda v: freq[v] * len(v),
        reverse=True
    )

    accepted = {}
    baseline = token_count(json.dumps(records, ensure_ascii=False))

    while True:
        best_gain = 0
        best_val = None
        best_tok = None

        for val in candidates:
            if val in accepted:
                continue

            tok = f"V{len(accepted)+1}"
            test_map = accepted | {val: tok}

            rows = []
            for r in records:
                row = []
                for k in keys:
                    v = str(r.get(k, ""))
                    row.append(test_map.get(v, esc(v)))
                rows.append(PAIR.join(row))

            body = REC.join(
                [f"table[{len(records)}]{{{','.join(keys)}}}"] + rows
            )
            meta = f"META&ORDER={','.join(keys)}&vmap=" + ";".join(
                f"{t}:{v}" for v, t in test_map.items()
            )

            tokens = token_count(meta + "|" + body)
            gain = baseline - tokens

            if gain > best_gain:
                best_gain = gain
                best_val = val
                best_tok = tok

        if best_gain > 0:
            accepted[best_val] = best_tok
            baseline -= best_gain
        else:
            break

    return accepted

# ---------------- TABLE ENCODER ----------------

def encode_table(records):
    global TABLE_SEQ, TYPE_REGISTRY
    TABLE_SEQ += 1
    tid = f"tbl_{TABLE_SEQ}"

    keys = collect_keys(records)
    vmap = greedy_vmap(records, keys)

    # Build encoded body
    rows = []
    for r in records:
        row = []
        for k in keys:
            v = str(r.get(k, ""))
            row.append(vmap.get(v, esc(v)))
        rows.append(PAIR.join(row))

    body = REC.join(
        [f"table[{len(records)}]{{{','.join(keys)}}}"] + rows
    )

    meta = f"META&ORDER={','.join(keys)}&tid={tid}"
    if vmap:
        meta += "&vmap=" + ";".join(f"{t}:{v}" for v, t in vmap.items())

    encoded_tokens = token_count(meta + "|" + body)
    original_tokens = token_count(json.dumps(records, ensure_ascii=False))

    if encoded_tokens >= original_tokens:
        return records  # auto-skip

    # Store types
    TYPE_REGISTRY[tid] = {
        k: type(next((r[k] for r in records if k in r), "")).__name__
        for k in keys
    }

    return {"meta": meta, "body": "BODY|" + body}

# ---------------- LOG ENCODER (1-COLUMN TABLE) ----------------

def encode_logs(logs):
    records = [{"msg": s} for s in logs]
    return encode_table(records)

# ---------------- RECURSIVE ENCODER ----------------

def encode_any(obj):
    if isinstance(obj, list) and is_table(obj):
        return encode_table(obj)

    if isinstance(obj, list) and is_categorical_strings(obj):
        return encode_logs(obj)

    if isinstance(obj, dict):
        return {k: encode_any(v) for k, v in obj.items()}

    if isinstance(obj, list):
        return [encode_any(x) for x in obj]

    return obj

def encode(payload):
    global TYPE_REGISTRY, TABLE_SEQ
    TYPE_REGISTRY = {}
    TABLE_SEQ = 0

    result = encode_any(deepcopy(payload))

    with open(TYPE_FILE, "w", encoding="utf-8") as f:
        json.dump(TYPE_REGISTRY, f, indent=2)

    return result
