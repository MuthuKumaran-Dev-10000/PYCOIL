# enc.py — General COIL Encoder (type-safe, side-channel)
# Type info stored locally in coil_types.json (NOT sent to LLM)

import json
from collections import Counter
from copy import deepcopy

ESC = "\\"
PAIR = ","
REC = "|"

TYPE_FILE = "coil_types.json"
TABLE_ID = 0
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

def is_table(arr):
    return isinstance(arr, list) and len(arr) >= 2 and all(isinstance(x, dict) for x in arr)

def collect_keys(records):
    keys = set()
    for r in records:
        keys.update(r.keys())
    return sorted(keys)

# ---------------- VALUE MAP ----------------

def propose_vmap(records, payload_text, min_freq=2):
    vals = []
    for r in records:
        for v in r.values():
            if v is not None:
                vals.append(str(v))
    freq = Counter(vals)
    return {
        v: f"V{i+1}"
        for i, v in enumerate(
            sorted([v for v, c in freq.items() if c >= min_freq],
                   key=lambda x: freq[x]*len(x),
                   reverse=True)
        )
    }

# ---------------- TABLE ENCODER (AUTO-SKIP ENABLED) ----------------

def encode_table(records):
    global TABLE_ID, TYPE_REGISTRY

    keys = collect_keys(records)

    # -------- Original token cost (baseline) --------
    original_text = json.dumps(records, ensure_ascii=False)
    original_tokens = token_count(original_text)

    # -------- Build encoded candidate --------
    TABLE_ID += 1
    table_id = f"tbl_{TABLE_ID}"

    payload_text = original_text.lower()
    vmap = propose_vmap(records, payload_text)

    rows = []
    for r in records:
        row = []
        for k in keys:
            v = r.get(k, "")
            vs = str(v)
            row.append(vmap.get(vs, esc(vs)))
        rows.append(PAIR.join(row))

    body = REC.join(
        [f"table[{len(records)}]{{{','.join(keys)}}}"] + rows
    )

    meta = f"META&ORDER={','.join(keys)}&tid={table_id}"
    if vmap:
        meta += "&vmap=" + ";".join(f"{t}:{v}" for v, t in vmap.items())

    encoded_tokens = token_count(meta + "|" + body)

    # -------- AUTO-SKIP DECISION --------
    if encoded_tokens >= original_tokens:
        # ❌ Skip COIL for this table
        return records

    # -------- ACCEPT ENCODING --------
    TYPE_REGISTRY[table_id] = {
        k: type(next((r[k] for r in records if k in r), "")).__name__
        for k in keys
    }

    return {"meta": meta, "body": "BODY|" + body}

# ---------------- RECURSIVE ENCODER ----------------

def encode_any(obj):
    if isinstance(obj, list) and is_table(obj):
        return encode_table(obj)
    if isinstance(obj, dict):
        return {k: encode_any(v) for k, v in obj.items()}
    if isinstance(obj, list):
        return [encode_any(x) for x in obj]
    return obj

def encode(payload):
    global TYPE_REGISTRY
    TYPE_REGISTRY = {}
    result = encode_any(deepcopy(payload))
    with open(TYPE_FILE, "w", encoding="utf-8") as f:
        json.dump(TYPE_REGISTRY, f, indent=2)
    return result
