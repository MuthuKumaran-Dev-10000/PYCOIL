# dec.py — General COIL Decoder (type restoring)

import json
from copy import deepcopy

ESC = "\\"
PAIR = ","
REC = "|"
TYPE_FILE = "coil_types.json"

def unesc(v):
    out, i = [], 0
    while i < len(v):
        if v[i] == ESC and i+1 < len(v):
            out.append(v[i+1])
            i += 2
        else:
            out.append(v[i])
            i += 1
    return "".join(out)

def restore_type(v, t):
    if t == "int": return int(v)
    if t == "float": return float(v)
    if t == "bool": return v.lower() == "true"
    if t == "NoneType": return None
    return v

def decode_table(meta, body, types):
    meta = meta[len("META&"):]
    body = body[len("BODY|"):]

    meta_kv = dict(p.split("=", 1) for p in meta.split("&") if "=" in p)
    keys = meta_kv["ORDER"].split(",")
    table_id = meta_kv["tid"]

    col_types = types.get(table_id, {})
    vmap = {}

    if "vmap" in meta_kv:
        for e in meta_kv["vmap"].split(";"):
            t, v = e.split(":", 1)
            vmap[t] = v

    rows = body.split(REC)[1:]
    records = []

    for row in rows:
        vals = row.split(PAIR)
        rec = {}
        for i, k in enumerate(keys):
            raw = vals[i]
            val = vmap.get(raw, unesc(raw))
            rec[k] = restore_type(val, col_types.get(k, "str"))
        records.append(rec)

    return records

def decode_any(obj, types):
    if isinstance(obj, dict):
        if "meta" in obj and "body" in obj:
            return decode_table(obj["meta"], obj["body"], types)
        return {k: decode_any(v, types) for k, v in obj.items()}
    if isinstance(obj, list):
        return [decode_any(x, types) for x in obj]
    return obj

def decode(payload):
    with open(TYPE_FILE, "r", encoding="utf-8") as f:
        types = json.load(f)
    return decode_any(deepcopy(payload), types)
