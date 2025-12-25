# ============================================================
# COIL Token Benchmark Script (OLD enc/dec compatible)
# ============================================================

import json
from datetime import datetime
from typing import Any, List

import enc              # old enc.py
from enc import encode
from dec import decode

# ------------------------------------------------------------
# Config
# ------------------------------------------------------------

CASE_FILE = "cases.txt"
SEPARATOR = "____________________"
LOG_FILE = "token_log.txt"

MODEL_NAME = "gpt-4o-mini"   # tokenizer fixed in enc.py

# ------------------------------------------------------------
# Load JSON cases from TXT file
# ------------------------------------------------------------

def load_cases_from_txt(path: str) -> List[Any]:
    with open(path, "r", encoding="utf-8") as f:
        content = f.read()

    blocks = content.split(SEPARATOR)
    cases = []

    for idx, block in enumerate(blocks, start=1):
        block = block.strip()
        if not block:
            continue
        try:
            cases.append(json.loads(block))
        except json.JSONDecodeError as e:
            print(f"⚠️ Skipping invalid JSON in case #{idx}: {e}")

    return cases


# ------------------------------------------------------------
# Benchmark runner
# ------------------------------------------------------------

def run_benchmark():
    cases = load_cases_from_txt(CASE_FILE)

    if not cases:
        print("❌ No valid JSON cases found.")
        return

    log_lines = []

    # --------------------------------------------------------
    # Header
    # --------------------------------------------------------
    log_lines.append("COIL TOKEN BENCHMARK REPORT (OLD ENCODER)")
    log_lines.append(f"Generated at : {datetime.now()}")
    log_lines.append(f"Tokenizer    : {MODEL_NAME}")
    log_lines.append(f"Total cases  : {len(cases)}")
    log_lines.append("=" * 100)

    total_orig = total_enc = total_dec = 0

    # --------------------------------------------------------
    # Per-case evaluation
    # --------------------------------------------------------
    for idx, original in enumerate(cases, start=1):
        log_lines.append(f"\nCASE #{idx}")
        log_lines.append("-" * 100)

        encoded = encode(original)
        print(encoded)
        decoded = decode(encoded)

        # Serialize
        original_json = json.dumps(original, indent=2, ensure_ascii=False)
        encoded_json  = json.dumps(encoded, indent=2, ensure_ascii=False)
        decoded_json  = json.dumps(decoded, indent=2, ensure_ascii=False)

        # Token counts (OLD enc uses private _token_count)
        orig_tokens = enc._token_count(original_json)
        enc_tokens  = enc._token_count(encoded_json)
        dec_tokens  = enc._token_count(decoded_json)

        total_orig += orig_tokens
        total_enc  += enc_tokens
        total_dec  += dec_tokens

        delta = enc_tokens - orig_tokens
        savings_pct = (
            (orig_tokens - enc_tokens) / orig_tokens * 100
            if orig_tokens > 0 else 0
        )

        # ----------------------------------------------------
        # Metrics
        # ----------------------------------------------------
        log_lines.append(f"Original tokens : {orig_tokens}")
        log_lines.append(f"Encoded tokens  : {enc_tokens}")
        log_lines.append(f"Decoded tokens  : {dec_tokens}")
        log_lines.append(f"Delta           : {delta}")
        log_lines.append(f"Savings (%)     : {savings_pct:.2f}%")

        # ----------------------------------------------------
        # Payloads
        # ----------------------------------------------------
        # log_lines.append("\n--- ORIGINAL JSON ---")
        # log_lines.append(original_json)

        # log_lines.append("\n--- COIL ENCODED JSON ---")
        # log_lines.append(encoded_json)

        # log_lines.append("\n--- DECODED JSON ---")
        # log_lines.append(decoded_json)

        # log_lines.append(
        #     "\nNOTE: OLD decoder restores only 'sensordata' "
        #     "→ structural loss expected\n"
        # )

    # --------------------------------------------------------
    # Summary
    # --------------------------------------------------------
    log_lines.append("\n" + "=" * 100)
    log_lines.append("SUMMARY")
    log_lines.append("=" * 100)

    avg_savings = (
        (total_orig - total_enc) / total_orig * 100
        if total_orig > 0 else 0
    )

    log_lines.append(f"Total original tokens : {total_orig}")
    log_lines.append(f"Total encoded tokens  : {total_enc}")
    log_lines.append(f"Total decoded tokens  : {total_dec}")
    log_lines.append(f"Average savings (%)   : {avg_savings:.2f}%")
    log_lines.append("Decoder is STRUCTURALLY LOSSY by design (old COIL)")

    # --------------------------------------------------------
    # Write log file
    # --------------------------------------------------------
    with open(LOG_FILE, "w", encoding="utf-8") as f:
        f.write("\n".join(log_lines))

    print(f"✅ Benchmark complete → {LOG_FILE}")


# ------------------------------------------------------------
# Entry
# ------------------------------------------------------------

if __name__ == "__main__":
    run_benchmark()
