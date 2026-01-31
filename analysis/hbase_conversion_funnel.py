import os
import json
import csv
import happybase
from collections import Counter

import matplotlib.pyplot as plt

# ==========================
# CONFIG
# ==========================
HBASE_HOST = "localhost"
HBASE_PORT = 9090          # thrift port
TABLE_NAME = "session"

# Output folders
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
OUT_DIR = os.path.join(BASE_DIR, "outputs", "analysis_plots")
TAB_DIR = os.path.join(BASE_DIR, "outputs", "analysis_tables")
os.makedirs(OUT_DIR, exist_ok=True)
os.makedirs(TAB_DIR, exist_ok=True)

PLOT_PATH = os.path.join(OUT_DIR, "hbase_conversion_funnel.png")
CSV_PATH  = os.path.join(TAB_DIR, "hbase_conversion_funnel_summary.csv")

# Funnel definition (order matters)
FUNNEL_STAGES = [
    ("Home", "home"),
    ("Category Listing", "category_listing"),
    ("Product Detail", "product_detail"),
    ("Cart", "cart"),
    ("Checkout", "checkout"),
    ("Confirmation", "confirmation"),
]

def safe_decode(x):
    return x.decode("utf-8") if isinstance(x, (bytes, bytearray)) else x

def extract_page_types(raw_json_str: str):
    """
    Extract set of page_type values from raw_json["page_views"].
    Returns: set(str)
    """
    try:
        obj = json.loads(raw_json_str)
    except Exception:
        return set()

    page_views = obj.get("page_views", [])
    types = set()

    if isinstance(page_views, list):
        for pv in page_views:
            if isinstance(pv, dict):
                pt = pv.get("page_type")
                if pt:
                    types.add(str(pt).strip().lower())
    return types

def main():
    print("🔌 Connecting to HBase (Thrift)...")
    conn = happybase.Connection(HBASE_HOST, HBASE_PORT, timeout=200000)
    conn.open()

    table = conn.table(TABLE_NAME)

    print("📥 Scanning HBase table for cf:raw_json (this may take a bit for 50k rows)...")

    total_rows = 0
    stage_hits = Counter({label: 0 for (label, _) in FUNNEL_STAGES})

    # We'll scan only the raw_json column to keep it lighter
    scan_cols = [b"cf:raw_json"]

    for row_key, data in table.scan(columns=scan_cols, batch_size=500):
        total_rows += 1

        raw_json = data.get(b"cf:raw_json", b"")
        raw_json = safe_decode(raw_json)

        page_types = extract_page_types(raw_json)

        # Determine if this session reached each stage
        # Important: funnel is sequential concept, but we count "reached stage" if page_types contains it.
        for label, stage_key in FUNNEL_STAGES:
            if stage_key in page_types:
                stage_hits[label] += 1

        # progress log every 5000
        if total_rows % 5000 == 0:
            print(f"  ...processed {total_rows} sessions")

    conn.close()

    if total_rows == 0:
        print("❌ No rows found in HBase table. Stop.")
        return

    print(f"✅ Total sessions scanned: {total_rows}")

    # Build ordered results
    labels = [label for (label, _) in FUNNEL_STAGES]
    counts = [stage_hits[label] for label in labels]

    # Make funnel-like (ensure non-increasing by applying cumulative min from top)
    # This avoids weird cases where "cart" appears without "home" etc.
    adjusted = []
    running = None
    for c in counts:
        if running is None:
            running = c
        else:
            running = min(running, c)
        adjusted.append(running)

    # Compute conversion rates relative to previous stage and relative to total sessions
    rows_out = []
    prev = None
    for i, label in enumerate(labels):
        c = adjusted[i]
        rate_from_total = (c / total_rows) * 100.0
        if prev is None:
            rate_from_prev = 100.0
        else:
            rate_from_prev = (c / prev) * 100.0 if prev > 0 else 0.0
        rows_out.append([label, c, f"{rate_from_prev:.2f}%", f"{rate_from_total:.2f}%"])
        prev = c

    # Save CSV
    with open(CSV_PATH, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(["stage", "sessions_count", "pct_from_previous_stage", "pct_of_total_sessions"])
        writer.writerows(rows_out)

    print(f"📄 Saved summary CSV: {CSV_PATH}")

    # Plot (funnel bar chart)
    plt.figure(figsize=(10, 5))
    plt.bar(labels, adjusted, edgecolor="white", linewidth=1.5)  # white bar borders to separate bars
    plt.title("Conversion Funnel (from HBase Sessions)")
    plt.ylabel("Number of Sessions")
    plt.xticks(rotation=25, ha="right")
    plt.tight_layout()
    plt.savefig(PLOT_PATH, dpi=200)
    plt.close()

    print(f"🖼️ Saved funnel plot: {PLOT_PATH}")
    print("✅ Done.")

if __name__ == "__main__":
    main()
