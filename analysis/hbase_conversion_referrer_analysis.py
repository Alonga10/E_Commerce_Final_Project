import os
import csv
from collections import Counter, defaultdict

import happybase
import pandas as pd
import matplotlib.pyplot as plt

# =========================
# CONFIG
# =========================
HBASE_HOST = "localhost"
HBASE_THRIFT_PORT = 9090
TABLE_NAME = "session"

# If you want full scan (50k), keep LIMIT = None
# If testing, set LIMIT = 5000
LIMIT = None

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
OUT_DIR = os.path.join(BASE_DIR, "outputs", "analysis_plots")
os.makedirs(OUT_DIR, exist_ok=True)

SUMMARY_CSV = os.path.join(OUT_DIR, "conversion_status_summary.csv")
REFERRER_CSV = os.path.join(OUT_DIR, "referrer_by_status_summary.csv")
PLOT_STATUS = os.path.join(OUT_DIR, "conversion_status_distribution.png")
PLOT_REFERRER = os.path.join(OUT_DIR, "top_referrers_converted.png")

COL_STATUS = b"cf:conversion_status"
COL_REFERRER = b"cf:referrer"

def b2s(x):
    if x is None:
        return ""
    if isinstance(x, bytes):
        return x.decode("utf-8", errors="replace")
    return str(x)

def main():
    conn = happybase.Connection(HBASE_HOST, HBASE_THRIFT_PORT, timeout=200000)
    conn.open()
    table = conn.table(TABLE_NAME)

    status_counter = Counter()
    ref_converted = Counter()
    ref_by_status = defaultdict(Counter)

    scanned = 0
    for _, data in table.scan(columns=[COL_STATUS, COL_REFERRER], batch_size=1000):
        status = b2s(data.get(COL_STATUS)).strip() or "unknown"
        ref = b2s(data.get(COL_REFERRER)).strip() or "unknown"

        status_counter[status] += 1
        ref_by_status[status][ref] += 1

        # for "converted" only referrer ranking
        if status.lower() in ["converted", "completed", "purchased"]:
            ref_converted[ref] += 1

        scanned += 1
        if scanned % 5000 == 0:
            print(f"Scanned {scanned} rows...")
        if LIMIT is not None and scanned >= LIMIT:
            break

    conn.close()
    print(f"\n✅ Done scanning. Total rows scanned: {scanned}")

    # -------------------------
    # 1) Save conversion status summary
    # -------------------------
    status_df = pd.DataFrame(
        [{"conversion_status": k, "count": v} for k, v in status_counter.items()]
    ).sort_values("count", ascending=False)

    status_df.to_csv(SUMMARY_CSV, index=False)
    print(f"Saved: {SUMMARY_CSV}")

    # Plot conversion status distribution
    plt.figure(figsize=(9, 5))
    plt.bar(status_df["conversion_status"].astype(str), status_df["count"])
    plt.title("Conversion Status Distribution (HBase Sessions)")
    plt.xlabel("Conversion Status")
    plt.ylabel("Number of Sessions")
    plt.xticks(rotation=25, ha="right")
    plt.tight_layout()
    plt.savefig(PLOT_STATUS, dpi=200)
    plt.close()
    print(f"Saved: {PLOT_STATUS}")

    # -------------------------
    # 2) Save referrer-by-status matrix (top referrers per status)
    # -------------------------
    rows = []
    for st, c in ref_by_status.items():
        for ref, cnt in c.items():
            rows.append({"conversion_status": st, "referrer": ref, "count": cnt})

    ref_df = pd.DataFrame(rows).sort_values(["conversion_status", "count"], ascending=[True, False])
    ref_df.to_csv(REFERRER_CSV, index=False)
    print(f"Saved: {REFERRER_CSV}")

    # Plot Top 10 referrers for "converted"
    top_ref = ref_converted.most_common(10)
    if top_ref:
        top_ref_df = pd.DataFrame(top_ref, columns=["referrer", "converted_sessions"])
        plt.figure(figsize=(9, 5))
        plt.bar(top_ref_df["referrer"].astype(str), top_ref_df["converted_sessions"])
        plt.title("Top Referrers (Converted Sessions)")
        plt.xlabel("Referrer")
        plt.ylabel("Converted Sessions")
        plt.xticks(rotation=25, ha="right")
        plt.tight_layout()
        plt.savefig(PLOT_REFERRER, dpi=200)
        plt.close()
        print(f"Saved: {PLOT_REFERRER}")
    else:
        print("⚠️ No 'converted' sessions found (or status name differs).")

    print("\n✅ Analysis complete.")

if __name__ == "__main__":
    main()
