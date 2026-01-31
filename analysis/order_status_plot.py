import os
import glob
import pandas as pd
import matplotlib.pyplot as plt

# =========================
# PATHS (relative + safe)
# =========================
# analysis/order_status_plot.py  -> BASE_DIR = project root
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
OUTPUTS_DIR = os.path.join(BASE_DIR, "outputs")

# ✅ Your current Spark output location (as you said)
ORDER_STATUS_DIR_PRIMARY = os.path.join(
    OUTPUTS_DIR, "spark_sql_analytics", "order_status_summary_csv"
)

# ✅ Fallback: old expected location (in case someone runs old pipeline)
ORDER_STATUS_DIR_FALLBACK = os.path.join(OUTPUTS_DIR, "order_status_summary_csv")

PLOTS_DIR = os.path.join(OUTPUTS_DIR, "analysis_plots")
os.makedirs(PLOTS_DIR, exist_ok=True)

# =========================
# 1) FIND Spark CSV (part-*.csv)
# =========================
def find_part_csv(folder: str):
    if not os.path.isdir(folder):
        return []
    return glob.glob(os.path.join(folder, "part-*.csv"))

files = find_part_csv(ORDER_STATUS_DIR_PRIMARY)
used_dir = ORDER_STATUS_DIR_PRIMARY

if not files:
    files = find_part_csv(ORDER_STATUS_DIR_FALLBACK)
    used_dir = ORDER_STATUS_DIR_FALLBACK

if not files:
    raise FileNotFoundError(
        "No Spark output found (part-*.csv).\n\n"
        f"Tried:\n"
        f"  1) {ORDER_STATUS_DIR_PRIMARY}\n"
        f"  2) {ORDER_STATUS_DIR_FALLBACK}\n\n"
        "Fix:\n"
        "  - Ensure your Spark SQL job created folder: order_status_summary_csv\n"
        "  - And it contains files like: part-00000-....csv\n"
    )

# Use first part file (usually enough for aggregated results)
df = pd.read_csv(files[0])
print(f"✅ Loaded Spark output from: {used_dir}")
print(f"✅ Using file: {os.path.basename(files[0])}")

# =========================
# 2) Detect columns safely
# =========================
cols = {c.lower().strip(): c for c in df.columns}

def pick(*names):
    for n in names:
        key = n.lower().strip()
        if key in cols:
            return cols[key]
    return None

status_col = pick("status", "order_status")
count_col  = pick("num_transactions", "orders_count", "count", "total_orders", "orders", "order_count")
revenue_col = pick("revenue", "total_revenue", "amount", "total_amount")

if not status_col or not count_col:
    raise ValueError(
        f"Missing required columns. Found columns: {list(df.columns)}\n"
        "Need at least:\n"
        "  - status (or order_status)\n"
        "  - num_transactions / orders_count / count / total_orders / orders\n"
    )

# Clean + sort
df[count_col] = pd.to_numeric(df[count_col], errors="coerce").fillna(0)
df = df.sort_values(by=count_col, ascending=False)

# =========================
# 3) PLOT: Orders by Status
# =========================
plt.figure(figsize=(9, 5))
plt.bar(df[status_col].astype(str), df[count_col])
plt.title("Order Status Distribution")
plt.xlabel("Status")
plt.ylabel("Number of Orders")
plt.xticks(rotation=25, ha="right")

# Add value labels on bars
for i, v in enumerate(df[count_col].tolist()):
    plt.text(i, v, f"{int(v)}", ha="center", va="bottom")

plt.tight_layout()
out_path = os.path.join(PLOTS_DIR, "order_status_distribution.png")
plt.savefig(out_path, dpi=200)
plt.close()
print(f"✅ Saved: {out_path}")

# =========================
# 4) OPTIONAL: Revenue by status
# =========================
if revenue_col:
    df[revenue_col] = pd.to_numeric(df[revenue_col], errors="coerce").fillna(0)

    plt.figure(figsize=(9, 5))
    plt.bar(df[status_col].astype(str), df[revenue_col])
    plt.title("Revenue by Order Status")
    plt.xlabel("Status")
    plt.ylabel("Revenue")
    plt.xticks(rotation=25, ha="right")

    plt.tight_layout()
    out_path2 = os.path.join(PLOTS_DIR, "revenue_by_order_status.png")
    plt.savefig(out_path2, dpi=200)
    plt.close()
    print(f"✅ Saved: {out_path2}")
else:
    print("ℹ️ No revenue column found — saved only the orders-count chart.")
