import pandas as pd
import matplotlib.pyplot as plt
import os

# ===============================
# PATHS
# ===============================
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

CLV_CSV_DIR = os.path.join(
    BASE_DIR,
    "outputs",
    "final_integrated_outputs",
    "csv",
    "customer_lifetime_value"
)

# Spark writes CSV as part-00000*.csv
CLV_CSV_PATH = os.path.join(CLV_CSV_DIR, "part-00000*.csv")

# Output directory for plots (optional but recommended)
PLOTS_DIR = os.path.join(BASE_DIR, "outputs", "analysis_plots")
os.makedirs(PLOTS_DIR, exist_ok=True)

# ===============================
# LOAD CLV DATA
# ===============================
# df = pd.read_csv(CLV_CSV_PATH)

df = pd.read_csv(r"C:\Users\mpalo\Documents\BIG DATA CLASS\BIG DATA ANALYTICS\Final\E_Commerce_Final_Project\outputs\final_integrated_outputs\csv\customer_lifetime_value\part-00000-1d963e0f-82fc-40ec-bcfa-ac985d1eebd0-c000.csv")



print("CLV dataset shape:", df.shape)
print(df.head())

# ===============================
# TOP 10 CUSTOMERS BY CLV
# ===============================
top10_clv = df.sort_values("clv", ascending=False).head(10)

print("\nTop 10 Customers by CLV:")
print(top10_clv[["user_id", "clv", "total_orders", "total_sessions"]])

# ===============================
# PLOT 1: TOP 10 CLV CUSTOMERS
# ===============================
plt.figure(figsize=(10, 5))
plt.bar(top10_clv["user_id"], top10_clv["clv"])
plt.xticks(rotation=45)
plt.xlabel("User ID")
plt.ylabel("Customer Lifetime Value (CLV)")
plt.title("Top 10 Customers by CLV")
plt.tight_layout()
plt.savefig(os.path.join(PLOTS_DIR, "top10_customers_by_clv.png"))
plt.close()

# ===============================
# PLOT 2: CLV DISTRIBUTION
# ===============================
# ===============================
# PLOT 2: CLV DISTRIBUTION (SCALED + GRIDLINES)
# ===============================
# ===============================
# PLOT 2: CLV DISTRIBUTION WITH VERTICAL LIMIT LINES
# ===============================
plt.figure(figsize=(9, 5))

plt.hist(
    df["clv"] / 1_000_000,
    bins=25,
    edgecolor="white",      # ✅ white vertical separators
    linewidth=1.2
)

plt.xlabel("Customer Lifetime Value (Million Units)")
plt.ylabel("Number of Customers")
plt.title("Distribution of Customer Lifetime Value (CLV)")

plt.tight_layout()
plt.savefig(os.path.join(PLOTS_DIR, "clv_distribution.png"))
plt.close()

# ===============================
# CLV SEGMENTATION (LOW / MEDIUM / HIGH)
# ===============================

# Scale CLV for readability
df["clv_millions"] = df["clv"] / 1_000_000

# Define thresholds using quantiles
low_thr = df["clv_millions"].quantile(0.33)
high_thr = df["clv_millions"].quantile(0.66)

# Assign segments
def clv_segment(value):
    if value <= low_thr:
        return "Low CLV"
    elif value <= high_thr:
        return "Medium CLV"
    else:
        return "High CLV"

df["clv_segment"] = df["clv_millions"].apply(clv_segment)

# Count customers per segment
segment_counts = df["clv_segment"].value_counts().reindex(
    ["Low CLV", "Medium CLV", "High CLV"]
)

print("\nCLV Segmentation Counts:")
print(segment_counts)

# ===============================
# PLOT: CLV SEGMENTATION BAR CHART
# ===============================

plt.figure(figsize=(7, 5))
plt.bar(
    segment_counts.index,
    segment_counts.values,
    edgecolor="white",
    linewidth=1.2
)

plt.xlabel("CLV Segment")
plt.ylabel("Number of Customers")
plt.title("Customer Segmentation by Lifetime Value (CLV)")

plt.tight_layout()
plt.savefig(os.path.join(PLOTS_DIR, "clv_segmentation.png"))
plt.close()


# ===============================
# SUMMARY STATISTICS
# ===============================
summary = {
    "total_customers": len(df),
    "average_clv": df["clv"].mean(),
    "median_clv": df["clv"].median(),
    "max_clv": df["clv"].max()
}

print("\nCLV Summary Statistics:")
for k, v in summary.items():
    print(f"{k}: {v:,.2f}")

print("\nCLV analysis completed successfully!")
