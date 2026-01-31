import os
from glob import glob

import pandas as pd
import matplotlib.pyplot as plt


# ===============================
# PATHS
# ===============================
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
OUTPUTS_DIR = os.path.join(BASE_DIR, "outputs")
PLOTS_DIR = os.path.join(OUTPUTS_DIR, "analysis_plots")
os.makedirs(PLOTS_DIR, exist_ok=True)


def first_match(pattern: str) -> str:
    files = glob(pattern)
    if not files:
        raise FileNotFoundError(f"No files found for pattern:\n  {pattern}")
    return files[0]


def save_clv_segmentation():
    print("2️⃣ Generating CLV segmentation plot...")

    clv_path = first_match(
        os.path.join(
            OUTPUTS_DIR,
            "final_integrated_outputs",
            "csv",
            "customer_lifetime_value",
            "part-*.csv",
        )
    )
    print(f"   Using CLV file: {clv_path}")

    clv_df = pd.read_csv(clv_path)
    print(f"   CLV columns: {list(clv_df.columns)}")

    if "clv" not in clv_df.columns:
        raise ValueError(f"CLV CSV missing 'clv'. Found: {list(clv_df.columns)}")

    clv_df["clv"] = pd.to_numeric(clv_df["clv"], errors="coerce").fillna(0)
    clv_df = clv_df[clv_df["clv"] > 0].copy()

    low = clv_df["clv"].quantile(0.33)
    high = clv_df["clv"].quantile(0.66)

    def segment(v):
        if v <= low:
            return "Low CLV"
        elif v <= high:
            return "Medium CLV"
        return "High CLV"

    clv_df["segment"] = clv_df["clv"].apply(segment)
    counts = (
        clv_df["segment"]
        .value_counts()
        .reindex(["Low CLV", "Medium CLV", "High CLV"])
        .fillna(0)
    )

    out_path = os.path.join(PLOTS_DIR, "clv_segmentation.png")

    plt.figure(figsize=(7, 5))
    plt.bar(counts.index, counts.values, edgecolor="black", linewidth=1.0)
    plt.xlabel("CLV Segment")
    plt.ylabel("Number of Customers")
    plt.title("Customer Segmentation by Lifetime Value")
    plt.grid(axis="y", alpha=0.25)
    plt.tight_layout()
    plt.savefig(out_path, dpi=200)
    plt.close()

    print(f"✅ Saved: {out_path}")


def save_sales_trend():
    print("1️⃣ Generating sales trend plot...")

    sales_path = first_match(os.path.join(OUTPUTS_DIR, "daily_sales_trend_csv", "part-*.csv"))
    sales_df = pd.read_csv(sales_path)

    sales_df["date"] = pd.to_datetime(sales_df["date"], errors="coerce")
    sales_df = sales_df.sort_values("date")

    rev_candidates = ["daily_revenue", "revenue", "total_revenue", "sales"]
    rev_col = next((c for c in rev_candidates if c in sales_df.columns), None)
    if rev_col is None:
        raise ValueError(f"Sales CSV missing revenue column. Found: {list(sales_df.columns)}")

    sales_df[rev_col] = pd.to_numeric(sales_df[rev_col], errors="coerce").fillna(0)

    out_path = os.path.join(PLOTS_DIR, "daily_sales_trend.png")
    plt.figure(figsize=(9, 5))
    plt.plot(sales_df["date"], sales_df[rev_col])
    plt.xlabel("Date")
    plt.ylabel("Revenue")
    plt.title("Daily Sales Performance Over Time")
    plt.grid(alpha=0.3)
    plt.tight_layout()
    plt.savefig(out_path, dpi=200)
    plt.close()

    print(f"✅ Saved: {out_path}")


def save_top_products():
    print("3️⃣ Generating product performance plot...")

    products_path = first_match(os.path.join(OUTPUTS_DIR, "top_products_csv", "part-*.csv"))
    products_df = pd.read_csv(products_path)

    revenue_candidates = ["revenue", "total_revenue", "sales", "total_sales", "amount", "total_amount"]
    name_candidates = ["product_name", "name", "product", "product_id", "productId", "prod_id"]

    revenue_col = next((c for c in revenue_candidates if c in products_df.columns), None)
    name_col = next((c for c in name_candidates if c in products_df.columns), None)

    if revenue_col is None or name_col is None:
        raise ValueError(
            f"Products CSV missing columns. Found: {list(products_df.columns)}\n"
            f"Need one of {revenue_candidates} and one of {name_candidates}."
        )

    products_df[revenue_col] = pd.to_numeric(products_df[revenue_col], errors="coerce").fillna(0)
    top10 = products_df.sort_values(revenue_col, ascending=False).head(10)

    out_path = os.path.join(PLOTS_DIR, "top_products_revenue_top10.png")
    plt.figure(figsize=(9, 5))
    plt.barh(top10[name_col].astype(str), top10[revenue_col], edgecolor="black", linewidth=0.8)
    plt.xlabel(revenue_col)
    plt.title("Top 10 Products by Revenue")
    plt.gca().invert_yaxis()
    plt.grid(axis="x", alpha=0.25)
    plt.tight_layout()
    plt.savefig(out_path, dpi=200)
    plt.close()

    print(f"✅ Saved: {out_path}")


if __name__ == "__main__":
    print(f"BASE_DIR  = {BASE_DIR}")
    print(f"OUTPUTS   = {OUTPUTS_DIR}")
    print(f"PLOTS_DIR = {PLOTS_DIR}")

    # CLV FIRST so it will still save even if something else breaks later
    try:
        save_clv_segmentation()
    except Exception as e:
        print(f"❌ CLV plot failed: {e}")

    try:
        save_sales_trend()
    except Exception as e:
        print(f"❌ Sales plot failed: {e}")

    try:
        save_top_products()
    except Exception as e:
        print(f"❌ Products plot failed: {e}")

    print("✅ Done.")
