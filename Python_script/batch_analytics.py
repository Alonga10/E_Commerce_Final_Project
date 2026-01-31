import os
from pyspark.sql.functions import lit

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, explode, to_timestamp, to_date,
    sum as _sum, count as _count, desc
)

# =========================
# PATHS (match your project)
# =========================
RAW_DIR = "data_raw"
OUT_DIR = "outputs/batch_analytics"   # keep separate from your older outputs

def ensure_dir(path: str):
    os.makedirs(path, exist_ok=True)

def write_parquet_and_csv(df, name: str):
    """
    Save output in both Parquet (best for Spark) and CSV (easy to open).
    """
    ensure_dir(OUT_DIR)
    parquet_path = f"{OUT_DIR}/{name}_parquet"
    csv_path = f"{OUT_DIR}/{name}_csv"

    df.write.mode("overwrite").parquet(parquet_path)
    df.coalesce(1).write.mode("overwrite").option("header", True).csv(csv_path)

def pick_col(df_cols, candidates):
    """
    Return first matching column name from candidates list.
    Helps handle small schema variations.
    """
    cols_lower = {c.lower(): c for c in df_cols}
    for cand in candidates:
        if cand.lower() in cols_lower:
            return cols_lower[cand.lower()]
    return None

def main():
    spark = (
        SparkSession.builder
        .appName("EcommerceBatchAnalytics_JSON")
        .getOrCreate()
    )

    # -------------------------
    # 1) Read JSON datasets
    # -------------------------
    transactions = spark.read.json(f"{RAW_DIR}/transactions.json")
    products     = spark.read.json(f"{RAW_DIR}/products.json")
    categories   = spark.read.json(f"{RAW_DIR}/categories.json")

    # Quick debug (optional)
    # transactions.printSchema()
    # transactions.show(5, truncate=False)

    # -------------------------
    # 2) Detect key columns safely
    # -------------------------
    # transactions expected: items, timestamp, payment_method, status, transaction_id, user_id, total/subtotal/discount
    ts_col = pick_col(transactions.columns, ["timestamp", "event_time", "time"])
    pm_col = pick_col(transactions.columns, ["payment_method", "payment", "pay_method"])
    st_col = pick_col(transactions.columns, ["status"])
    tid_col = pick_col(transactions.columns, ["transaction_id", "txn_id", "id"])
    uid_col = pick_col(transactions.columns, ["user_id", "customer_id"])

    # products expected: product_id, product_name, category_id
    pid_col = pick_col(products.columns, ["product_id", "id", "pid"])
    pname_col = pick_col(products.columns, ["product_name", "name", "title"])
    catid_col_products = pick_col(products.columns, ["category_id", "category", "cat_id"])

    # categories expected: category_id, category_name
    catid_col_categories = pick_col(categories.columns, ["category_id", "id", "cat_id"])
    catname_col = pick_col(categories.columns, ["category_name", "name", "category"])

    # Hard checks (fail with clear message)
    if ts_col is None or tid_col is None or uid_col is None:
        raise ValueError(f"Transactions missing required columns. Found: {transactions.columns}")

    if pid_col is None:
        raise ValueError(f"Products missing product_id-like column. Found: {products.columns}")

    if catid_col_products is None:
        raise ValueError(f"Products missing category_id-like column. Found: {products.columns}")

    if catid_col_categories is None:
        raise ValueError(f"Categories missing category_id-like column. Found: {categories.columns}")

    # -------------------------
    # 3) Add parsed timestamp + date
    # -------------------------
    tx = (
        transactions
        .withColumn("ts", to_timestamp(col(ts_col)))
        .withColumn("date", to_date(col("ts")))
    )

    # -------------------------
    # 4) Normalize items array
    # items is array<struct<...>> (your print shows {prod_00537, 1, 269.68, 269.68})
    # We'll explode then extract struct fields WITHOUT assuming field names.
    #
    # Trick: convert struct to string columns by selecting struct.* dynamically.
    # -------------------------
    tx_exploded = tx.select(
        col(tid_col).alias("transaction_id"),
        col(uid_col).alias("user_id"),
        (col(pm_col).alias("payment_method") if pm_col else col(lit(None)).alias("payment_method")),
        (col(st_col).alias("status") if st_col else col(lit(None)).alias("status")),
        col("date"),
        explode(col("items")).alias("item")
    )

    # Get item struct fields dynamically
    item_fields = tx_exploded.select("item.*").columns
    if len(item_fields) < 4:
        raise ValueError(
            f"Unexpected items struct shape. item.* fields: {item_fields} "
            "Expected at least 4 fields (product_id, qty, unit_price, line_total)."
        )

    # Assume first 4 positions map to: product_id, quantity, unit_price, line_total
    # (matches what your table shows)
    prod_f, qty_f, unit_f, total_f = item_fields[0], item_fields[1], item_fields[2], item_fields[3]

    tx_items = (
        tx_exploded
        .select(
            "transaction_id",
            "user_id",
            "payment_method",
            "status",
            "date",
            col(f"item.{prod_f}").alias("product_id"),
            col(f"item.{qty_f}").cast("int").alias("quantity"),
            col(f"item.{unit_f}").cast("double").alias("unit_price"),
            col(f"item.{total_f}").cast("double").alias("line_total"),
        )
        .filter(col("product_id").isNotNull() & col("line_total").isNotNull())
    )

    # -------------------------
    # 5) Prepare product + category tables (rename columns consistently)
    # -------------------------
    products_clean = (
        products
        .select(
            col(pid_col).alias("product_id"),
            (col(pname_col).alias("product_name") if pname_col else col(pid_col).alias("product_name")),
            col(catid_col_products).alias("category_id")
        )
    )

    categories_clean = (
        categories
        .select(
            col(catid_col_categories).alias("category_id"),
            (col(catname_col).alias("category_name") if catname_col else col(catid_col_categories).alias("category_name"))
        )
    )

    # -------------------------
    # 6) Join for enriched analytics
    # -------------------------
    tx_enriched = (
        tx_items
        .join(products_clean, on="product_id", how="left")
        .join(categories_clean, on="category_id", how="left")
    )

    # -------------------------
    # 7) Analytics outputs
    # -------------------------
    daily_revenue = (
        tx_enriched.groupBy("date")
        .agg(
            _sum("line_total").alias("daily_revenue"),
            _count("*").alias("items_count")
        )
        .orderBy("date")
    )

    top_products = (
        tx_enriched.groupBy("product_id", "product_name")
        .agg(
            _sum("line_total").alias("total_revenue"),
            _sum("quantity").alias("total_qty")
        )
        .orderBy(desc("total_revenue"))
        .limit(20)
    )

    revenue_by_category = (
        tx_enriched.groupBy("category_id", "category_name")
        .agg(_sum("line_total").alias("category_revenue"))
        .orderBy(desc("category_revenue"))
    )

    payment_method_revenue = (
        tx_enriched.groupBy("payment_method")
        .agg(_sum("line_total").alias("payment_method_revenue"))
        .orderBy(desc("payment_method_revenue"))
    )

    # -------------------------
    # 8) Save outputs
    # -------------------------
    write_parquet_and_csv(daily_revenue, "daily_revenue")
    write_parquet_and_csv(top_products, "top_products")
    write_parquet_and_csv(revenue_by_category, "revenue_by_category")
    write_parquet_and_csv(payment_method_revenue, "payment_method_revenue")

    # Small console preview
    daily_revenue.show(10, truncate=False)
    top_products.show(10, truncate=False)
    revenue_by_category.show(10, truncate=False)
    payment_method_revenue.show(10, truncate=False)

    spark.stop()

if __name__ == "__main__":
    main()
