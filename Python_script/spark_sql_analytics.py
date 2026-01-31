import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, to_timestamp, to_date, explode, sum as _sum, count as _count, desc
)
from pyspark.sql.types import StructType, ArrayType


# -----------------------------
# CONFIG (relative to project root)
# -----------------------------
RAW_DIR = "data_raw"
OUT_DIR = os.path.join("outputs", "spark_sql_analytics")


def ensure_dir(path: str):
    os.makedirs(path, exist_ok=True)


def write_csv(df, name: str):
    out = os.path.join(OUT_DIR, f"{name}_csv")
    df.coalesce(1).write.mode("overwrite").option("header", True).csv(out)
    print(f"Saved: {out}")


def write_parquet(df, name: str):
    out = os.path.join(OUT_DIR, f"{name}_parquet")
    df.write.mode("overwrite").parquet(out)
    print(f"Saved: {out}")


def abs_path_from_project_root(rel_path: str) -> str:
    # file is in spark_jobs/, project root is one level up
    base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    return os.path.join(base_dir, rel_path)


def read_json(spark: SparkSession, rel_path: str):
    path = abs_path_from_project_root(rel_path)
    if not os.path.exists(path):
        raise FileNotFoundError(f"Missing file: {path}")
    return spark.read.json(path)


def main():
    ensure_dir(OUT_DIR)

    spark = (
        SparkSession.builder
        .appName("EcommerceSparkSQLAnalytics")
        # a bit more driver memory (helps on Windows)
        .config("spark.driver.memory", "4g")
        .getOrCreate()
    )

    # -----------------------------
    # 1) READ DATA
    # -----------------------------
    transactions = read_json(spark, os.path.join(RAW_DIR, "transactions.json"))
    products      = read_json(spark, os.path.join(RAW_DIR, "products.json"))
    categories    = read_json(spark, os.path.join(RAW_DIR, "categories.json"))

    # basic typing for tx
    tx = (
        transactions
        .withColumn("ts", to_timestamp(col("timestamp")))
        .withColumn("date", to_date(col("ts")))
        .withColumn("total", col("total").cast("double"))
    )

    # -----------------------------
    # 2) NORMALIZE ITEMS SAFELY
    #    Supports:
    #    - items: array<struct<product_id, quantity, subtotal, unit_price>>
    #    - items: array<array<...>>  (fallback)
    # -----------------------------
    if "items" not in tx.columns:
        raise ValueError("transactions has no 'items' column")

    # Determine element type of items
    items_dtype = tx.schema["items"].dataType
    element_type = None
    if isinstance(items_dtype, ArrayType):
        element_type = items_dtype.elementType

    tx_items_tmp = (
        tx
        .withColumn("item", explode(col("items")))
        .select(
            col("transaction_id"),
            col("user_id"),
            col("payment_method"),
            col("status"),
            col("date"),
            col("total"),
            col("item")
        )
    )

    # Case A: item is STRUCT
    if isinstance(element_type, StructType):
        fields = [f.name for f in element_type.fields]
        # We map: line_total <- subtotal (your generator uses subtotal)
        required = {"product_id", "quantity", "unit_price"}
        if not required.issubset(set(fields)):
            raise ValueError(f"items struct fields are unexpected: {fields}")

        # subtotal might exist (common), if not, we compute line_total = quantity * unit_price
        has_subtotal = "subtotal" in fields

        tx_items = (
            tx_items_tmp
            .withColumn("product_id", col("item").getField("product_id"))
            .withColumn("quantity",   col("item").getField("quantity").cast("int"))
            .withColumn("unit_price", col("item").getField("unit_price").cast("double"))
        )

        if has_subtotal:
            tx_items = tx_items.withColumn("line_total", col("item").getField("subtotal").cast("double"))
        else:
            tx_items = tx_items.withColumn("line_total", col("quantity") * col("unit_price"))

        tx_items = tx_items.drop("item")

    # Case B: item is ARRAY (fallback)
    else:
        # expected order: [product_id, qty, line_total, unit_price] (based on your earlier display)
        tx_items = (
            tx_items_tmp
            .withColumn("product_id", col("item").getItem(0))
            .withColumn("quantity",   col("item").getItem(1).cast("int"))
            .withColumn("line_total", col("item").getItem(2).cast("double"))
            .withColumn("unit_price", col("item").getItem(3).cast("double"))
            .drop("item")
        )

    # -----------------------------
    # 3) CREATE TEMP VIEWS
    # -----------------------------
    tx.createOrReplaceTempView("transactions")
    tx_items.createOrReplaceTempView("transaction_items")
    products.createOrReplaceTempView("products")
    categories.createOrReplaceTempView("categories")

    # -----------------------------
    # 4) SPARK SQL QUERIES
    # -----------------------------
    q_daily_revenue = """
    SELECT date, SUM(total) AS daily_revenue
    FROM transactions
    WHERE status IN ('completed','shipped')
      AND date IS NOT NULL
    GROUP BY date
    ORDER BY date
    """

    q_payment_method = """
    SELECT payment_method, SUM(total) AS payment_method_revenue
    FROM transactions
    WHERE status IN ('completed','shipped')
    GROUP BY payment_method
    ORDER BY payment_method_revenue DESC
    """

    q_top_products = """
    SELECT
      ti.product_id,
      p.name AS product_name,
      SUM(ti.quantity) AS total_units,
      SUM(ti.line_total) AS revenue
    FROM transaction_items ti
    LEFT JOIN products p ON p.product_id = ti.product_id
    GROUP BY ti.product_id, p.name
    ORDER BY revenue DESC
    LIMIT 20
    """

    q_revenue_by_category = """
    SELECT
      c.category_id,
      c.name AS category_name,
      SUM(ti.line_total) AS revenue
    FROM transaction_items ti
    LEFT JOIN products p ON p.product_id = ti.product_id
    LEFT JOIN categories c ON c.category_id = p.category_id
    GROUP BY c.category_id, c.name
    ORDER BY revenue DESC
    """

    q_order_status_summary = """
    SELECT status, COUNT(*) AS orders_count, SUM(total) AS revenue
    FROM transactions
    GROUP BY status
    ORDER BY orders_count DESC
    """

    daily_revenue = spark.sql(q_daily_revenue)
    payment_method_revenue = spark.sql(q_payment_method)
    top_products = spark.sql(q_top_products)
    revenue_by_category = spark.sql(q_revenue_by_category)
    order_status_summary = spark.sql(q_order_status_summary)

    # -----------------------------
    # 5) SAVE OUTPUTS
    # -----------------------------
    print("\nDaily revenue:")
    daily_revenue.show(20, truncate=False)
    write_csv(daily_revenue, "daily_revenue")
    write_parquet(daily_revenue, "daily_revenue")

    print("\nPayment method revenue:")
    payment_method_revenue.show(50, truncate=False)
    write_csv(payment_method_revenue, "payment_method_revenue")
    write_parquet(payment_method_revenue, "payment_method_revenue")

    print("\nTop products:")
    top_products.show(20, truncate=False)
    write_csv(top_products, "top_products")
    write_parquet(top_products, "top_products")

    print("\nRevenue by category:")
    revenue_by_category.show(50, truncate=False)
    write_csv(revenue_by_category, "revenue_by_category")
    write_parquet(revenue_by_category, "revenue_by_category")

    print("\nOrder status summary:")
    order_status_summary.show(50, truncate=False)
    write_csv(order_status_summary, "order_status_summary")
    write_parquet(order_status_summary, "order_status_summary")

    print(f"\nAll outputs saved under: {OUT_DIR}")
    spark.stop()


if __name__ == "__main__":
    main()
