from pyspark.sql import SparkSession
from pyspark.sql.functions import explode, col, sum as _sum, round as _round

spark = (
    SparkSession.builder
    .appName("01_TopProducts")
    .master("local[*]")
    .getOrCreate()
)

# Read transactions JSON (array of objects)
df = spark.read.option("multiLine", True).json("data_raw/transactions.json")

# items is an array -> explode it
items = df.select(explode(col("items")).alias("item"))

agg = (
    items.select(
        col("item.product_id").alias("product_id"),
        col("item.quantity").alias("qty"),
        col("item.subtotal").alias("subtotal"),
    )
    .groupBy("product_id")
    .agg(
        _sum("qty").alias("total_qty"),
        _round(_sum("subtotal"), 2).alias("total_revenue"),
    )
    .orderBy(col("total_qty").desc())
)

print("\n=== TOP 10 PRODUCTS (by quantity) ===")
agg.show(10, truncate=False)

# Save result
agg.coalesce(1).write.mode("overwrite").option("header", True).csv("outputs/top_products_csv")

spark.stop()
