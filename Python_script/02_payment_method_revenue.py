from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum as _sum, count as _count, round as _round

spark = (
    SparkSession.builder
    .appName("02_PaymentMethodRevenue")
    .master("local[*]")
    .getOrCreate()
)

df = spark.read.option("multiLine", True).json("data_raw/transactions.json")

result = (
    df.groupBy("payment_method")
    .agg(
        _count("*").alias("num_transactions"),
        _round(_sum("total"), 2).alias("total_revenue"),
        _round((_sum("total") / _count("*")), 2).alias("avg_order_value")
    )
    .orderBy(col("total_revenue").desc())
)

print("\n=== REVENUE BY PAYMENT METHOD ===")
result.show(truncate=False)

result.coalesce(1).write.mode("overwrite").option("header", True).csv("outputs/payment_method_revenue_csv")

spark.stop()
