from pyspark.sql import SparkSession
from pyspark.sql.functions import count, sum, avg, col

# -------------------------------------------------
# 1) Create Spark session
# -------------------------------------------------
spark = (
    SparkSession.builder
    .appName("OrderStatusAnalysis")
    .master("local[*]")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

# -------------------------------------------------
# 2) Load transactions data
# -------------------------------------------------
df = spark.read.json("data_raw/transactions.json")

# -------------------------------------------------
# 3) Order status analytics
# -------------------------------------------------
result = (
    df.groupBy("status")
      .agg(
          count("*").alias("num_transactions"),
          sum("total").alias("total_revenue"),
          avg("total").alias("avg_order_value")
      )
      .orderBy(col("num_transactions").desc())
)

# -------------------------------------------------
# 4) Show results in terminal
# -------------------------------------------------
print("\n=== ORDER STATUS ANALYSIS ===")
result.show(truncate=False)

# -------------------------------------------------
# 5) Write results to CSV (OVERWRITE SAFELY)
# -------------------------------------------------
output_path = "outputs/order_status_summary_csv"

(
    result
    .coalesce(1)              # optional: single CSV file
    .write
    .mode("overwrite")        # ✅ THIS FIXES YOUR ISSUE
    .option("header", True)
    .csv(output_path)
)

print(f"\n✅ Results written to: {output_path}")

# -------------------------------------------------
# 6) Stop Spark
# -------------------------------------------------
spark.stop()
