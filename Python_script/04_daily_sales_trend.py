from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, sum, to_date

spark = (
    SparkSession.builder
    .appName("DailySalesTrend")
    .master("local[*]")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

# 1) Load transactions
df = spark.read.json("data_raw/transactions.json")

# 2) Extract date from timestamp (timestamp is a string in your data)
df2 = df.withColumn("date", to_date(col("timestamp")))

# 3) Daily trend
daily = (
    df2.groupBy("date")
       .agg(
           count("*").alias("num_transactions"),
           sum("total").alias("daily_revenue")
       )
       .orderBy(col("date").asc())
)

print("\n=== DAILY SALES TREND ===")
daily.show(200, truncate=False)

# 4) Write to CSV (overwrite)
output_path = "outputs/daily_sales_trend_csv"
(
    daily
    .coalesce(1)
    .write
    .mode("overwrite")
    .option("header", True)
    .csv(output_path)
)

print(f"\n✅ Results written to: {output_path}")

spark.stop()
