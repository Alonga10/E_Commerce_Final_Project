import sys
import io
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8")


import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import sum as _sum, countDistinct, col, round, min as _min, max as _max

# ============================================================
# PATHS (MATCH YOUR PROJECT STRUCTURE)
# ============================================================

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
OUTPUTS_DIR = os.path.join(BASE_DIR, "outputs")

SESSIONS_PARQUET = os.path.join(OUTPUTS_DIR, "sessions_clean_parquet")
REVENUE_PARQUET = os.path.join(
    OUTPUTS_DIR,
    "spark_sql_analytics",
    "order_status_summary_parquet"
)

FINAL_DIR = os.path.join(OUTPUTS_DIR, "final_integrated_outputs")
CSV_OUT = os.path.join(FINAL_DIR, "csv", "customer_lifetime_value")
PARQUET_OUT = os.path.join(FINAL_DIR, "parquet", "customer_lifetime_value")

os.makedirs(os.path.dirname(CSV_OUT), exist_ok=True)
os.makedirs(os.path.dirname(PARQUET_OUT), exist_ok=True)

# ============================================================
# SPARK SESSION
# ============================================================

spark = (
    SparkSession.builder
    .appName("CLV Integration")
    .getOrCreate()
)

# ============================================================
# 1) READ DATA
# ============================================================

print("📥 Reading session data...")
sessions = spark.read.parquet(SESSIONS_PARQUET)
sessions.printSchema()

print("📥 Reading revenue data...")
revenue = spark.read.parquet(REVENUE_PARQUET)
revenue.printSchema()

# ============================================================
# 2) AGGREGATE SESSIONS PER USER
# ============================================================

print("📊 Aggregating sessions per user...")

sessions_user = (
    sessions
    .filter(col("user_id").isNotNull())
    .groupBy("user_id")
    .agg(
        countDistinct("session_id").alias("total_sessions"),
        _min("start_ts").alias("first_session"),
        _max("end_ts").alias("last_session")
    )
)

# ============================================================
# 3) AGGREGATE REVENUE PER USER
# ============================================================
# NOTE: order_status_summary has NO user_id,
# so we join via sessions

print("💰 Aggregating revenue per user...")

revenue_completed = revenue.filter(col("status") == "completed")

revenue_user = (
    sessions
    .select("user_id", "session_id")
    .join(revenue_completed, how="inner")
    .groupBy("user_id")
    .agg(
        _sum("revenue").alias("total_revenue"),
        _sum("orders_count").alias("total_orders")
    )
)

# ============================================================
# 4) COMPUTE CLV
# ============================================================

print("🔗 Computing Customer Lifetime Value (CLV)...")

clv = (
    sessions_user
    .join(revenue_user, on="user_id", how="left")
    .fillna(0)
    .withColumn("clv", round(col("total_revenue"), 2))
    .orderBy(col("clv").desc())
)

clv.show(10, truncate=False)

# ============================================================
# 5) SAVE OUTPUTS
# ============================================================

print("💾 Saving CLV outputs...")

clv.write.mode("overwrite").option("header", True).csv(CSV_OUT)
clv.write.mode("overwrite").parquet(PARQUET_OUT)

print("✅ CLV integration completed successfully!")

spark.stop()

