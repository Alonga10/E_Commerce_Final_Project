import sys
import io
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8")


import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, desc, to_timestamp, to_date

# =========================
# PATHS
# =========================
RAW_DIR = "data_raw"
OUT_DIR = "outputs/batch_sessions"   # separate folder (no overwrite)

def ensure_dir(path: str):
    os.makedirs(path, exist_ok=True)

def write_csv(df, name: str):
    ensure_dir(OUT_DIR)
    out_path = f"{OUT_DIR}/{name}_csv"
    df.coalesce(1).write.mode("overwrite").option("header", True).csv(out_path)
    print(f"Saved: {out_path}")

def main():
    spark = (
        SparkSession.builder
        .appName("ECommerce-Spark-Batch-Sessions")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    # ---------------------------
    # 1) Read sessions JSON
    # ---------------------------
    # Always resolve from the script location (works even if you run from another folder)
    BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))  # project root
  
    
    sessions_pattern = os.path.join(BASE_DIR, RAW_DIR, "sessions_*.json")
    sessions_uri = "file:///" + sessions_pattern.replace("\\", "/")

    print("Spark will read:", sessions_uri)

    #sessions_df = spark.read.json(sessions_uri).cache()
    sessions_df = spark.read.json(sessions_uri)

    
    print(" Sessions schema:")
    sessions_df.printSchema()
    print(" Sample sessions:")
    sessions_df.select("session_id", "user_id", "start_time").show(10, truncate=True)

    # sessions_df.show(10, truncate=False)

    # ---------------------------
    # 2) Basic cleaning / date (optional)
    # ---------------------------
    # Try to detect a timestamp column commonly used in sessions data
    ts_col = None
    for c in ["timestamp", "start_time", "event_time", "created_at", "ts"]:
        if c in sessions_df.columns:
            ts_col = c
            break

    if ts_col:
        sessions_df = (
            sessions_df
            .withColumn("ts", to_timestamp(col(ts_col)))
            .withColumn("date", to_date(col("ts")))
        )

    # ---------------------------
    # 3) Analytics examples (Session-level)
    #    A) Top devices used
    #    B) Top users by sessions
    #    C) Sessions per day (if date exists)
    # ---------------------------
    if "device" in sessions_df.columns:
        top_devices = (
            sessions_df.groupBy("device")
            .agg(count("*").alias("sessions_count"))
            .orderBy(desc("sessions_count"))
        )
        print(" Top devices:")
        top_devices.show(20, truncate=False)
        write_csv(top_devices, "top_devices")
        
        print("Columns:", sessions_df.columns)


    if "user_id" in sessions_df.columns:
        top_users = (
            sessions_df.groupBy("user_id")
            .agg(count("*").alias("sessions_count"))
            .orderBy(desc("sessions_count"))
        )
        print(" Top users by sessions:")
        top_users.show(20, truncate=False)
        write_csv(top_users, "top_users")

    if "date" in sessions_df.columns:
        sessions_per_day = (
            sessions_df.groupBy("date")
            .agg(count("*").alias("sessions_count"))
            .orderBy("date")
        )
        print(" Sessions per day:")
        sessions_per_day.show(20, truncate=False)
        write_csv(sessions_per_day, "sessions_per_day")

    print(f" All outputs saved under: {OUT_DIR}/")
    spark.stop()

if __name__ == "__main__":
    main()
