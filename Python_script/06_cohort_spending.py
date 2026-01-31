from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date, date_format, sum as fsum, countDistinct
from pyspark.sql.functions import trunc, months_between, floor

spark = SparkSession.builder.appName("CohortSpending").master("local[*]").getOrCreate()
spark.sparkContext.setLogLevel("WARN")

users = spark.read.json("data_raw/users.json")
tx = spark.read.json("data_raw/transactions.json")

# Adjust these field names if your users.json uses different column names
# Common: user_id, created_at OR registration_date
date_col = None
for c in ["registration_date", "created_at", "signup_date"]:
    if c in users.columns:
        date_col = c
        break

if not date_col:
    raise Exception("No registration date column found in users.json. Expected one of: registration_date, created_at, signup_date")

users2 = (
    users.select("user_id", to_date(col(date_col)).alias("reg_date"))
         .dropna()
         .withColumn("cohort_month", trunc(col("reg_date"), "month"))
)

tx2 = (
    tx.select("user_id", to_date(col("timestamp")).alias("tx_date"), col("total").cast("double").alias("total"))
      .dropna()
      .withColumn("tx_month", trunc(col("tx_date"), "month"))
)

# Join users to transactions
joined = users2.join(tx2, on="user_id", how="inner")

# Month index since cohort start
cohort = (
    joined.withColumn("month_index", floor(months_between(col("tx_month"), col("cohort_month"))))
          .groupBy("cohort_month", "month_index")
          .agg(
              countDistinct("user_id").alias("active_users"),
              fsum("total").alias("total_revenue")
          )
          .orderBy("cohort_month", "month_index")
)

print("\n=== COHORT SPENDING (cohort_month x month_index) ===")
cohort.show(50, truncate=False)

out_path = "outputs/cohort_spending_csv"
cohort.coalesce(1).write.mode("overwrite").option("header", True).csv(out_path)
print(f"✅ Results written to: {out_path}")

spark.stop()
