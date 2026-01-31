from pyspark.sql import SparkSession
from pyspark.sql.types import ArrayType, StructType
from pyspark.sql.functions import (
    col, trim, lower, regexp_replace, to_timestamp, when,
    size, to_json
)

spark = (
    SparkSession.builder
    .appName("CleanNormalizeSessions")
    .master("local[*]")
    .config("spark.sql.debug.maxToStringFields", "200")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")

# Pick the correct sessions file (you have sessions_0.json)
CANDIDATES = ["data_raw/sessions.json", "data_raw/sessions_0.json", "data_raw/sessions*.json"]
INPUT_PATH = None
for p in CANDIDATES:
    try:
        spark.read.json(p).limit(1).count()
        INPUT_PATH = p
        break
    except Exception:
        pass

if INPUT_PATH is None:
    raise FileNotFoundError("No sessions file found in data_raw (sessions.json / sessions_0.json).")

OUT_CSV = "outputs/sessions_clean_csv"

sessions = spark.read.json(INPUT_PATH)

def clean_text(c):
    return when(col(c).isNull(), None).otherwise(
        lower(trim(regexp_replace(col(c).cast("string"), r"\s+", " ")))
    )

# timestamps
sessions = sessions.withColumn("start_ts", to_timestamp(col("start_time"))) \
                   .withColumn("end_ts", to_timestamp(col("end_time")))

# duration
if "duration_seconds" in sessions.columns:
    sessions = sessions.withColumn("duration_seconds", col("duration_seconds").cast("bigint"))
else:
    sessions = sessions.withColumn("duration_seconds", when(col("session_id").isNull(), None).otherwise(None))

# ---- FIX: page_views_count based on real schema type ----
page_views_count_expr = when(col("page_views").isNull(), None)

if "page_views" in sessions.columns:
    pv_type = sessions.schema["page_views"].dataType

    # Your case: array<struct<category_id,...>>
    if isinstance(pv_type, ArrayType):
        page_views_count_expr = when(col("page_views").isNull(), None).otherwise(size(col("page_views")))

    # If it ever becomes struct (rare)
    elif isinstance(pv_type, StructType):
        page_views_count_expr = when(col("page_views").isNull(), None).otherwise(1)

    # Otherwise try numeric/string
    else:
        page_views_count_expr = when(col("page_views").cast("bigint").isNotNull(), col("page_views").cast("bigint")) \
            .otherwise(None)

sessions = sessions.withColumn("page_views_count", page_views_count_expr)

# viewed_products -> count
if "viewed_products" in sessions.columns:
    sessions = sessions.withColumn(
        "viewed_products_count",
        when(col("viewed_products").isNull(), None).otherwise(size(col("viewed_products")))
    )
else:
    sessions = sessions.withColumn("viewed_products_count", when(col("session_id").isNull(), None).otherwise(None))

# geo_data struct -> JSON string so CSV can write
if "geo_data" in sessions.columns:
    gd_type = sessions.schema["geo_data"].dataType
    if isinstance(gd_type, StructType):
        sessions = sessions.withColumn("geo_data", to_json(col("geo_data")))
    else:
        sessions = sessions.withColumn("geo_data", col("geo_data").cast("string"))
else:
    sessions = sessions.withColumn("geo_data", when(col("session_id").isNull(), None).otherwise(None))

sessions_clean = sessions.select(
    col("session_id"),
    col("user_id"),
    col("start_ts"),
    col("end_ts"),
    col("duration_seconds"),
    clean_text("conversion_status").alias("conversion_status"),
    clean_text("device_profile").alias("device_profile"),
    clean_text("referrer").alias("referrer"),
    col("page_views_count"),
    col("viewed_products_count"),
    col("geo_data")
)

print("\n=== CLEANED SESSIONS (preview) ===")
sessions_clean.show(10, truncate=True)

(
    sessions_clean
    .coalesce(1)
    .write
    .mode("overwrite")
    .option("header", True)
    .csv(OUT_CSV)
)

print(f"\n✅ Results written to: {OUT_CSV}")
spark.stop()
