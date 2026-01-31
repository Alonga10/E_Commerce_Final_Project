import happybase
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("HBaseRead").getOrCreate()
spark.sparkContext.setLogLevel("WARN")

connection = happybase.Connection(
    host="localhost",
    port=9090,
    transport="buffered"   # ✅ add this
)

print("Tables:", connection.tables())

table = connection.table("sessions_clean")

rows = []
for key, data in table.scan(limit=50):
    row = {"session_id": key.decode()}
    for col, val in data.items():
        # col looks like b'cf:user_id' -> take the part after :
        row[col.decode().split(":")[1]] = val.decode(errors="ignore")
    rows.append(row)

connection.close()

df = spark.createDataFrame(rows)
df.show(10, truncate=False)
