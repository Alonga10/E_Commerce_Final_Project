from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("ReadTransactions") \
    .master("local[*]") \
    .getOrCreate()

df = spark.read.json("data_raw/transactions.json")

print("Total rows:", df.count())
df.printSchema()

spark.stop()
