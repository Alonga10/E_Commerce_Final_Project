from pyspark.sql import SparkSession
from pyspark.sql.functions import explode, col, sum as _sum, desc

spark = SparkSession.builder.appName("TopProducts").master("local[*]").getOrCreate()

tx = spark.read.json("data_raw/transactions.json")

# explode items array
items = tx.select(explode("items").alias("it"))

top = (items
       .groupBy(col("it.product_id").alias("product_id"))
       .agg(
           _sum(col("it.quantity")).alias("total_qty"),
           _sum(col("it.subtotal")).alias("total_revenue")
       )
       .orderBy(desc("total_qty"))
       .limit(10))

top.show(truncate=False)

spark.stop()
