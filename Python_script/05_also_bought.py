from pyspark.sql import SparkSession
from pyspark.sql.functions import explode, col, array_sort, array_distinct, size, expr, count as fcount
from pyspark.sql.functions import greatest, least

spark = SparkSession.builder.appName("AlsoBought").master("local[*]").getOrCreate()
spark.sparkContext.setLogLevel("WARN")

# Load transactions
tx = spark.read.json("data_raw/transactions.json")

# 1) explode items to product list per transaction
tx_products = (
    tx.select("transaction_id", explode("items").alias("it"))
      .select("transaction_id", col("it.product_id").alias("product_id"))
      .dropna()
)

# 2) collect products per transaction (unique)
basket = (
    tx_products.groupBy("transaction_id")
    .agg(array_sort(array_distinct(expr("collect_list(product_id)"))).alias("products"))
    .filter(size(col("products")) >= 2)
)

# 3) generate product pairs per basket (X,Y)
pairs = (
    basket.selectExpr("transaction_id", "explode(transform(sequence(0, size(products)-2), i -> transform(sequence(i+1, size(products)-1), j -> array(products[i], products[j])))) as pair_nested")
          .selectExpr("transaction_id", "explode(pair_nested) as pair")
          .select(col("pair")[0].alias("p1"), col("pair")[1].alias("p2"))
)

# 4) count co-occurrence
also_bought = (
    pairs.groupBy("p1", "p2")
         .agg(fcount("*").alias("co_purchase_count"))
         .orderBy(col("co_purchase_count").desc())
)

print("\n=== USERS WHO BOUGHT X ALSO BOUGHT Y (Top Pairs) ===")
also_bought.show(20, truncate=False)

# Save
out_path = "outputs/also_bought_pairs_csv"
also_bought.coalesce(1).write.mode("overwrite").option("header", True).csv(out_path)
print(f"✅ Results written to: {out_path}")

spark.stop()
