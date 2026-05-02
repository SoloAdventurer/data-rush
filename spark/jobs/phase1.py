import os
import pandas

os.environ["PYSPARK_PYTHON"] = r"D:\data-rush\venv\Scripts\python.exe"
os.environ["PYSPARK_DRIVER_PYTHON"] = r"D:\data-rush\venv\Scripts\python.exe"
os.environ["HADOOP_HOME"] = r"D:\winutils"

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = SparkSession.builder \
    .appName("Phase1") \
    .master("local[*]") \
    .config("spark.sql.shuffle.partitions", "8") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

DATA_PATH = "spark/data/ecommerce_logs.csv"

df = spark.read \
    .option("multiLine", True) \
    .option("quote", '"') \
    .option("escape", '"') \
    .csv(DATA_PATH, header=True, inferSchema=True)

# Show raw sample to debug category extraction
print("=== RAW SAMPLE ===")
df.select("product_metadata").show(5, truncate=False)

# Extract category from product_metadata JSON string
df = df.withColumn(
    "category",
    F.regexp_extract(F.col("product_metadata"), r'"category":\s*"([^"]+)"', 1)
).withColumn(
    "category",
    F.when(F.col("category") == "", "Unknown").otherwise(F.col("category"))
)

df.cache()

print("=== CATEGORY DISTRIBUTION ===")
df.groupBy("category").count().orderBy(F.desc("count")).show(20, truncate=False)

# ── Task 1.1: Market Basket Analysis ─────────────────────────────────────────
purchases = df.filter(F.col("event_type") == "purchase") \
    .select("session_id", "product_id") \
    .withColumnRenamed("product_id", "item1")

pairs_df = purchases.alias("a").join(
    purchases.withColumnRenamed("item1", "item2").alias("b"),
    on="session_id"
).filter(F.col("item1") < F.col("item2")) \
 .groupBy("item1", "item2") \
 .count() \
 .withColumnRenamed("count", "frequency") \
 .orderBy(F.desc("frequency"))

print("=== Task 1.1: Top 20 Frequently Bought Together ===")
pairs_df.show(20, truncate=False)

os.makedirs("spark/data/output", exist_ok=True)
pairs_df.toPandas().to_csv("spark/data/output/market_basket.csv", index=False)
print("✅ Market basket saved")

# ── Task 1.2: User Affinity Aggregation ──────────────────────────────────────
affinity_df = df.filter(F.col("category") != "Unknown") \
    .withColumn("weight",
        F.when(F.col("event_type") == "purchase", 5)
         .when(F.col("event_type") == "cart", 3)
         .otherwise(1)
    ) \
    .groupBy("user_id", "category") \
    .agg(F.sum("weight").alias("affinity_score")) \
    .orderBy("user_id", F.desc("affinity_score"))

print("=== Task 1.2: Sample User Affinity Scores ===")
affinity_df.show(20, truncate=False)

affinity_df.toPandas().to_csv("spark/data/output/user_affinity.csv", index=False)
print("✅ User affinity saved")

spark.stop()