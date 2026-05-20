import os
import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, ArrayType

os.environ["PYSPARK_PYTHON"]        = r"D:\data-rush\venv\Scripts\python.exe"
os.environ["PYSPARK_DRIVER_PYTHON"] = r"D:\data-rush\venv\Scripts\python.exe"
os.environ["HADOOP_HOME"]           = r"D:\winutils"

os.environ["PYSPARK_SUBMIT_ARGS"] = (
    "--packages org.mongodb.spark:mongo-spark-connector_2.13:11.0.0 pyspark-shell"
)

MONGO_URI = "mongodb://localhost:27017"

spark = SparkSession.builder \
    .appName("Phase3_CartAbandonmentRecovery") \
    .master("local[*]") \
    .config("spark.sql.shuffle.partitions", "8") \
    .config("spark.mongodb.read.connection.uri", MONGO_URI) \
    .config("spark.mongodb.write.connection.uri", MONGO_URI) \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

print("=" * 60)
print("PHASE 3: Cart Abandonment Recovery")
print("=" * 60)

# ── Step 1: Detect abandoned carts ───────────────────────────────────────────
print("\n--- Step 1: Detecting abandoned carts ---")

df = spark.read \
    .option("multiLine", True).option("quote", '"').option("escape", '"') \
    .csv("spark/data/ecommerce_logs.csv", header=True, inferSchema=True) \
    .withColumn("category",
        F.regexp_extract("product_metadata", r'"category":\s*"([^"]+)"', 1)) \
    .withColumn("category",
        F.when(F.col("category") == "", "Unknown").otherwise(F.col("category")))

carts = df.filter(F.col("event_type") == "cart").select(
    F.col("user_id").cast("string"), "session_id", "product_id",
    F.col("category").alias("item_category"))

purchases = df.filter(F.col("event_type") == "purchase").select(
    "session_id", F.col("product_id").alias("purchased_item"))

abandoned = carts.join(
    purchases,
    (carts.session_id == purchases.session_id) &
    (carts.product_id == purchases.purchased_item),
    "left_anti"
).select("user_id", "product_id", "item_category")

abandoned_count = abandoned.count()
print(f"✅ Found {abandoned_count} abandoned cart items")

# ── Step 2: Load user profiles directly from MongoDB via Spark connector ──────
print("\n--- Step 2: Loading user profiles from MongoDB via Spark connector ---")

raw_profiles = spark.read \
    .format("mongodb") \
    .option("database", "ecommerce") \
    .option("collection", "user_profiles") \
    .load()

# top_categories is stored as [{category: str, score: int}, ...]
profiles_df = raw_profiles.select(
    F.col("_id").cast("string").alias("user_id"),
    F.transform(
        F.col("top_categories"),
        lambda x: x.getField("category")
    ).alias("top_cats")
)

print(f"✅ Loaded {profiles_df.count()} user profiles")

# ── Step 3: Join and assign actions ──────────────────────────────────────────
print("\n--- Step 3: Joining and assigning actions ---")

result = abandoned.join(profiles_df, on="user_id", how="left") \
    .withColumn("action", F.when(
        F.array_contains(F.col("top_cats"), F.col("item_category")), "High_Discount"
    ).otherwise("Standard_Reminder")) \
    .select("user_id", "product_id", "item_category", "action")

print("\nSample results:")
result.show(20, truncate=False)

# ── Step 4: Save results ─────────────────────────────────────────────────────
result_pd = result.toPandas()
spark.stop()

print("\n" + "=" * 60)
print("📊 SUMMARY")
print("=" * 60)
print(f"Total abandoned carts: {len(result_pd)}")
high = (result_pd["action"] == "High_Discount").sum()
std  = (result_pd["action"] == "Standard_Reminder").sum()
print(f"High_Discount:      {high}")
print(f"Standard_Reminder:  {std}")

os.makedirs("spark/data/output", exist_ok=True)
result_pd.to_csv("spark/data/output/cart_abandonment.csv", index=False)
print("\n✅ Saved to spark/data/output/cart_abandonment.csv")
print("=" * 60)