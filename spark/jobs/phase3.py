import os
import pandas as pd
from pymongo import MongoClient
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

os.environ["PYSPARK_PYTHON"] = r"D:\data-rush\venv\Scripts\python.exe"
os.environ["PYSPARK_DRIVER_PYTHON"] = r"D:\data-rush\venv\Scripts\python.exe"
os.environ["HADOOP_HOME"] = r"D:\winutils"

spark = SparkSession.builder \
    .appName("Phase3") \
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

df = df.withColumn(
    "category",
    F.regexp_extract(F.col("product_metadata"), r'"category":\s*"([^"]+)"', 1)
).withColumn(
    "category",
    F.when(F.col("category") == "", "Unknown").otherwise(F.col("category"))
)

# ── Find cart abandonments in Spark (pure JVM, no Python workers) ─────────────
carts = df.filter(F.col("event_type") == "cart") \
    .select("user_id", "session_id", "product_id", "category") \
    .withColumnRenamed("category", "item_category")

purchases = df.filter(F.col("event_type") == "purchase") \
    .select("session_id", "product_id") \
    .withColumnRenamed("product_id", "purchased_item")

abandoned = carts.join(
    purchases,
    (carts.session_id == purchases.session_id) &
    (carts.product_id == purchases.purchased_item),
    "left_anti"
)

print("=== Abandoned cart events ===")
abandoned.show(10, truncate=False)

# ── Collect to pandas, join with MongoDB profiles in pandas ──────────────────
print("Collecting abandoned carts to pandas...")
abandoned_pd = abandoned.toPandas()
spark.stop()

print(f"Total abandoned carts: {len(abandoned_pd)}")

# Fetch user profiles from MongoDB
client = MongoClient("mongodb://localhost:27017")
db = client["ecommerce"]
profiles = list(db.user_profiles.find({}, {"_id": 0, "user_id": 1, "primary_category": 1}))
client.close()

profiles_pd = pd.DataFrame(profiles)

# Join in pandas
result = abandoned_pd.merge(profiles_pd, on="user_id", how="left")

# Assign action
result["action"] = result.apply(
    lambda row: "High_Discount" if row["item_category"] == row["primary_category"]
    else "Standard_Reminder",
    axis=1
)

print("=== Phase 3: Cart Abandonment Results ===")
print(result[["user_id", "product_id", "item_category", "primary_category", "action"]].head(20).to_string())

os.makedirs("spark/data/output", exist_ok=True)
result[["user_id", "product_id", "item_category", "primary_category", "action"]] \
    .to_csv("spark/data/output/cart_abandonment.csv", index=False)
print("✅ Cart abandonment saved to spark/data/output/cart_abandonment.csv")