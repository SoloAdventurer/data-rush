from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("peek") \
    .master("local[*]") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

df = spark.read.csv(
    "spark/data/ecommerce_logs.csv",  # ← change to your actual filename
    header=True,
    inferSchema=True
)

print("=== COLUMNS ===")
print(df.columns)

print("\n=== SCHEMA ===")
df.printSchema()

print("\n=== FIRST 5 ROWS ===")
df.show(5, truncate=False)

print(f"\n=== TOTAL ROWS ===")
print(df.count())