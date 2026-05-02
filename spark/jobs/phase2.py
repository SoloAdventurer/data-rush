import os
import pandas as pd
from pymongo import MongoClient

client = MongoClient("mongodb://localhost:27017")
db = client["ecommerce"]

# ── Load market basket ────────────────────────────────────────────────────────
basket_df = pd.read_csv("spark/data/output/market_basket.csv")
basket_records = basket_df.to_dict("records")
db.market_basket.drop()
db.market_basket.insert_many(basket_records)
print(f"✅ Inserted {len(basket_records)} market basket pairs")

# ── Load user affinity as denormalized user profiles ─────────────────────────
affinity_df = pd.read_csv("spark/data/output/user_affinity.csv")

# Group by user_id → one document per user with ranked categories
user_profiles = []
for user_id, group in affinity_df.groupby("user_id"):
    top_categories = group.sort_values("affinity_score", ascending=False)[["category","affinity_score"]].to_dict("records")
    user_profiles.append({
        "user_id": user_id,
        "top_categories": top_categories,
        "primary_category": top_categories[0]["category"]
    })

db.user_profiles.drop()
db.user_profiles.insert_many(user_profiles)
print(f"✅ Inserted {len(user_profiles)} user profiles")

# ── Create indexes for fast lookups ──────────────────────────────────────────
db.user_profiles.create_index("user_id", unique=True)
db.market_basket.create_index([("item1", 1), ("item2", 1)])
print("✅ Indexes created")

client.close()