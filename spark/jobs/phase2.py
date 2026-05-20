import pandas as pd
from pymongo import MongoClient, DESCENDING

client  = MongoClient("mongodb://localhost:27017/")
db      = client["ecommerce"]

basket_df   = pd.read_csv("spark/data/output/market_basket.csv")
affinity_df = pd.read_csv("spark/data/output/user_affinity.csv")

#task1

user_docs = []
for userID, group in affinity_df.groupby("user_id"):
    categories = group.sort_values("affinity_score", ascending=False)
    user_docs.append({
        "_id": userID,
        "top_categories": [
            {"category": row["category"], "score": int(row["affinity_score"])}
            for _, row in categories.iterrows()
        ]
    })

db["user_profiles"].drop()
db["user_profiles"].insert_many(user_docs)
print(f"Inserted {len(user_docs)} user profiles")

#task2

product_docs = []
for item, group in basket_df.groupby("item1"):
    product_docs.append({
        "_id": item,
        "bought_with": [
            {"product": row["item2"], "frequency": int(row["frequency"])}
            for _, row in group.sort_values("frequency", ascending=False).iterrows()
        ]
    })

db["product_pairs"].drop()
db["product_pairs"].insert_many(product_docs)
print(f"Inserted {len(product_docs)} product pair documents")

client.close()
print("Phase 2 complete!")