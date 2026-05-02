"""
Query Demo Script — E-Commerce Behavioral Analytics
Phase 2 & 3 deliverable: fetch recommendations by user_id and item_id
Usage: python query_demo.py
"""

from pymongo import MongoClient

MONGO_URI = "mongodb://localhost:27017"

client = MongoClient(MONGO_URI)
db = client["ecommerce"]


def get_user_profile(user_id: str):
    """Fetch a user's category affinities from MongoDB."""
    profile = db.user_profiles.find_one({"user_id": user_id}, {"_id": 0})
    if not profile:
        print(f"User '{user_id}' not found.")
        return
    print(f"\n=== User Profile: {user_id} ===")
    print(f"Primary Category : {profile['primary_category']}")
    print(f"Top Categories   :")
    for i, cat in enumerate(profile["top_categories"], 1):
        print(f"  {i}. {cat['category']} (score: {cat['affinity_score']})")


def get_frequently_bought_with(item_id: str, top_n: int = 5):
    """Fetch items frequently bought together with a given item."""
    results = list(db.market_basket.find(
        {"$or": [{"item1": item_id}, {"item2": item_id}]},
        {"_id": 0}
    ).sort("frequency", -1).limit(top_n))

    if not results:
        print(f"\nNo co-purchase data found for item '{item_id}'.")
        return

    print(f"\n=== Items Frequently Bought With: {item_id} ===")
    for r in results:
        partner = r["item2"] if r["item1"] == item_id else r["item1"]
        print(f"  {partner}  (bought together {r['frequency']} times)")


def get_recommendation(user_id: str, item_id: str):
    """
    Given a user and an abandoned item, determine the discount action.
    Mirrors Phase 3 logic: High_Discount if item category matches user's
    primary category, else Standard_Reminder.
    """
    profile = db.user_profiles.find_one({"user_id": user_id}, {"_id": 0})
    if not profile:
        print(f"User '{user_id}' not found.")
        return

    # We don't store item→category mapping in Mongo, so note this is a demo
    # In production you'd also store a product catalog collection
    primary = profile["primary_category"]
    print(f"\n=== Recommendation for {user_id} abandoning {item_id} ===")
    print(f"User's primary category : {primary}")
    print(f"Action (if item matches): High_Discount")
    print(f"Action (if item differs): Standard_Reminder")
    print(f"(Compare item category to '{primary}' to determine final action)")


if __name__ == "__main__":
    # ── Demo queries ──────────────────────────────────────────────────────────
    print("=" * 60)
    print("E-Commerce Analytics Query Demo")
    print("=" * 60)

    # 1. User profile lookup
    get_user_profile("User_0")

    # 2. Market basket — items bought with a given item
    get_frequently_bought_with("ITEM_309")

    # 3. Recommendation action for a user + abandoned item
    get_recommendation("User_0", "ITEM_309")

    # ── Interactive mode ──────────────────────────────────────────────────────
    print("\n" + "=" * 60)
    print("Interactive Mode (press Ctrl+C to exit)")
    print("=" * 60)
    while True:
        try:
            print("\nOptions: [1] User Profile  [2] Bought Together  [3] Recommendation")
            choice = input("Choose (1/2/3): ").strip()
            if choice == "1":
                uid = input("Enter user_id (e.g. User_42): ").strip()
                get_user_profile(uid)
            elif choice == "2":
                iid = input("Enter item_id (e.g. ITEM_309): ").strip()
                get_frequently_bought_with(iid)
            elif choice == "3":
                uid = input("Enter user_id: ").strip()
                iid = input("Enter item_id: ").strip()
                get_recommendation(uid, iid)
        except KeyboardInterrupt:
            print("\nExiting.")
            break

    client.close()