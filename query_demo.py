"""
Query Demo Script — E-Commerce Behavioral Analytics
Phase 2 & 3 deliverable: fetch recommendations by user_id and item_id
Interactive mode: choose 1, 2, or 3.
Usage: python query_demo.py
"""

from pymongo import MongoClient

MONGO_URI = "mongodb://localhost:27017"

client = MongoClient(MONGO_URI)
db = client["ecommerce"]


def get_user_profile(user_id: str):
    """Fetch a user's category affinities from MongoDB."""
    doc = db.user_profiles.find_one({"_id": user_id})
    if not doc:
        print(f"\nUser '{user_id}' not found.")
        return
    top_cats = doc["top_categories"]
    primary = top_cats[0]["category"]
    print(f"\n=== User Profile: {user_id} ===")
    print(f"Primary Category : {primary}")
    print(f"Top Categories   :")
    for i, cat in enumerate(top_cats[:3], 1):
        print(f"  {i}. {cat['category']} (score: {cat['score']})")


def get_frequently_bought_with(item_id: str, top_n: int = 5):
    """Fetch items frequently bought together with a given item from product_pairs."""
    doc = db.product_pairs.find_one({"_id": item_id})
    if not doc:
        print(f"\nNo co-purchase data found for item '{item_id}'.")
        return
    print(f"\n=== Items Frequently Bought With: {item_id} ===")
    for i, item in enumerate(doc["bought_with"][:top_n], 1):
        print(f"  {i}. {item['product']}  (bought together {item['frequency']} times)")


def get_recommendation(user_id: str, abandoned_category: str):
    """
    Given a user and an abandoned item's category, determine the discount action.
    High_Discount if category matches user's primary category, else Standard_Reminder.
    """
    doc = db.user_profiles.find_one({"_id": user_id})
    if not doc:
        print(f"\nUser '{user_id}' not found.")
        return
    primary = doc["top_categories"][0]["category"]
    action = "High_Discount" if abandoned_category == primary else "Standard_Reminder"
    print(f"\n=== Recommendation for {user_id} ===")
    print(f"Abandoned category : {abandoned_category}")
    print(f"User's primary category : {primary}")
    print(f"Action → {action}")


if __name__ == "__main__":
    print("=" * 60)
    print("E-Commerce Analytics Query Demo (Interactive)")
    print("=" * 60)
    print("\nOptions:\n  [1] User Profile\n  [2] Frequently Bought Together\n  [3] Recommendation (by category)\n")
    
    while True:
        try:
            choice = input("Choose (1/2/3) or 'q' to quit: ").strip()
            if choice.lower() == 'q':
                break
            if choice == "1":
                uid = input("Enter user_id (e.g. User_0): ").strip()
                get_user_profile(uid)
            elif choice == "2":
                iid = input("Enter item_id (e.g. ITEM_100): ").strip()
                get_frequently_bought_with(iid)
            elif choice == "3":
                uid = input("Enter user_id: ").strip()
                cat = input("Enter abandoned category (e.g. Electronics): ").strip()
                get_recommendation(uid, cat)
            else:
                print("Invalid choice. Enter 1, 2, 3, or q.")
        except KeyboardInterrupt:
            print("\nExiting.")
            break

    client.close()