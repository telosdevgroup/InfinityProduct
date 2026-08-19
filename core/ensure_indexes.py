from pymongo import MongoClient

def ensure_indexes():
    client = MongoClient("mongodb://localhost:27017/", serverSelectionTimeoutMS=3000)
    db = client["infinityproduct_dev"]

    print("[*] Creating database indexes in MongoDB...")
    db["source_products"].create_index("source_url", unique=True, background=True)
    db["source_products"].create_index("inferred_klass", background=True)
    db["source_products"].create_index("status", background=True)
    db["source_products"].create_index("raw_hash", background=True)

    db["discovered_urls"].create_index("source_id", background=True)
    db["discovered_urls"].create_index("status", background=True)

    db["klass_metadata"].create_index("klass", background=True)
    db["klass_metadata"].create_index("status", background=True)

    print("[OK] All database indexes created successfully.")

if __name__ == "__main__":
    ensure_indexes()
