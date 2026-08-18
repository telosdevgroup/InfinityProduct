import os
import sys
import json
import time
import base64
import socket
import datetime
import uvicorn
from fastapi import FastAPI, Request
from pymongo import MongoClient
import pypdfium2 as pdfium
from PIL import Image

from contextlib import asynccontextmanager

MONGO_URI = os.getenv("MONGO_URI", "mongodb://localhost:27017/")
DB_NAME = "infinityproduct_dev"
TASKS_COLL = "swarm_tasks"
OBSERVATIONS_COLL = "observations"
KLASSES_COLL = "klasses"
CATALOGS_COLL = "catalogs"

client = MongoClient(MONGO_URI)
db = client[DB_NAME]
tasks_col = db[TASKS_COLL]
obs_col = db[OBSERVATIONS_COLL]
klass_col = db[KLASSES_COLL]
catalogs_col = db[CATALOGS_COLL]

@asynccontextmanager
async def lifespan(app: FastAPI):
    # Ensure indexes and clean any stale documents without slugs
    klass_col.delete_many({"slug": None})
    tasks_col.create_index([("status", 1), ("created_at", 1)])
    tasks_col.create_index("task_id", unique=True)
    obs_col.create_index("cat_no")
    klass_col.create_index("slug", unique=True)
    print("🚀 [Server Prime] Online and connected to MongoDB!")
    yield

app = FastAPI(title="Infinity Product - Server Prime", lifespan=lifespan)

@app.get("/api/status")
def get_status():
    remaining = tasks_col.count_documents({})
    total_obs = obs_col.count_documents({})
    total_klasses = klass_col.count_documents({})
    
    return {
        "status": "online",
        "remaining_tasks": remaining,
        "total_observations": total_obs,
        "total_klasses": total_klasses
    }

@app.post("/api/reset-queue")
def reset_queue(purge_data: bool = True):
    """Purge all remaining tasks and observations."""
    tasks_col.delete_many({})
    if purge_data:
        obs_col.delete_many({})
        klass_col.delete_many({})
    print("🧹 [Server Prime] Queue and data wiped clean!")
@app.post("/api/enqueue-catalog")
def enqueue_catalog(pdf_path: str = "foyomed catalogue.pdf", start_spread: int = 4, end_spread: int = 48):
    """Seed atomic page tasks into swarm_tasks queue from a catalog PDF."""
    if not os.path.exists(pdf_path):
        return {"error": f"File {pdf_path} not found"}
        
    doc = pdfium.PdfDocument(pdf_path)
    total_spreads = len(doc)
    os.makedirs("assets/catalog_pages", exist_ok=True)
    
    enqueued_count = 0
    now = datetime.datetime.now(datetime.timezone.utc)
    
    for sheet_idx in range(start_spread - 1, min(end_spread, total_spreads)):
        spread_num = sheet_idx + 1
        page = doc[sheet_idx]
        pil_image = page.render(scale=2).to_pil()
        w, h = pil_image.size
        
        left_img = pil_image.crop((0, 0, w // 2, h))
        right_img = pil_image.crop((w // 2, 0, int(w * 0.93), h))
        
        for side_name, side_img in [("left", left_img), ("right", right_img)]:
            task_id = f"spread_{spread_num:02d}_{side_name}"
            img_rel_path = f"assets/catalog_pages/{task_id}.jpg"
            side_img.save(img_rel_path)
            
            task_doc = {
                "task_id": task_id,
                "catalog_file": os.path.basename(pdf_path),
                "spread_num": spread_num,
                "side": side_name,
                "image_path": img_rel_path,
                "action": "ocr_and_extract",
                "created_at": now
            }
            
            tasks_col.update_one(
                {"task_id": task_id},
                {"$setOnInsert": task_doc},
                upsert=True
            )
            enqueued_count += 1
            
    return {"enqueued_tasks": enqueued_count, "start_spread": start_spread, "end_spread": end_spread}

@app.get("/api/get-work")
def get_work(worker: str = "anonymous_worker"):
    """DUMB worker asks for work. Server Prime pops/deletes 1 random task and returns it."""
    # Pop a random task from the queue
    pipeline = [{"$sample": {"size": 1}}]
    samples = list(tasks_col.aggregate(pipeline))
    if not samples:
        return {"has_work": False}
        
    task = samples[0]
    # Delete immediately
    tasks_col.delete_one({"_id": task["_id"]})
    
    img_b64 = None
    if os.path.exists(task["image_path"]):
        with open(task["image_path"], "rb") as f:
            img_b64 = base64.b64encode(f.read()).decode("utf-8")
            
    return {
        "has_work": True,
        "task_id": task["task_id"],
        "action": task["action"],
        "data": {
            "image_b64": img_b64,
            "spread_num": task["spread_num"],
            "side": task["side"],
            "catalog_file": task["catalog_file"]
        }
    }

@app.post("/api/submit-work")
async def submit_work(request: Request):
    """DUMB worker returns products payload. Server Prime saves to MongoDB."""
    payload = await request.json()
    task_id = payload.get("task_id")
    products = payload.get("products", [])
    now = datetime.datetime.now(datetime.timezone.utc)
    
    # 1. Insert Observations
    for prod in products:
        prod["ingested_at"] = now
        obs_col.insert_one(prod)
        
        # 2. Update Klass living ontology
        k_name = prod.get("klass_name", "General")
        slug = k_name.lower().replace(" ", "-").replace("/", "-")
        all_k_obs = list(obs_col.find({"klass_name": k_name}))
        
        all_mats = sorted(list(set(m for o in all_k_obs for m in o.get("materials", []))))
        all_sizes = sorted(list(set(o.get("attributes", {}).get("size") for o in all_k_obs if o.get("attributes", {}).get("size"))))
        
        klass_col.update_one(
            {"slug": slug},
            {
                "$set": {
                    "name": k_name,
                    "slug": slug,
                    "total_observations": len(all_k_obs),
                    "observed_facet_space": {
                        "materials": all_mats,
                        "sizes": all_sizes
                    },
                    "updated_at": now
                },
                "$setOnInsert": {"created_at": now}
            },
            upsert=True
        )
        
    print(f"  ✅ [Task {task_id}] Received! Saved {len(products)} observations.")
    return {"status": "ok", "ingested": len(products)}

if __name__ == "__main__":
    port = int(os.getenv("PORT", 8371))
    print(f"\n==================================================")
    print(f"  ⚡ SERVER PRIME - Infinity Product Swarm Master")
    print(f"  Listening on: http://0.0.0.0:{port}")
    print(f"==================================================\n")
    uvicorn.run(app, host="0.0.0.0", port=port)
