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
    """DUMB worker asks for work. Server Prime selects 1 random task and returns it."""
    pipeline = [{"$sample": {"size": 1}}]
    samples = list(tasks_col.aggregate(pipeline))
    if not samples:
        return {"has_work": False}
        
    task = samples[0]
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
    """DUMB worker returns products payload. Server Prime saves to MongoDB and DELETES the task from queue."""
    payload = await request.json()
    task_id = payload.get("task_id")
    products = payload.get("products", [])
    now = datetime.datetime.now(datetime.timezone.utc)
    
    # 1. Insert Observations
    for prod in products:
        prod["ingested_at"] = now
        obs_col.insert_one(prod)
        
    # 2. Delete task from queue
    if task_id:
        tasks_col.delete_one({"task_id": task_id})
        
    remaining = tasks_col.count_documents({})
    return {"status": "ok", "ingested": len(products), "remaining_tasks": remaining}

from fastapi.responses import HTMLResponse
from fastapi.staticfiles import StaticFiles

if os.path.exists("assets"):
    app.mount("/assets", StaticFiles(directory="assets"), name="assets")

BASE_CSS = """
    * { box-sizing: border-box; }
    body {
        font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, Helvetica, Arial, sans-serif;
        background: #f8fafc;
        color: #0f172a;
        margin: 0;
        padding: 48px 24px;
        line-height: 1.5;
        -webkit-font-smoothing: antialiased;
    }
    .container {
        max-width: 680px;
        margin: 0 auto;
    }
    .card {
        background: #ffffff;
        border: 1px solid #e2e8f0;
        border-radius: 12px;
        padding: 36px 32px;
        box-shadow: 0 1px 3px rgba(0,0,0,0.02);
    }
    h1 {
        margin: 0 0 28px 0;
        font-size: 24px;
        font-weight: 800;
        color: #0f172a;
        letter-spacing: 0.5px;
        text-transform: uppercase;
    }
    .hero-img {
        width: 100%;
        max-height: 260px;
        object-fit: contain;
        background: #f8fafc;
        border: 1px solid #f1f5f9;
        border-radius: 8px;
        margin-bottom: 28px;
        padding: 12px;
    }
    .facet-group {
        margin-bottom: 24px;
    }
    .facet-group:last-child {
        margin-bottom: 0;
    }
    .facet-label {
        font-size: 13px;
        font-weight: 700;
        color: #64748b;
        margin-bottom: 10px;
        text-transform: uppercase;
        letter-spacing: 0.5px;
    }
    .chip-bag {
        display: flex;
        flex-wrap: wrap;
        gap: 8px;
    }
    .chip {
        display: inline-flex;
        align-items: center;
        background: #f1f5f9;
        color: #0369a1;
        border: 1px solid #e2e8f0;
        padding: 6px 14px;
        border-radius: 6px;
        font-size: 14px;
        font-weight: 600;
        text-decoration: none;
        transition: all 0.12s ease;
    }
    .chip:hover {
        background: #e0f2fe;
        border-color: #bae6fd;
        color: #0284c7;
        transform: translateY(-1px);
    }
    .klass-chip {
        display: inline-flex;
        align-items: center;
        background: #f8fafc;
        color: #0f172a;
        border: 1px solid #e2e8f0;
        padding: 8px 16px;
        border-radius: 6px;
        font-size: 14px;
        font-weight: 600;
        text-decoration: none;
        transition: all 0.12s ease;
    }
    .klass-chip:hover {
        background: #f1f5f9;
        border-color: #cbd5e1;
        color: #0284c7;
        transform: translateY(-1px);
    }
    .top-nav {
        margin-bottom: 20px;
        font-size: 13px;
    }
    .top-nav a {
        color: #64748b;
        text-decoration: none;
        font-weight: 600;
    }
    .top-nav a:hover {
        color: #0284c7;
    }
"""

def derive_facet_bag_from_observations(obs_list: list) -> dict:
    """Runtime derivation: computes a clean FacetBag view directly over a set of observations."""
    bag = {}
    
    # 1. Materials
    mats = sorted(list(set(m for o in obs_list for m in o.get("materials", []))))
    if mats:
        bag["Material"] = mats
        
    # 2. All arbitrary dynamic keys discovered in facet_bag / attributes
    all_keys = set()
    for o in obs_list:
        all_keys.update(o.get("facet_bag", {}).keys())
        all_keys.update(o.get("attributes", {}).keys())
        
    for k in sorted(list(all_keys)):
        if k.lower() in ["material", "materials", "klass_name", "product_name", "raw_product_name", "cat_no", "sku"]:
            continue
        vals = set()
        for o in obs_list:
            v = o.get("facet_bag", {}).get(k) or o.get("attributes", {}).get(k)
            if isinstance(v, list):
                vals.update([str(x).strip() for x in v if x])
            elif v and str(v).lower() not in ["none", "n/a", "null"]:
                vals.add(str(v).strip())
        if vals:
            # Natural sort for sizes / numbers
            if k.lower() in ["size", "volume", "length", "gauge", "balloon_capacity", "drops"]:
                def s_sort(s):
                    m = re.search(r"[-+]?\d*\.\d+|\d+", str(s))
                    return (float(m.group(0)) if m else 9999, str(s))
                bag[k.replace("_", " ").title()] = sorted(list(vals), key=s_sort)
            else:
                bag[k.replace("_", " ").title()] = sorted(list(vals))
                
    return bag

@app.get("/", response_class=HTMLResponse)
def index():
    """Generic root: derives all observed Klasses dynamically from observations."""
    raw_klasses = obs_col.distinct("klass_name")
    clean_klasses = sorted([k for k in raw_klasses if k and len(k.strip()) > 1])
    
    chips = "".join([f'<a href="/klass/{k.lower().replace(" ", "-").replace("/", "-")}" class="klass-chip">{k}</a>' for k in clean_klasses])
    
    html = f"""
    <!DOCTYPE html>
    <html>
    <head>
        <title>Infinity Product</title>
        <meta charset="utf-8">
        <style>{BASE_CSS}</style>
    </head>
    <body>
        <div class="container">
            <div class="card">
                <h1>Infinity Product</h1>
                <div class="facet-label">Observed Klasses ({len(clean_klasses)})</div>
                <div class="chip-bag">
                    {chips or '<span style="color:#94a3b8;">No observations ingested yet.</span>'}
                </div>
            </div>
        </div>
    </body>
    </html>
    """
    return HTMLResponse(content=html)


@app.get("/klass/{slug}", response_class=HTMLResponse)
def view_klass(slug: str):
    """Pure Runtime View: Queries raw observations for this Klass and derives the FacetBag instantly."""
    clean_name = slug.replace("-", " ")
    obs_list = list(obs_col.find({"klass_name": {"$regex": f"^{clean_name}$", "$options": "i"}}))
    
    if not obs_list:
        return HTMLResponse("<h2>Klass not found in current observations</h2>", status_code=404)
        
    k_name = obs_list[0].get("klass_name", clean_name.title())
    facet_bag = derive_facet_bag_from_observations(obs_list)
    
    # Hero image from first observation if available
    img_html = ""
    spread_num = obs_list[0].get("spread_num")
    side = obs_list[0].get("side", "left")
    if spread_num:
        img_path = f"/assets/catalog_pages/spread_{spread_num:02d}_{side}.jpg"
        if os.path.exists(f"assets/catalog_pages/spread_{spread_num:02d}_{side}.jpg"):
            img_html = f'<img src="{img_path}" class="hero-img" alt="{k_name}" />'

    # Render labeled chip rows
    facet_sections = ""
    for facet_name, values in facet_bag.items():
        if not values:
            continue
        pills = "".join([f'<a href="/facet/{facet_name}/{str(v).replace("/", "-").replace(" ", "-")}" class="chip">{v}</a>' for v in values])
        facet_sections += f"""
        <div class="facet-group">
            <div class="facet-label">{facet_name}</div>
            <div class="chip-bag">
                {pills}
            </div>
        </div>
        """

    html = f"""
    <!DOCTYPE html>
    <html>
    <head>
        <title>{k_name} — Infinity Product</title>
        <meta charset="utf-8">
        <style>{BASE_CSS}</style>
    </head>
    <body>
        <div class="container">
            <div class="top-nav">
                <a href="/">← All Klasses</a>
            </div>
            
            <div class="card">
                <h1>{k_name}</h1>
                {img_html}
                {facet_sections}
            </div>
        </div>
    </body>
    </html>
    """
    return HTMLResponse(content=html)


@app.get("/facet/{facet_name}/{facet_val}", response_class=HTMLResponse)
def view_inverted_facet(facet_name: str, facet_val: str):
    """Pure Runtime Inversion: Searches raw observations for the facet value and groups by Klass."""
    clean_val = facet_val.replace("-", " ")
    
    # Dynamic MongoDB query matching facet value across materials, facet_bag, or attributes
    if facet_name.lower() in ["material", "materials"]:
        query = {"materials": {"$regex": f"^{clean_val}$", "$options": "i"}}
    else:
        query = {
            "$or": [
                {f"facet_bag.{facet_name.lower()}": {"$regex": f"^{clean_val}$", "$options": "i"}},
                {f"attributes.{facet_name.lower()}": {"$regex": f"^{clean_val}$", "$options": "i"}},
                {f"facet_bag.{facet_name}": clean_val},
                {f"attributes.{facet_name}": clean_val}
            ]
        }
        
    matching_klasses = sorted(list(set(obs_col.distinct("klass_name", query))))
    chips = "".join([f'<a href="/klass/{k.lower().replace(" ", "-").replace("/", "-")}" class="klass-chip">{k}</a>' for k in matching_klasses if k])

    html = f"""
    <!DOCTYPE html>
    <html>
    <head>
        <title>{clean_val.upper()} — Infinity Product</title>
        <meta charset="utf-8">
        <style>{BASE_CSS}</style>
    </head>
    <body>
        <div class="container">
            <div class="top-nav">
                <a href="/">← All Klasses</a>
            </div>
            
            <div class="card">
                <h1>{clean_val.upper()}</h1>
                
                <div class="facet-group">
                    <div class="facet-label">Observed on:</div>
                    <div class="chip-bag">
                        {chips or '<span style="color:#94a3b8;">No connected Klasses found</span>'}
                    </div>
                </div>
            </div>
        </div>
    </body>
    </html>
    """
    return HTMLResponse(content=html)


@app.get("/material/{mat_name}", response_class=HTMLResponse)
def redirect_material(mat_name: str):
    return view_inverted_facet("Material", mat_name)

if __name__ == "__main__":
    port = int(os.getenv("PORT", 8371))
    print(f"\n==================================================")
    print(f"  ⚡ SERVER PRIME - Infinity Product Swarm Master")
    print(f"  Listening on: http://0.0.0.0:{port} (Auto-Reload Enabled)")
    print(f"==================================================\n")
    uvicorn.run("server_prime:app", host="0.0.0.0", port=port, reload=True)
