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
        
from fastapi.responses import HTMLResponse

@app.get("/klass/{slug}", response_class=HTMLResponse)
def view_klass_diagnostic(slug: str):
    """Diagnostic Explorer for a Klass: clean business light theme with evidence lineage."""
    klass = klass_col.find_one({"slug": slug})
    if not klass:
        klass = klass_col.find_one({"name": {"$regex": f"^{slug.replace('-', ' ')}$", "$options": "i"}})
        
    k_name = klass["name"] if klass else slug.replace("-", " ").title()
    observations = list(obs_col.find({"klass_name": {"$regex": f"^{k_name}$", "$options": "i"}}))
    
    observed_mats = sorted(list(set(m for o in observations for m in o.get("materials", []))))
    observed_sizes = sorted(list(set(o.get("attributes", {}).get("size") for o in observations if o.get("attributes", {}).get("size"))))
    
    obs_cards = ""
    for o in observations:
        mats_badge = " ".join([f'<a href="/material/{m.lower()}" style="background:#e0f2fe;color:#0369a1;border:1px solid #bae6fd;padding:2px 8px;border-radius:4px;text-decoration:none;font-size:12px;font-weight:600;margin-right:4px;">{m}</a>' for m in o.get('materials', [])]) or '<span style="color:#94a3b8;">None specified</span>'
        attrs = o.get("attributes", {})
        attr_rows = "".join([f"<tr><td style='color:#64748b;padding:4px 8px;border-bottom:1px solid #f1f5f9;width:35%;'>{k.replace('_', ' ').title()}</td><td style='color:#1e293b;padding:4px 8px;border-bottom:1px solid #f1f5f9;'><b>{v}</b></td></tr>" for k, v in attrs.items()])
        
        obs_cards += f"""
        <div style="background:#ffffff;border:1px solid #e2e8f0;border-radius:6px;padding:14px;margin-bottom:12px;box-shadow:0 1px 2px rgba(0,0,0,0.03);">
            <div style="display:flex;justify-content:space-between;align-items:center;border-bottom:1px solid #f1f5f9;padding-bottom:8px;">
                <h4 style="margin:0;color:#0f172a;font-size:14px;font-weight:700;">{o.get('product_name')}</h4>
                <span style="font-size:12px;color:#475569;">SKU: <code style="background:#f8fafc;border:1px solid #e2e8f0;padding:2px 6px;border-radius:4px;color:#0f766e;font-weight:600;">{o.get('cat_no') or 'N/A'}</code></span>
            </div>
            <div style="margin:10px 0;font-size:13px;color:#334155;">
                <b style="color:#64748b;">Materials:</b> {mats_badge}
            </div>
            <table style="width:100%;font-size:12px;background:#f8fafc;border:1px solid #e2e8f0;border-radius:4px;border-collapse:collapse;margin-top:6px;">
                {attr_rows or "<tr><td style='padding:6px;color:#94a3b8;'>No extra attributes</td></tr>"}
            </table>
            <div style="margin-top:10px;font-size:11px;color:#64748b;display:flex;justify-content:space-between;">
                <span>Source: <b style="color:#475569;">{o.get('source_catalog', 'foyomed catalogue.pdf')}</b> (Spread {o.get('spread_num', '?')})</span>
                <span>Ingested: {str(o.get('ingested_at', ''))[:19]}</span>
            </div>
        </div>
        """

    html = f"""
    <!DOCTYPE html>
    <html>
    <head>
        <title>Klass Diagnostic: {k_name}</title>
        <meta charset="utf-8">
        <style>
            body {{ font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, sans-serif; background: #f8fafc; color: #1e293b; margin: 0; padding: 28px; line-height: 1.5; }}
            .container {{ max-width: 960px; margin: 0 auto; }}
            .header {{ background: #ffffff; border: 1px solid #e2e8f0; border-radius: 8px; padding: 24px; margin-bottom: 20px; box-shadow: 0 1px 3px rgba(0,0,0,0.04); }}
            .badge-pill {{ background: #f1f5f9; color: #334155; border: 1px solid #cbd5e1; padding: 3px 8px; border-radius: 4px; font-size: 12px; font-weight: 600; margin-right: 4px; display: inline-block; }}
            a {{ color: #0284c7; text-decoration: none; }}
            a:hover {{ text-decoration: underline; }}
        </style>
    </head>
    <body>
        <div class="container">
            <div style="margin-bottom:14px;">
                <a href="/material/silicone" style="font-size:13px;font-weight:600;">← View Material Diagnostic: Silicone</a>
            </div>
            <div class="header">
                <div style="display:flex;justify-content:space-between;align-items:flex-start;">
                    <div>
                        <div style="font-size:12px;font-weight:700;color:#0284c7;text-transform:uppercase;letter-spacing:0.5px;">Klass Diagnostic View</div>
                        <h1 style="margin:4px 0 0 0;font-size:22px;color:#0f172a;">{k_name}</h1>
                    </div>
                    <span style="background:#0284c7;color:#ffffff;padding:5px 12px;border-radius:4px;font-size:13px;font-weight:700;">
                        {len(observations)} Empirical Observations
                    </span>
                </div>
                <div style="margin-top:20px;display:grid;grid-template-columns:1fr 1fr;gap:14px;">
                    <div style="background:#f8fafc;border:1px solid #e2e8f0;padding:14px;border-radius:6px;">
                        <div style="font-size:11px;color:#64748b;text-transform:uppercase;font-weight:700;letter-spacing:0.5px;">Observed Materials</div>
                        <div style="margin-top:8px;">
                            {' '.join([f'<a href="/material/{m.lower()}" class="badge-pill" style="background:#e0f2fe;color:#0369a1;border-color:#bae6fd;">{m}</a>' for m in observed_mats]) or '<span style="color:#94a3b8;font-size:13px;">None</span>'}
                        </div>
                    </div>
                    <div style="background:#f8fafc;border:1px solid #e2e8f0;padding:14px;border-radius:6px;">
                        <div style="font-size:11px;color:#64748b;text-transform:uppercase;font-weight:700;letter-spacing:0.5px;">Observed Dimensions ({len(observed_sizes)})</div>
                        <div style="margin-top:8px;max-height:85px;overflow-y:auto;">
                            {' '.join([f'<span class="badge-pill">{s}</span>' for s in observed_sizes]) or '<span style="color:#94a3b8;font-size:13px;">None</span>'}
                        </div>
                    </div>
                </div>
            </div>

            <div style="display:flex;justify-content:space-between;align-items:center;margin-bottom:12px;">
                <h3 style="color:#334155;margin:0;font-size:16px;">Evidence Lineage ({len(observations)} SKU Observations)</h3>
            </div>
            {obs_cards or '<p style="color:#64748b;">No observations found for this Klass.</p>'}
        </div>
    </body>
    </html>
    """
    return HTMLResponse(content=html)


@app.get("/material/{mat_name}", response_class=HTMLResponse)
def view_material_diagnostic(mat_name: str):
    """Inverted Diagnostic Explorer: clean business light theme connecting Klasses to materials."""
    target_mat = mat_name.strip()
    observations = list(obs_col.find({"materials": {"$regex": f"^{target_mat}$", "$options": "i"}}))
    
    klass_groups = {}
    for o in observations:
        k = o.get("klass_name", "Uncategorized")
        klass_groups.setdefault(k, []).append(o)
        
    sorted_klasses = sorted(klass_groups.items(), key=lambda x: len(x[1]), reverse=True)
    
    sections = ""
    for k_name, obs_list in sorted_klasses:
        k_slug = k_name.lower().replace(" ", "-").replace("/", "-")
        
        is_single = len(obs_list) == 1
        badge_style = "background:#fef2f2;color:#991b1b;border:1px solid #fecaca;" if is_single else "background:#f0fdf4;color:#166534;border:1px solid #bbf7d0;"
        badge_text = "⚠️ 1 Observation (Inspect for Bleed)" if is_single else f"✓ {len(obs_list)} observations"
        
        sample_obs_html = ""
        for o in obs_list[:4]:
            attrs = o.get("attributes", {})
            attr_str = " | ".join([f"{k.replace('_', ' ').title()}: {v}" for k, v in attrs.items()]) if attrs else "No extra attributes"
            sample_obs_html += f"""
            <div style="background:#ffffff;border:1px solid #f1f5f9;padding:8px 12px;border-radius:4px;margin-top:6px;font-size:12px;">
                <div style="display:flex;justify-content:space-between;">
                    <span style="color:#0f172a;font-weight:600;">{o.get('product_name')}</span>
                    <span style="color:#0f766e;font-weight:600;">SKU: {o.get('cat_no') or 'N/A'}</span>
                </div>
                <div style="color:#64748b;margin-top:2px;">{attr_str}</div>
                <div style="color:#94a3b8;font-size:10px;margin-top:4px;">Spread: {o.get('spread_num', '?')} | Source: {o.get('source_catalog', 'foyomed catalogue.pdf')}</div>
            </div>
            """
            
        sections += f"""
        <div style="background:#ffffff;border:1px solid #e2e8f0;border-radius:8px;padding:16px;margin-bottom:14px;box-shadow:0 1px 2px rgba(0,0,0,0.03);">
            <div style="display:flex;justify-content:space-between;align-items:center;">
                <h3 style="margin:0;font-size:16px;">
                    <a href="/klass/{k_slug}" style="color:#0f172a;font-weight:700;">🏷️ {k_name}</a>
                </h3>
                <span style="{badge_style}padding:3px 10px;border-radius:4px;font-size:12px;font-weight:600;">{badge_text}</span>
            </div>
            <div style="margin-top:8px;background:#f8fafc;padding:8px;border-radius:6px;border:1px solid #f1f5f9;">
                {sample_obs_html}
                {f'<div style="font-size:11px;color:#64748b;margin-top:6px;padding-left:4px;">+ {len(obs_list)-4} more observations...</div>' if len(obs_list) > 4 else ''}
            </div>
        </div>
        """

    html = f"""
    <!DOCTYPE html>
    <html>
    <head>
        <title>Material Diagnostic: {target_mat.title()}</title>
        <meta charset="utf-8">
        <style>
            body {{ font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, sans-serif; background: #f8fafc; color: #1e293b; margin: 0; padding: 28px; line-height: 1.5; }}
            .container {{ max-width: 960px; margin: 0 auto; }}
            .header {{ background: #ffffff; border: 1px solid #e2e8f0; border-radius: 8px; padding: 24px; margin-bottom: 20px; box-shadow: 0 1px 3px rgba(0,0,0,0.04); }}
            a {{ color: #0284c7; text-decoration: none; }}
            a:hover {{ text-decoration: underline; }}
        </style>
    </head>
    <body>
        <div class="container">
            <div style="margin-bottom:14px;">
                <a href="/klass/tracheostomy-tube" style="font-size:13px;font-weight:600;">← View Klass Diagnostic: Tracheostomy Tube</a>
            </div>
            <div class="header">
                <div style="display:flex;justify-content:space-between;align-items:flex-start;">
                    <div>
                        <div style="font-size:12px;font-weight:700;color:#0f766e;text-transform:uppercase;letter-spacing:0.5px;">Inverted Material Facet</div>
                        <h1 style="margin:4px 0 0 0;font-size:22px;color:#0f172a;">{target_mat.title()}</h1>
                    </div>
                    <span style="background:#0f766e;color:#ffffff;padding:5px 12px;border-radius:4px;font-size:13px;font-weight:700;">
                        {len(observations)} Observations / {len(klass_groups)} Klasses
                    </span>
                </div>
                <p style="color:#64748b;font-size:13px;margin:10px 0 0 0;">
                    Lineage mapping connecting raw catalog evidence to Klass categories. Green indicates recurring multi-observation consensus; red flags singletons for ETL bleed inspection.
                </p>
            </div>

            <h3 style="color:#334155;margin:0 0 12px 0;font-size:16px;">Connected Klasses & Lineage</h3>
            {sections or '<p style="color:#64748b;">No observations found for this material.</p>'}
        </div>
    </body>
    </html>
    """
    return HTMLResponse(content=html)

if __name__ == "__main__":
    port = int(os.getenv("PORT", 8371))
    print(f"\n==================================================")
    print(f"  ⚡ SERVER PRIME - Infinity Product Swarm Master")
    print(f"  Listening on: http://0.0.0.0:{port}")
    print(f"==================================================\n")
    uvicorn.run(app, host="0.0.0.0", port=port)
