import os
import sys
import json
import time
import base64
import io
import re
import datetime
import requests
import pypdfium2 as pdfium
from PIL import Image
from pymongo import MongoClient

OLLAMA_URL = "http://localhost:11434/api/generate"
MODEL_NAME = "qwen3-vl:latest"
MONGO_URI = "mongodb://localhost:27017/"
DB_NAME = "infinityproduct_dev"
OBSERVATIONS_COLL = "observations"
KLASSES_COLL = "klasses"
CATALOGS_COLL = "catalogs"

def normalize_sizes(sizes_input):
    """Deterministically clean and standardize medical size codes."""
    if not sizes_input:
        return []
    
    if isinstance(sizes_input, str):
        sizes_list = [sizes_input]
    elif isinstance(sizes_input, list):
        sizes_list = sizes_input
    else:
        sizes_list = [str(sizes_input)]
        
    standard_sizes = []
    for s in sizes_list:
        text = str(s).strip()
        text = re.sub(r'\bO\s*#', '0#', text, flags=re.IGNORECASE)
        text = re.sub(r'\b(OO|OOO)\s*#', lambda m: '00#' if len(m.group(1))==2 else '000#', text, flags=re.IGNORECASE)
        
        num_match = re.search(r'(000|00|[0-6])\s*#', text)
        if num_match:
            standard_sizes.append(f"{num_match.group(1)}#")
            continue
            
        fr_match = re.search(r'Fr\s*(\d+)', text, re.IGNORECASE)
        if fr_match:
            standard_sizes.append(f"Fr{fr_match.group(1)}")
            continue
            
        alpha_match = re.search(r'\b(XS|S|M|L|XL|XXL)\b', text, re.IGNORECASE)
        if alpha_match:
            standard_sizes.append(alpha_match.group(1).upper())
            continue
            
        if len(text) <= 15 and not any(c in text for c in ["\n", "{", "}"]):
            standard_sizes.append(text)
            
    seen = set()
    deduped = []
    for sz in standard_sizes:
        if sz not in seen:
            seen.add(sz)
            deduped.append(sz)
    return deduped

def normalize_materials(materials_list):
    """Deterministically sanitize and canonicalize chemical polymer terminology."""
    if not materials_list:
        return []
    if isinstance(materials_list, str):
        materials_list = [materials_list]
        
    canonical = []
    for mat in materials_list:
        m = str(mat).strip().lower()
        if any(ign in m for ign in ["latex free", "dehp free", "sterile", "single patient", "non-toxic"]):
            continue
        if "silicon" in m:
            canonical.append("Silicone")
        elif "pvc" in m or "polyvinyl" in m:
            canonical.append("Medical Grade PVC")
        elif "tpe" in m or "thermoplastic" in m:
            canonical.append("TPE (Thermoplastic Elastomer)")
        elif "polycarbonate" in m or re.search(r'\bpc\b', m):
            canonical.append("Polycarbonate (PC)")
        elif "polypropylene" in m or re.search(r'\bpp\b', m):
            canonical.append("Polypropylene (PP)")
        elif "polyethylene" in m or re.search(r'\bpe\b', m):
            canonical.append("Polyethylene (PE)")
        elif "steel" in m:
            canonical.append("Stainless Steel")
        elif "cotton" in m:
            canonical.append("Cotton")
        elif "non-woven" in m or "nonwoven" in m:
            canonical.append("Medical Non-woven")
        elif len(m) > 2 and len(m) < 40:
            canonical.append(mat.strip().title())
            
    seen = set()
    deduped = []
    for c in canonical:
        if c not in seen:
            seen.add(c)
            deduped.append(c)
    return deduped

def register_catalog_source(pdf_path, total_pages, ingested_obs_count, klass_histogram, start_page=4, end_page=48):
    """Store rich metadata for the catalog source including Klass distribution histogram."""
    try:
        client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=3000)
        db = client[DB_NAME]
        cat_col = db[CATALOGS_COLL]
        
        file_stat = os.stat(pdf_path) if os.path.exists(pdf_path) else None
        now = datetime.datetime.now(datetime.timezone.utc)
        
        cat_doc = {
            "catalog_name": os.path.splitext(os.path.basename(pdf_path))[0].title(),
            "filename": os.path.basename(pdf_path),
            "file_size_bytes": file_stat.st_size if file_stat else 0,
            "total_spread_pages": total_pages,
            "content_page_ranges": {
                "cover_and_index_pages": [1, 3],
                "product_pages_start": start_page,
                "product_pages_end": end_page,
                "active_product_spreads_count": (end_page - start_page + 1)
            },
            "ingested_observations_count": ingested_obs_count,
            "total_unique_klasses": len(klass_histogram),
            "klass_distribution_histogram": dict(sorted(klass_histogram.items(), key=lambda x: x[1], reverse=True)),
            "last_ingested_at": now
        }
        
        cat_col.update_one(
            {"filename": os.path.basename(pdf_path)},
            {"$set": cat_doc, "$setOnInsert": {"created_at": now}},
            upsert=True
        )
        print(f"  📚 [Catalog Source] Registered '{os.path.basename(pdf_path)}' ({len(klass_histogram)} Klasses) in '{CATALOGS_COLL}'")
    except Exception as e:
        print(f"  [!] Catalog registration error: {e}")

def persist_and_rollup_to_mongodb(observations, source_pdf="foyomed catalogue.pdf"):
    """Persist atomic observations to MongoDB and update living Klass ontology documents."""
    if not observations:
        return
        
    try:
        client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=3000)
        db = client[DB_NAME]
        obs_col = db[OBSERVATIONS_COLL]
        klass_col = db[KLASSES_COLL]
        
        now = datetime.datetime.now(datetime.timezone.utc)
        
        # 1. Insert Observations
        for obs in observations:
            obs["source_catalog"] = os.path.basename(source_pdf)
            obs["ingested_at"] = now
            obs_col.insert_one(obs)
            
        # 2. Rollup to Klass documents
        klasses_in_batch = set(obs.get("klass_name", "General") for obs in observations if obs.get("klass_name"))
        for k_name in klasses_in_batch:
            slug = re.sub(r'[^a-z0-9]+', '-', k_name.lower()).strip('-')
            k_obs = list(obs_col.find({"klass_name": k_name}))
            total_obs = len(k_obs)
            
            all_mats = sorted(list(set(m for o in k_obs for m in o.get("materials", []))))
            
            all_sizes = set()
            for o in k_obs:
                sz = o.get("attributes", {}).get("size")
                if sz:
                    all_sizes.add(sz)
            all_sizes = sorted(list(all_sizes))
            
            hero_img = None
            gallery_images = []
            for o in k_obs:
                img_url = o.get("image_url")
                if img_url and img_url not in gallery_images:
                    gallery_images.append(img_url)
            if gallery_images:
                hero_img = gallery_images[0]
                
            klass_doc = {
                "name": k_name,
                "slug": slug,
                "total_observations": total_obs,
                "hero_image": hero_img,
                "gallery_images": gallery_images,
                "observed_facet_space": {
                    "materials": all_mats,
                    "sizes": all_sizes
                },
                "updated_at": now
            }
            
            klass_col.update_one(
                {"slug": slug},
                {"$set": klass_doc, "$setOnInsert": {"created_at": now}},
                upsert=True
            )
            
        print(f"  🍃 [MongoDB] Ingested {len(observations)} observations -> Updated '{KLASSES_COLL}' and '{OBSERVATIONS_COLL}'")
    except Exception as e:
        print(f"  [!] MongoDB persistence error: {e}")

def extract_products_with_vision(image_pil, page_num):
    """Direct single-pass Vision-Language extraction using Qwen3-VL on the rendered page image."""
    # Convert PIL Image to Base64
    buffered = io.BytesIO()
    image_pil.save(buffered, format="JPEG", quality=90)
    img_b64 = base64.b64encode(buffered.getvalue()).decode("utf-8")
    
    prompt = """You are an expert visual medical catalog parser.
Look at this catalog page image and extract EVERY product box and table into clean JSON.

RULES:
1. IDENTIFY EACH PRODUCT BOX: Each distinct product heading and table (e.g. PVCFreeAnesthesiaMask, Soft Anesthesia Mask, Silicone Anesthesia Mask, Endoscope Mask, CPAP Mask) is its OWN item in the 'products' array.
2. ACCURATE MATERIALS: Read the material directly from the product box description (e.g. PVC, Silicone, TPE).
3. TABLE VARIANTS: For each product, extract all catalog numbers (Cat.No.) and sizes from its specific table into 'variants'.
4. IGNORE MARGIN TABS: Ignore vertical navigation margin tabs (e.g. 'Wound Dressing', 'Urology').

Output JSON format:
{
  "products": [
    {
      "klass_name": "Anesthesia Face Mask",
      "product_name": "PVC Free Anesthesia Mask",
      "materials": ["TPE (Thermoplastic Elastomer)", "Polypropylene (PP)"],
      "compliance_flags": {"latex_free": true, "sterile": false},
      "variants": [
        {"cat_no": "LB301100", "size": "0#", "connector": "15mmOD"},
        {"cat_no": "LB301102", "size": "2#", "connector": "22mmID"}
      ]
    },
    {
      "klass_name": "Anesthesia Face Mask",
      "product_name": "Soft Anesthesia Mask",
      "materials": ["Medical Grade PVC"],
      "compliance_flags": {"latex_free": true, "sterile": false},
      "variants": [
        {"cat_no": "LB302101", "size": "0#", "connector": "15mmOD"},
        {"cat_no": "LB302103", "size": "2#", "connector": "22mmID"}
      ]
    }
  ]
}"""

    payload = {
        "model": MODEL_NAME,
        "prompt": prompt,
        "images": [img_b64],
        "format": "json",
        "stream": False,
        "keep_alive": "1h",
        "options": {
            "temperature": 0.1,
            "num_ctx": 4096
        }
    }

    try:
        resp = requests.post(OLLAMA_URL, json=payload, stream=False, timeout=180)
        raw_response = resp.json().get("response", "").strip()
        
        data = []
        try:
            parsed = json.loads(raw_response)
            if isinstance(parsed, dict) and "products" in parsed:
                data = parsed["products"]
            elif isinstance(parsed, list):
                data = parsed
        except:
            match = re.search(r'\[.*\]', raw_response, re.DOTALL)
            if match:
                data = json.loads(match.group(0))
            else:
                return []
                
        atomic_observations = []
        for prod in data:
            if not isinstance(prod, dict):
                continue
                
            k_name = prod.get("klass_name", prod.get("category", "General")).strip()
            base_name = prod.get("product_name", prod.get("name", k_name)).strip()
            mats = normalize_materials(prod.get("materials", []))
            comp = prod.get("compliance_flags", {})
            desc = prod.get("description", "")
            
            variants = prod.get("variants", [])
            if not variants:
                raw_sz = prod.get("attributes", {}).get("sizes", prod.get("sizes", []))
                norm_sz = normalize_sizes(raw_sz)
                if norm_sz and len(norm_sz) > 1:
                    variants = [{"size": s} for s in norm_sz]
                else:
                    variants = [prod.get("attributes", {})]
                    
            for var in variants:
                if not isinstance(var, dict):
                    continue
                var_clean = {k: v for k, v in var.items() if v and str(v).lower() not in ["not specified", "null", "none", "n/a", "color"]}
                var_clean.pop("color", None)
                
                if "size" in var_clean:
                    sz_norm = normalize_sizes(var_clean["size"])
                    if sz_norm:
                        var_clean["size"] = sz_norm[0]
                        
                sku = var_clean.pop("cat_no", None)
                obs_doc = {
                    "klass_name": k_name,
                    "product_name": base_name,
                    "cat_no": sku,
                    "description": desc,
                    "materials": mats,
                    "compliance_flags": comp,
                    "attributes": {k: v for k, v in var_clean.items() if v is not None and v != "" and v != []}
                }
                atomic_observations.append(obs_doc)
                
        def size_rank(doc):
            sz = str(doc.get("attributes", {}).get("size", ""))
            order = ["000#", "00#", "0#", "1#", "2#", "3#", "4#", "5#", "6#", "XS", "S", "M", "L", "XL", "XXL"]
            if sz in order:
                return order.index(sz)
            return 99
            
        atomic_observations.sort(key=lambda d: (d.get("klass_name", ""), d.get("cat_no") or "", size_rank(d)))
        return atomic_observations
    except Exception as e:
        print(f"  [!] Vision extraction error on page {page_num}: {e}")
        return []

def process_catalog(pdf_path, start_page=4, end_page=48, output_file="extracted_products.json", images_dir="assets/catalog_pages"):
    print("\n" + "="*60)
    print(f"⚡ INFINITY CATALOG VISION ENGINE (Direct Qwen3-VL Single Pass)")
    print(f"   Target: {pdf_path}")
    print(f"   Model : {MODEL_NAME}")
    print(f"   Pages : {start_page} -> {end_page}")
    print(f"   DB    : {DB_NAME} -> collections: ['{CATALOGS_COLL}', '{KLASSES_COLL}', '{OBSERVATIONS_COLL}']")
    print("="*60)
    
    os.makedirs(images_dir, exist_ok=True)
    
    doc = pdfium.PdfDocument(pdf_path)
    all_extracted = []
    t_start = time.time()
    
    for sheet_idx in range(start_page - 1, min(end_page, len(doc))):
        sheet_num = sheet_idx + 1
        page_t0 = time.time()
        print(f"\n📄 [PDF SPREAD {sheet_num:02d}/{len(doc):02d}] Vision Ingestion started...")
        
        page = doc[sheet_idx]
        pil_image = page.render(scale=2).to_pil()
        
        w, h = pil_image.size
        # Slices off the vertical side margin tabs (rightmost ~7% of spread) destructively
        left_page_img = pil_image.crop((0, 0, w // 2, h))
        right_page_img = pil_image.crop((w // 2, 0, int(w * 0.93), h))
        
        pages_in_spread = [
            ("Left_Page", left_page_img),
            ("Right_Page", right_page_img)
        ]
        
        spread_products = []
        for side_name, side_img in pages_in_spread:
            page_filename = f"spread_{sheet_num:02d}_{side_name.lower()}.jpg"
            page_img_path = os.path.join(images_dir, page_filename)
            side_img.save(page_img_path)
            
            print(f"  ├─ 👁️ [Qwen3-VL Vision] Ingesting [{side_name}]...", end="", flush=True)
            products = extract_products_with_vision(side_img, sheet_num)
            print(f" -> Found {len(products)} products")
            
            for item in products:
                item["image_url"] = page_img_path.replace("\\", "/")
                spread_products.append(item)
                
                mats = ", ".join(item.get("materials", [])) or "N/A"
                sizes = item.get("attributes", {}).get("size") or "N/A"
                print(f"      • 📦 [{item.get('klass_name', 'General')}] {item.get('product_name')}")
                print(f"           ↳ Mats: {mats} | Size: {sizes}")
                
        all_extracted.extend(spread_products)
        print(f"  └─ Ingested {len(spread_products)} total products from Spread {sheet_num} in {time.time()-page_t0:.2f}s")
        
    with open(output_file, "w", encoding="utf-8") as f:
        json.dump(all_extracted, f, indent=2, ensure_ascii=False)
        
    persist_and_rollup_to_mongodb(all_extracted, source_pdf=pdf_path)
    
    import collections
    klass_counts = collections.Counter([item.get("klass_name", "General") for item in all_extracted])
    register_catalog_source(pdf_path, len(doc), len(all_extracted), dict(klass_counts), start_page=start_page, end_page=end_page)
        
    print("\n" + "="*60)
    print(f"✅ Pipeline complete in {time.time()-t_start:.2f}s!")
    print(f"   Total Observations Ingested: {len(all_extracted)}")
    print(f"   MongoDB Collections Updated: ['{CATALOGS_COLL}', '{KLASSES_COLL}', '{OBSERVATIONS_COLL}']")
    print("="*60 + "\n")
    return all_extracted

if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="Extract structured products from PDF catalog using Qwen3-VL Vision")
    parser.add_argument("--pdf", default="foyomed catalogue.pdf", help="Path to PDF catalog")
    parser.add_argument("--start", type=int, default=4, help="Start page number (1-indexed, products start on 4)")
    parser.add_argument("--end", type=int, default=48, help="End page number (1-indexed)")
    parser.add_argument("--out", default="extracted_products.json", help="Output JSON file")
    
    args = parser.parse_args()
    process_catalog(args.pdf, start_page=args.start, end_page=args.end, output_file=args.out)
