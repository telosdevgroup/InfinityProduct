import os
import io
import re
import sys
import json
import time
import requests
import datetime
import numpy as np
from pymongo import MongoClient
import pypdfium2 as pdfium
from rapidocr_onnxruntime import RapidOCR

OLLAMA_URL = "http://localhost:11434/api/generate"
MODEL_NAME = "granite4.1:8b"
MONGO_URI = "mongodb://localhost:27017/"
DB_NAME = "infinityproduct_dev"
OBSERVATIONS_COLL = "observations"
KLASSES_COLL = "klasses"
CATALOGS_COLL = "catalogs"

def normalize_sizes(sizes_input):
    """Deterministically clean and standardize medical size codes."""
    if not sizes_input:
        return []
    
    # Handle single string like "Size O#-Neonate" or list
    if isinstance(sizes_input, str):
        sizes_list = [sizes_input]
    elif isinstance(sizes_input, list):
        sizes_list = sizes_input
    else:
        sizes_list = [str(sizes_input)]
        
    standard_sizes = []
    for s in sizes_list:
        text = str(s).strip()
        # Fix OCR confusion where letter 'O' was scanned instead of '0'
        text = re.sub(r'\bO\s*#', '0#', text, flags=re.IGNORECASE)
        text = re.sub(r'\b(OO|OOO)\s*#', lambda m: '00#' if len(m.group(1))==2 else '000#', text, flags=re.IGNORECASE)
        
        # Match medical sharp sizes: 000#, 00#, 0#, 1#, 2#, 3#, 4#, 5#, 6#
        num_match = re.search(r'(000|00|[0-6])\s*#', text)
        if num_match:
            standard_sizes.append(f"{num_match.group(1)}#")
            continue
            
        # Match Fr sizes
        fr_match = re.search(r'Fr\s*(\d+)', text, re.IGNORECASE)
        if fr_match:
            standard_sizes.append(f"Fr{fr_match.group(1)}")
            continue
            
        # Match standard alpha sizes
        alpha_match = re.search(r'\b(XS|S|M|L|XL|XXL)\b', text, re.IGNORECASE)
        if alpha_match:
            standard_sizes.append(alpha_match.group(1).upper())
            continue
            
        # If it's a clean short string, preserve it
        if len(text) <= 15 and not any(c in text for c in ["\n", "{", "}"]):
            standard_sizes.append(text)
            
    seen = set()
    deduped = []
    for sz in standard_sizes:
        if sz not in seen:
            seen.add(sz)
            deduped.append(sz)
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

def persist_and_rollup_to_mongodb(observations, source_pdf="foyomed catalogue.pdf", sheet_num=7):
    """Persist raw observations and auto-rebuild the aggregate Klass facet space."""
    if not observations:
        return
        
    try:
        client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=3000)
        db = client[DB_NAME]
        obs_col = db[OBSERVATIONS_COLL]
        klass_col = db[KLASSES_COLL]
        
        now = datetime.datetime.now(datetime.timezone.utc)
        
        for obs in observations:
            klass_name = obs.get("klass_name", "General Medical Consumable")
            
            # 1. Insert/update the raw observation instance
            obs_doc = {
                "klass_name": klass_name,
                "product_name": obs.get("product_name"),
                "description": obs.get("description"),
                "materials": obs.get("materials", []),
                "compliance_flags": obs.get("compliance_flags", {}),
                "features": obs.get("features", []),
                "attributes": obs.get("attributes", {}),
                "image_url": obs.get("image_url"),
                "source": {
                    "catalog": os.path.splitext(os.path.basename(source_pdf))[0].title(),
                    "pdf_file": os.path.basename(source_pdf),
                    "spread_sheet": sheet_num
                },
                "updated_at": now
            }
            
            obs_col.update_one(
                {"product_name": obs.get("product_name"), "source.pdf_file": os.path.basename(source_pdf)},
                {"$set": obs_doc, "$setOnInsert": {"created_at": now}},
                upsert=True
            )
            
            # 2. Re-compute aggregate facet space for this Klass
            all_klass_obs = list(obs_col.find({"klass_name": klass_name}))
            
            all_materials = set()
            all_sizes = set()
            all_features = set()
            all_images = []
            
            for o in all_klass_obs:
                for m in o.get("materials", []):
                    all_materials.add(m)
                for s in o.get("attributes", {}).get("sizes", []):
                    all_sizes.add(s)
                for f in o.get("features", []):
                    all_features.add(f)
                img = o.get("image_url")
                if img and img not in all_images:
                    all_images.append(img)
                    
            klass_doc = {
                "name": klass_name,
                "hero_image": all_images[0] if all_images else None,
                "gallery_images": all_images,
                "total_observations": len(all_klass_obs),
                "observed_facet_space": {
                    "materials": sorted(list(all_materials)),
                    "sizes": normalize_sizes(list(all_sizes)),
                    "common_features": sorted(list(all_features))[:15]
                },
                "updated_at": now
            }
            
            klass_col.update_one(
                {"name": klass_name},
                {"$set": klass_doc, "$setOnInsert": {"created_at": now}},
                upsert=True
            )
            
        print(f"  🍃 [MongoDB] Ingested {len(observations)} observations -> Updated '{KLASSES_COLL}' and '{OBSERVATIONS_COLL}'")
    except Exception as e:
        print(f"  [!] MongoDB error: {e}")

def normalize_materials(materials_list):
    """Normalize base polymers and isolate pure materials from marketing/compliance terms."""
    if not materials_list:
        return []
        
    known_polymers = {
        "silicone": "Silicone",
        "silicon": "Silicone",
        "tpe": "TPE (Thermoplastic Elastomer)",
        "pp": "Polypropylene (PP)",
        "polypropylene": "Polypropylene (PP)",
        "pvc": "Medical Grade PVC",
        "polyvinyl": "Medical Grade PVC",
        "pc": "Polycarbonate (PC)",
        "polycarbonate": "Polycarbonate (PC)",
        "neoprene": "Neoprene",
        "polyethylene": "Polyethylene (PE)",
        "pe": "Polyethylene (PE)",
        "polyisoprene": "Polyisoprene",
        "eva": "EVA",
        "abs": "ABS Plastic"
    }
    
    clean_mats = []
    for m in materials_list:
        m_lower = m.lower()
        matched = False
        for k, standard_name in known_polymers.items():
            if k in m_lower and "latex" not in m_lower and "dehp" not in m_lower:
                clean_mats.append(standard_name)
                matched = True
                break
        if not matched and "latex" not in m_lower and "free" not in m_lower and "sterile" not in m_lower:
            clean_mats.append(m.strip())
            
    seen = set()
    deduped = []
    for mat in clean_mats:
        if mat not in seen:
            seen.add(mat)
            deduped.append(mat)
    return deduped

def extract_facets_with_granite(page_text, page_num):
    """Send OCR text to Granite 4.1 8B with compact matrix extraction, then explode variants in Python."""
    prompt = f"""You are an expert medical catalog data extraction engine.
Analyze the raw OCR text from page {page_num} of a medical & surgical consumables catalog.

MISSION: Extract medical products into a COMPACT format with a 'variants' list for table rows/sizes.

CRITICAL NEGATIVE CONSTRAINTS:
1. IGNORE SIDEBAR MARGIN TABS: The sidebar margin contains vertical section headers like "Wound Dressing", "Urology", "Hypodermic", "Examination", "Medical Non-woven & Accessories", "Others". NEVER use these as product or category names!

Format per product:
{{
  "klass_name": "Natural category (e.g. 'Anesthesia Face Mask', 'Endotracheal Tube')",
  "product_name": "Product family name",
  "description": "Short summary of clinical use and key specifications",
  "materials": ["Polymer/metal names only, e.g. Silicone, TPE, Medical Grade PVC"],
  "compliance_flags": {{"latex_free": true/false, "sterile": true/false, "autoclavable": true/false}},
  "variants": [
    {{"cat_no": "LB301111", "size": "000#", "connector": "15mmOD"}},
    {{"cat_no": "LB301100", "size": "0#", "demographic": "Neonate", "connector": "15mmOD"}},
    {{"cat_no": "LB301102", "size": "2#", "demographic": "Pediatric", "color": "Yellow", "connector": "22mmID"}}
  ]
}}

Output JSON format:
{{
  "products": [
    {{
      "klass_name": "Anesthesia Face Mask",
      "product_name": "PVC Free Anesthesia Mask",
      "materials": ["TPE (Thermoplastic Elastomer)", "Polypropylene (PP)"],
      "compliance_flags": {{"latex_free": true, "sterile": false}},
      "variants": [
        {{"cat_no": "LB301111", "size": "000#", "connector": "15mmOD"}},
        {{"cat_no": "LB301102", "size": "2#", "demographic": "Pediatric", "color": "Yellow", "connector": "22mmID"}}
      ]
    }}
  ]
}}

OCR Text from Page {page_num}:
\"\"\"
{page_text}
\"\"\"
"""

    payload = {
        "model": MODEL_NAME,
        "prompt": prompt,
        "format": "json",
        "stream": False,
        "keep_alive": "1h",
        "options": {
            "temperature": 0.1,
            "num_ctx": 4096
        }
    }

    try:
        t0 = time.time()
        resp = requests.post(OLLAMA_URL, json=payload, stream=False, timeout=180)
        dur = time.time() - t0
        
        raw_response = resp.json().get("response", "").strip()
        
        data = []
        try:
            parsed = json.loads(raw_response)
            if isinstance(parsed, list):
                data = parsed
            elif isinstance(parsed, dict):
                found_list = False
                for val in parsed.values():
                    if isinstance(val, list) and len(val) > 0 and isinstance(val[0], dict):
                        data = val
                        found_list = True
                        break
                if not found_list:
                    data = [parsed]
        except Exception:
            match = re.search(r'\[.*\]', raw_response, re.DOTALL)
            if match:
                data = json.loads(match.group(0))
            else:
                return []
        
        # ⚡ Instant Python Cartesian Variant Explosion
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
            
            # If no explicit variants table was found, synthesize from attributes or single entity
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
                
                # Normalize size attribute in variant
                if "size" in var_clean:
                    sz_norm = normalize_sizes(var_clean["size"])
                    if sz_norm:
                        var_clean["size"] = sz_norm[0]
                        
                sku = var_clean.pop("cat_no", None)
                sku_tag = f" [{sku}]" if sku else ""
                size_label = f" (Size {var_clean['size']})" if "size" in var_clean else ""
                
                obs_doc = {
                    "klass_name": k_name,
                    "product_name": f"{base_name}{sku_tag}{size_label}",
                    "cat_no": sku,
                    "description": desc,
                    "materials": mats,
                    "compliance_flags": comp,
                    "attributes": {k: v for k, v in var_clean.items() if v is not None and v != "" and v != []}
                }
                atomic_observations.append(obs_doc)
                
        # Sort variants deterministically: by Klass, then natural size rank, then SKU
        def size_rank(doc):
            sz = str(doc.get("attributes", {}).get("size", ""))
            order = ["000#", "00#", "0#", "1#", "2#", "3#", "4#", "5#", "6#", "XS", "S", "M", "L", "XL", "XXL"]
            if sz in order:
                return order.index(sz)
            return 99
            
        atomic_observations.sort(key=lambda d: (d.get("klass_name", ""), d.get("cat_no") or "", size_rank(d)))
        return atomic_observations
    except Exception as e:
        print(f"  [!] Extraction error on page {page_num}: {e}")
        return []

def process_catalog(pdf_path, start_page=1, end_page=48, output_file="extracted_products.json", images_dir="assets/catalog_pages"):
    print("\n" + "="*60)
    print(f"⚡ INFINITY CATALOG ENRICHMENT ENGINE (Klasses, Observations, & Images)")
    print(f"   Target: {pdf_path}")
    print(f"   Model : {MODEL_NAME}")
    print(f"   Pages : {start_page} -> {end_page}")
    print(f"   DB    : {DB_NAME} -> collections: ['{CATALOGS_COLL}', '{KLASSES_COLL}', '{OBSERVATIONS_COLL}']")
    print("="*60)
    
    t_start = time.time()
    doc = pdfium.PdfDocument(pdf_path)
    ocr = RapidOCR()
    os.makedirs(images_dir, exist_ok=True)
    
    all_extracted = []
    
    for p_idx in range(start_page - 1, min(end_page, len(doc))):
        sheet_num = p_idx + 1
        page_t0 = time.time()
        print(f"\n📄 [PDF SPREAD {sheet_num:02d}/{len(doc):02d}] Ingestion started...")
        
        page = doc.get_page(p_idx)
        pil_img = page.render(scale=2.0).to_pil()
        w, h = pil_img.size
        
        half_w = int(w * 0.5)
        pages_in_spread = [
            ("Left_Page", pil_img.crop((0, 0, half_w, h))),
            ("Right_Page", pil_img.crop((half_w, 0, w, h)))
        ]
        
        spread_products = []
        
        for side_name, side_img in pages_in_spread:
            # Save visual page asset for UI & persistence
            page_filename = f"spread_{sheet_num:02d}_{side_name.lower()}.jpg"
            page_img_path = os.path.join(images_dir, page_filename)
            side_img.save(page_img_path)
            
            img_np = np.array(side_img)
            ocr_res, _ = ocr(img_np)
            if not ocr_res:
                continue
                
            text = "\n".join([line[1] for line in ocr_res])
            if len(text.strip()) < 30:
                continue
                
            print(f"\n  ├─ Ingesting [{side_name}] ({len(text)} OCR chars)...", end="", flush=True)
            products = extract_facets_with_granite(text, sheet_num)
            print(f" -> Found {len(products)} products")
            
            for item in products:
                item["image_url"] = page_img_path.replace("\\", "/")
                spread_products.append(item)
                
                mats = ", ".join(item.get("materials", [])) or "N/A"
                sizes = ", ".join(item.get("attributes", {}).get("sizes", [])) or "N/A"
                print(f"      • 📦 [{item.get('klass_name', 'General')}] {item.get('product_name')}")
                print(f"           ↳ Mats: {mats} | Sizes: {sizes}")
                
        all_extracted.extend(spread_products)
        print(f"\n  └─ Ingested {len(spread_products)} total products from Spread {sheet_num} in {time.time()-page_t0:.2f}s")
        
    with open(output_file, "w", encoding="utf-8") as f:
        json.dump(all_extracted, f, indent=2, ensure_ascii=False)
        
    # Persist and auto-rollup into Klass ontology
    persist_and_rollup_to_mongodb(all_extracted, source_pdf=pdf_path)
    
    # Register source catalog metadata with exact product page bounds and Klass histogram
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
    parser = argparse.ArgumentParser(description="Extract structured products from PDF catalog using OCR + Granite")
    parser.add_argument("--pdf", default="foyomed catalogue.pdf", help="Path to PDF catalog")
    parser.add_argument("--start", type=int, default=4, help="Start page number (1-indexed, products start on 4)")
    parser.add_argument("--end", type=int, default=48, help="End page number (1-indexed)")
    parser.add_argument("--out", default="extracted_products.json", help="Output JSON file")
    
    args = parser.parse_args()
    process_catalog(args.pdf, start_page=args.start, end_page=args.end, output_file=args.out)
