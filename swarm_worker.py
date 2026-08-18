import os
import sys
import time
import json
import socket
import re
import requests
import numpy as np
from PIL import Image
from rapidocr_onnxruntime import RapidOCR

SERVER_PRIME_URL = os.getenv("SERVER_PRIME_URL", "http://localhost:8371")
OLLAMA_URL = os.getenv("OLLAMA_URL", "http://localhost:11434/api/generate")
MODEL_NAME = os.getenv("MODEL_NAME", "granite4.1:8b")
WORKER_NAME = f"worker-{socket.gethostname()}-{os.getpid()}"

ocr = RapidOCR()

def normalize_sizes(sizes_input):
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

import base64
import io

def extract_products_from_image(image_data, spread_num):
    """Run OCR on image (base64 or local path), send text to Granite, explode variants."""
    pil_img = None
    if isinstance(image_data, str) and len(image_data) > 300:
        # Base64 image payload from Server Prime
        img_bytes = base64.b64decode(image_data)
        pil_img = Image.open(io.BytesIO(img_bytes))
    elif isinstance(image_data, str) and os.path.exists(image_data):
        pil_img = Image.open(image_data)
        
    if not pil_img:
        return []
        
    img_np = np.array(pil_img)
    ocr_res, _ = ocr(img_np)
    if not ocr_res:
        return []
        
    text = "\n".join([line[1] for line in ocr_res])
    if len(text.strip()) < 30:
        return []
        
    prompt = f"""You are an expert medical device catalog extraction engine.
Extract EVERY product model series from this catalog page OCR text into clean structured JSON.

CRITICAL RULES:
1. klass_name: Choose the ordinary generic product type that would remain if all configuration choices were removed (e.g. 'Foley Catheter', 'Endotracheal Tube', 'Tracheostomy Tube', 'Anesthesia Mask', 'Laryngeal Mask', 'Suction Catheter', 'Urethral Catheter', 'Urine Drainage Bag', 'Spinal Needle', 'Infusion Set').
   - DO NOT include configuration adjectives in klass_name (e.g. do NOT output '3-Way Latex Foley Catheter', 'Oral Preformed Endotracheal Tube', 'Disposable PVC Laryngeal Mask' as klass_name).
   - DO NOT over-generalize beyond the recognized product type (e.g. output 'Endobronchial Tube', NOT 'Tube').
2. raw_product_name: Preserve the manufacturer's complete original name verbatim (e.g. '3-Way Latex Foley Catheter', 'Oral Preformed Endotracheal Tube').
3. facet_bag: Extract EVERY meaningful qualifier removed from the name or stated in descriptions (e.g. ways, route, form, tip_style, construction, cuff, materials, compliance, balloon_capacity, connectors, etc.). Free-form discovered keys are expected.
4. variants: List individual SKU entries with their SKU-specific facets (e.g. size, gauge, length, balloon_capacity).

JSON Schema:
{{
  "products": [
    {{
      "klass_name": "Generic Product Type (e.g. 'Foley Catheter')",
      "raw_product_name": "Full Original Name (e.g. '3-Way Silicone Foley Catheter')",
      "facet_bag": {{
        "material": ["100% Silicone"],
        "ways": "3-Way",
        "tip_style": "Standard / Tiemann",
        "cuff": "Cuffed / Uncuffed"
      }},
      "variants": [
        {{
          "cat_no": "LB123401",
          "facet_bag": {{
            "size": "16Fr",
            "balloon_capacity": "30ml"
          }}
        }}
      ]
    }}
  ]
}}

OCR Text:
\"\"\"
{text}
\"\"\"
"""

    payload = {
        "model": MODEL_NAME,
        "prompt": prompt,
        "format": "json",
        "stream": False,
        "keep_alive": "1h",
        "options": {"temperature": 0.1, "num_ctx": 4096}
    }

    try:
        resp = requests.post(OLLAMA_URL, json=payload, timeout=90)
        raw = resp.json().get("response", "").strip()
        
        data = []
        try:
            parsed = json.loads(raw)
            if isinstance(parsed, dict) and "products" in parsed:
                data = parsed["products"]
            elif isinstance(parsed, list):
                data = parsed
            elif isinstance(parsed, dict):
                data = [parsed]
        except Exception:
            match = re.search(r'\[.*\]', raw, re.DOTALL)
            if match:
                data = json.loads(match.group(0))
            else:
                match_dict = re.search(r'\{.*\}', raw, re.DOTALL)
                if match_dict:
                    parsed = json.loads(match_dict.group(0))
                    data = parsed.get("products", [parsed])
                else:
                    return []
        
        atomic = []
        for prod in data:
            if not isinstance(prod, dict):
                continue
            k_name = prod.get("klass_name", "General").strip()
            raw_p_name = prod.get("raw_product_name", prod.get("product_name", k_name)).strip()
            
            # Base family-level facets
            base_facets = prod.get("facet_bag", prod.get("attributes", {}))
            if not isinstance(base_facets, dict):
                base_facets = {}
                
            # Extract materials cleanly
            prod_mats = prod.get("materials", base_facets.get("material", base_facets.get("materials", [])))
            mats = normalize_materials(prod_mats)
            
            variants = prod.get("variants", [])
            if not variants:
                variants = [{"facet_bag": {}}]
                
            for var in variants:
                if not isinstance(var, dict):
                    continue
                var_facets = var.get("facet_bag", var.get("attributes", {}))
                if not isinstance(var_facets, dict):
                    var_facets = {k: v for k, v in var.items() if k not in ["cat_no", "sku", "facet_bag"]}
                    
                # Merge product-level facet_bag + variant-level facet_bag
                merged_facets = dict(base_facets)
                merged_facets.update(var_facets)
                
                # Clean invalid/placeholder values
                clean_facets = {}
                for fk, fv in merged_facets.items():
                    if fv and str(fv).lower() not in ["not specified", "null", "none", "n/a", "color", "unknown"]:
                        if fk.lower() == "size":
                            sz_norm = normalize_sizes(fv)
                            clean_facets["size"] = sz_norm[0] if sz_norm else str(fv)
                        elif isinstance(fv, list):
                            clean_facets[fk] = [str(x).strip() for x in fv if x]
                        else:
                            clean_facets[fk] = str(fv).strip()
                            
                sku = var.get("cat_no", var.get("sku"))
                sku_tag = f" [{sku}]" if sku else ""
                size_label = f" (Size {clean_facets['size']})" if "size" in clean_facets else ""
                
                atomic.append({
                    "klass_name": k_name,
                    "product_name": f"{raw_p_name}{sku_tag}{size_label}",
                    "raw_product_name": raw_p_name,
                    "cat_no": sku,
                    "materials": mats,
                    "facet_bag": clean_facets,
                    "attributes": clean_facets
                })
        return atomic
    except Exception as e:
        print(f"  [!] Worker extraction error: {e}")
        return []

def worker_loop():
    print(f"\n==================================================")
    print(f"  ⚡ DUMB SWARM WORKER: {WORKER_NAME}")
    print(f"  Target Server Prime: {SERVER_PRIME_URL}")
    print(f"  Local Ollama Model : {MODEL_NAME}")
    print(f"==================================================\n")
    
    while True:
        try:
            r = requests.get(f"{SERVER_PRIME_URL}/api/get-work?worker={WORKER_NAME}", timeout=5)
            if r.status_code != 200:
                time.sleep(2)
                continue
                
            job = r.json()
            if not job.get("has_work"):
                print("  💤 [Idle] No pending tasks in Server Prime queue. Waiting 2s...", end="\r", flush=True)
                time.sleep(2)
                continue
                
            task_id = job["task_id"]
            data = job["data"]
            print(f"\n⚡ [Work Claimed] Task: {task_id} | Spread {data['spread_num']} ({data['side']})")
            
            t0 = time.time()
            img_payload = data.get("image_b64") or data.get("image_path")
            extracted_products = extract_products_from_image(img_payload, data["spread_num"])
            dur = time.time() - t0
            
            print(f"   ↳ Extracted {len(extracted_products)} products in {dur:.2f}s:")
            for p in extracted_products:
                mats = ", ".join(p.get("materials", [])) or "N/A"
                attrs = p.get("attributes", {})
                attr_str = " | ".join([f"{k.replace('_', ' ').title()}: {v}" for k, v in attrs.items()]) if attrs else "None"
                print(f"       • 📦 [{p.get('klass_name')}] {p.get('product_name')}")
                print(f"            ↳ Materials: {mats} | Attributes: {attr_str}")
                
            submit_resp = requests.post(
                f"{SERVER_PRIME_URL}/api/submit-work",
                json={"task_id": task_id, "products": extracted_products},
                timeout=10
            )
            res_data = submit_resp.json()
            rem = res_data.get("remaining_tasks", "?")
            print(f"   ↳ Server Prime status: {res_data.get('status')} | ⏳ Remaining in queue: {rem} tasks")
            
        except requests.exceptions.ConnectionError:
            print(f"  [!] Cannot connect to Server Prime at {SERVER_PRIME_URL}. Retrying...", end="\r", flush=True)
            time.sleep(3)
        except Exception as e:
            print(f"\n  [!] Worker exception: {e}")
            time.sleep(2)

if __name__ == "__main__":
    worker_loop()
