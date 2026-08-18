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
import argparse

parser = argparse.ArgumentParser(description="Infinity Product Swarm Worker")
parser.add_argument("--server", default=os.getenv("SERVER_PRIME_URL", "http://192.168.1.213:8371"), help="Server Prime URL")
parser.add_argument("--mongo_url", default=os.getenv("MONGO_URI", "192.168.1.213"), help="Mongo / Server Prime Host IP")
parser.add_argument("--ollama", default=os.getenv("OLLAMA_URL", "http://localhost:11434/api/generate"), help="Local Ollama URL")
parser.add_argument("--model", default=os.getenv("MODEL_NAME", "granite4.1:8b"), help="Ollama model name")
args, _ = parser.parse_known_args()

# Normalize server URL if user passes IP or raw host via --mongo_url / --server
server_host = args.mongo_url if args.mongo_url != "192.168.1.213" or "--server" not in sys.argv else args.server
if not server_host.startswith("http"):
    server_host = f"http://{server_host}:8371"
elif ":" not in server_host.split("//")[-1]:
    server_host = f"{server_host}:8371"

SERVER_PRIME_URL = server_host if "--server" in sys.argv or "--mongo_url" in sys.argv else args.server
OLLAMA_URL = args.ollama
MODEL_NAME = args.model
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
        
    img_h, img_w = img_np.shape[:2]
    mid_x = img_w // 2
    
    # Sort bounding boxes into natural reading order: Left Column (top-to-bottom), then Right Column (top-to-bottom)
    left_col = []
    right_col = []
    
    for item in ocr_res:
        box, text_val, conf = item[0], item[1], item[2]
        center_x = sum([p[0] for p in box]) / 4.0
        center_y = sum([p[1] for p in box]) / 4.0
        if center_x < mid_x:
            left_col.append((center_y, text_val))
        else:
            right_col.append((center_y, text_val))
            
    left_col.sort(key=lambda x: x[0])
    right_col.sort(key=lambda x: x[0])
    
    left_text = "\n".join([t[1] for t in left_col])
    right_text = "\n".join([t[1] for t in right_col])
    
    text = f"--- COLUMN 1 ---\n{left_text}\n\n--- COLUMN 2 ---\n{right_text}" if right_text else left_text
    if len(text.strip()) < 60:
        return []
        
    prompt = f"""You are an expert universal medical device catalog extraction engine.
Extract EVERY distinct product model block on this page into clean structured JSON.

CRITICAL RULES:
1. PRODUCT BLOCK INTEGRITY:
   - A page may contain multiple independent product blocks side-by-side or stacked.
   - Each product block consists of its title/heading, description, and its own SKU specification table.
   - Never merge SKUs or specifications from one product block into a different product.
2. klass_name: Choose the ordinary generic product type that would remain if all configuration choices were removed (e.g. 'Anesthesia Mask', 'Foley Catheter', 'Endotracheal Tube', 'Tracheostomy Tube', 'Laryngeal Mask', 'Nebulizer Mask', 'Suction Catheter').
   - DO NOT include configuration adjectives in klass_name (e.g. output 'Anesthesia Mask', NOT 'PVC Free Anesthesia Mask' or 'Silicone Anesthesia Mask').
   - DO NOT over-generalize beyond the recognized product type (e.g. output 'Endobronchial Tube', NOT 'Tube').
3. raw_product_name: Preserve the manufacturer's complete original heading verbatim for that specific product block.
4. facet_bag: Extract EVERY meaningful qualifier stated in that model's text (material, ways, valve, reusable/disposable, cuff, connector, etc.). Free-form discovered keys are expected.
5. variants: List individual SKU entries belonging ONLY to that model's table with their SKU-specific facets (size, gauge, cat_no, connector).

JSON Schema:
{{
  "products": [
    {{
      "klass_name": "Generic Product Type (e.g. 'Anesthesia Mask')",
      "raw_product_name": "Full Original Heading",
      "facet_bag": {{
        "material": ["Observed materials"],
        "use": "Single Patient Use / Reusable"
      }},
      "variants": [
        {{
          "cat_no": "SKU code",
          "facet_bag": {{
            "size": "Observed size",
            "connector": "Observed connector"
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
            t0 = time.time()
            img_payload = data.get("image_b64") or data.get("image_path")
            extracted_products = extract_products_from_image(img_payload, data["spread_num"])
            dur = time.time() - t0
            
            # Submit to Server Prime
            submit_resp = requests.post(
                f"{SERVER_PRIME_URL}/api/submit-work",
                json={"task_id": task_id, "products": extracted_products},
                timeout=10
            )
            res_data = submit_resp.json()
            rem = res_data.get("remaining_tasks", "?")

            if extracted_products:
                print(f"\n⚡ {task_id}: Ingested {len(extracted_products)} products ({dur:.1f}s) | ⏳ {rem} remaining")
                for p in extracted_products:
                    k = p.get('klass_name')
                    name = p.get('product_name')
                    print(f"    • 📦 [{k}] {name}")
            else:
                print(f"  ⏭️ {task_id}: [Non-product page skipped] | ⏳ {rem} remaining", end="\r", flush=True)
            
        except requests.exceptions.ConnectionError:
            print(f"  [!] Cannot connect to Server Prime at {SERVER_PRIME_URL}. Retrying...", end="\r", flush=True)
            time.sleep(3)
        except Exception as e:
            print(f"\n  [!] Worker exception: {e}")
            time.sleep(2)

if __name__ == "__main__":
    worker_loop()
