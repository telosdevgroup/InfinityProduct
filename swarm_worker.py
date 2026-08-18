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
        
    prompt = f"""You are an expert medical catalog extraction engine.
Extract EVERY distinct product model series from this catalog page OCR text into JSON.
DO NOT FUSE DIFFERENT MODEL HEADINGS (e.g. LB3011, LB3012, LB3021, LB3030 are separate products).

Output format:
{{
  "products": [
    {{
      "klass_name": "Natural category (e.g. 'Anesthesia Face Mask')",
      "product_name": "Product family name",
      "materials": ["Polymer names, e.g. Silicone, Medical Grade PVC"],
      "compliance_flags": {{"latex_free": true}},
      "variants": [
        {{"cat_no": "LB301100", "size": "0#", "connector": "15mmOD"}},
        {{"cat_no": "LB301102", "size": "2#", "connector": "22mmID"}}
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
            base_name = prod.get("product_name", k_name).strip()
            mats = normalize_materials(prod.get("materials", []))
            comp = prod.get("compliance_flags", {})
            desc = prod.get("description", "")
            
            variants = prod.get("variants", [])
            if not variants:
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
                sku_tag = f" [{sku}]" if sku else ""
                size_label = f" (Size {var_clean['size']})" if "size" in var_clean else ""
                
                atomic.append({
                    "klass_name": k_name,
                    "product_name": f"{base_name}{sku_tag}{size_label}",
                    "cat_no": sku,
                    "description": desc,
                    "materials": mats,
                    "compliance_flags": comp,
                    "attributes": {k: v for k, v in var_clean.items() if v}
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
                sz = p.get("attributes", {}).get("size") or "N/A"
                print(f"       • 📦 [{p.get('klass_name')}] {p.get('product_name')}")
                print(f"            ↳ Mats: {mats} | Size: {sz}")
                
            submit_resp = requests.post(
                f"{SERVER_PRIME_URL}/api/submit-work",
                json={"task_id": task_id, "products": extracted_products},
                timeout=10
            )
            print(f"   ↳ Server Prime status: {submit_resp.json().get('status')}")
            
        except requests.exceptions.ConnectionError:
            print(f"  [!] Cannot connect to Server Prime at {SERVER_PRIME_URL}. Retrying...", end="\r", flush=True)
            time.sleep(3)
        except Exception as e:
            print(f"\n  [!] Worker exception: {e}")
            time.sleep(2)

if __name__ == "__main__":
    worker_loop()
