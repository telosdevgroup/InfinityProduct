import xml.etree.ElementTree as ET
import urllib.request
import requests
import hashlib
import datetime
import json
import re
import os
import sys
import time
from pymongo import MongoClient

# Ensure project root is in sys.path
ROOT_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if ROOT_DIR not in sys.path:
    sys.path.insert(0, ROOT_DIR)

from core.celery_app import app, REDIS_HOST

MONGO_HOST = os.environ.get("MONGO_HOST", REDIS_HOST)
MONGO_URI = f"mongodb://{MONGO_HOST}:27017/"
DB_NAME = "infinityproduct_dev"
LOG_FILE_PATH = os.path.join(ROOT_DIR, "factory_stream.log")

raw_host = os.environ.get("OLLAMA_HOST", "127.0.0.1:11434")
if not raw_host.startswith("http://") and not raw_host.startswith("https://"):
    raw_host = f"http://{raw_host}"
if ":11434" not in raw_host and ("localhost" in raw_host or "127.0.0.1" in raw_host or "0.0.0.0" in raw_host):
    raw_host = f"{raw_host.rstrip('/')}:11434"
if "0.0.0.0" in raw_host:
    raw_host = raw_host.replace("0.0.0.0", "127.0.0.1")

OLLAMA_HOST = raw_host
OLLAMA_MODEL = os.environ.get("OLLAMA_MODEL", "granite4.1:8b")

# ANSI Colors for terminal output
CYAN = "\033[96m"
GREEN = "\033[92m"
YELLOW = "\033[93m"
RED = "\033[91m"
MAGENTA = "\033[95m"
BLUE = "\033[94m"
BOLD = "\033[1m"
DIM = "\033[2m"
RESET = "\033[0m"

STATION_COLORS = {
    "SITEMAP": MAGENTA,
    "INGEST": CYAN,
    "KLASS": BLUE,
    "RECONCILE": MAGENTA + BOLD,
    "BLURB": GREEN,
    "FACET": YELLOW,
    "QUEUE": DIM,
    "DONE": GREEN + BOLD,
    "ERROR": RED + BOLD,
}

STATION_QUEUES = {
    "SITEMAP": "check_sitemap_q",
    "INGEST": "ingest_q",
    "KLASS": "klass_q",
    "RECONCILE": "klass_reconcile_q",
    "BLURB": "klass_blurb_q",
    "FACET": "facet_blurb_q",
}

_redis_log_client = None
def _get_redis_log_client():
    global _redis_log_client
    if _redis_log_client is None:
        try:
            import redis
            _redis_log_client = redis.Redis(host=REDIS_HOST, port=6379, db=0, protocol=2, socket_timeout=0.2)
        except Exception:
            pass
    return _redis_log_client

def factory_log(station: str, message: str, blank_after: bool = False):
    """
    Emits loud, colorized factory logs with real-time queue depth (N)
    to stdout and appends to factory_stream.log with clean section spacing.
    """
    color = STATION_COLORS.get(station, "")
    
    depth_str = ""
    q_name = STATION_QUEUES.get(station)
    if q_name:
        try:
            r = _get_redis_log_client()
            if r:
                depth = r.llen(q_name)
                depth_str = f" ({depth:,})"
        except Exception:
            pass

    badge_text = f"{station}{depth_str}"
    badge = f"{color}{badge_text:<15}{RESET}"
    formatted_console = f"{badge} {message}"
    formatted_plain = f"{badge_text:<15} {message}"

    # Print directly to worker stdout
    sys.stdout.write(formatted_console + "\n")
    if blank_after:
        sys.stdout.write("\n")
    sys.stdout.flush()

    # Append plain text to factory_stream.log
    try:
        with open(LOG_FILE_PATH, "a", encoding="utf-8") as f:
            f.write(formatted_plain + "\n")
            if blank_after:
                f.write("\n")
    except Exception:
        pass

def get_mongo_db():
    client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=3000)
    return client[DB_NAME]

def parse_xml_elements(xml_text):
    clean_xml = re.sub(r'\sxmlns="[^"]+"', '', xml_text, count=1)
    return ET.fromstring(clean_xml)

def slugify(text: str) -> str:
    if not text:
        return ""
    text = str(text).lower().replace("_", "-").replace(" ", "-").replace("/", "-")
    return re.sub(r"[^a-z0-9\-]+", "", text).strip("-")

def format_title(text: str) -> str:
    if not text:
        return ""
    return " ".join(w.capitalize() for w in str(text).replace("-", " ").replace("_", " ").split())

def to_plural_name(title: str) -> str:
    title = title.strip().lower()
    if title.endswith("s") or title.endswith("formula") or title.endswith("tape"):
        return title
    elif title.endswith("y") and not title.endswith("ey") and not title.endswith("ay"):
        return title[:-1] + "ies"
    elif title.endswith("ch") or title.endswith("sh") or title.endswith("x") or title.endswith("ss"):
        return title + "es"
    else:
        return title + "s"

def extract_facetbag_from_raw(title: str, desc: str, vendor: str = None) -> dict:
    """
    Deterministic multi-attribute FacetBag extraction from raw title and description.
    """
    full_text = f"{title} {desc}".lower()
    facet_bag = {}

    # 1. Materials
    for m in ["nitrile", "vinyl", "latex", "silicone", "foam", "cotton", "gauze", "silver", "alginate", "polyurethane", "stainless steel", "polychloroprene", "rubber", "hydrocolloid", "hydrogel", "collagen", "zinc", "calcium alginate"]:
        if m in ["latex", "rubber"]:
            # Check for negative latex-free statements (e.g. "latex-free", "latex- and powder-free", "contains no latex", "free of latex")
            if (re.search(r"\blatex\s*-\s*(?:and\s*[\w-]+\s*)?free\b", full_text) or
                "latex-free" in full_text or "latex free" in full_text or "not made with natural rubber latex" in full_text or
                "non-latex" in full_text or "without latex" in full_text or "no latex" in full_text or "free of latex" in full_text):
                facet_bag["Latex Safety"] = "Latex-Free"
                continue
        if re.search(r"\b" + re.escape(m) + r"\b", full_text):
            facet_bag.setdefault("Material", []).append(m.title())

    # 2. Powder Content
    if "powder-free" in full_text or "powder free" in full_text or "powderfree" in full_text:
        facet_bag["Powder Content"] = "Powder-Free"
    elif "powdered" in full_text:
        facet_bag["Powder Content"] = "Powdered"

    # 3. Grade / Application
    if "chemo" in full_text or "chemotherapy" in full_text or "usp 800" in full_text:
        facet_bag["Safety Rating"] = "Chemo Rated"
    if "exam grade" in full_text or "examination grade" in full_text or "medical grade" in full_text:
        facet_bag["Grade"] = "Medical Exam Grade"
    elif "surgical grade" in full_text:
        facet_bag["Grade"] = "Surgical Grade"

    # 4. Thickness / Gauge
    mil_match = re.search(r'\b(\d+(?:\.\d+)?)\s*mil\b', full_text)
    if mil_match:
        facet_bag["Thickness"] = f"{mil_match.group(1)} mil"
    elif "heavy duty" in full_text or "high risk" in full_text:
        facet_bag["Thickness"] = "Heavy Duty"

    # 5. Hand Specificity
    if "ambidextrous" in full_text:
        facet_bag["Hand Fitting"] = "Ambidextrous"
    elif "hand specific" in full_text or "right/left" in full_text:
        facet_bag["Hand Fitting"] = "Hand-Specific"

    # 6. Sizing
    for sz in ["X-Small", "Small", "Medium", "Large", "Extra Large", "2XL", "3XL", "Pediatric", "Adult", "Bariatric"]:
        if re.search(r"\b" + re.escape(sz.lower()) + r"\b", full_text):
            facet_bag["Size"] = sz
            break

    # 7. Colors
    for c in ["blue", "pink", "black", "green", "clear", "white", "purple", "yellow", "teal", "orange"]:
        if re.search(r"\b" + re.escape(c) + r"\b", full_text):
            facet_bag["Color"] = c.title()
            break

    # 8. Sterility
    if "non-sterile" in full_text or "nonsterile" in full_text or "non sterile" in full_text:
        facet_bag["Sterility"] = "Non-Sterile"
    elif "sterile" in full_text:
        facet_bag["Sterility"] = "Sterile"

    # 9. Cuff Styles
    for cuff in ["beaded cuff", "extended cuff", "standard cuff", "rolled cuff"]:
        if cuff in full_text:
            facet_bag["Cuff"] = cuff.title()
            break

    # 10. Textures
    for tex in ["textured fingertips", "textured fingers", "fully textured", "micro-textured", "smooth"]:
        if tex in full_text:
            facet_bag["Texture"] = "Textured Fingertips" if "textured finger" in tex else tex.title()
            break

    # 11. Absorbency (Wound care / Incontinence)
    if "maximum absorbency" in full_text or "ultimate absorbency" in full_text or "super absorbency" in full_text or "heavy absorbency" in full_text:
        facet_bag["Absorbency"] = "Heavy / Maximum"
    elif "moderate absorbency" in full_text or "regular absorbency" in full_text:
        facet_bag["Absorbency"] = "Moderate"
    elif "light absorbency" in full_text or "ultra thin" in full_text:
        facet_bag["Absorbency"] = "Light"

    # 12. Adhesion & Border Style (Dressings)
    if "silicone adhesive" in full_text or "soft silicone" in full_text:
        facet_bag["Adhesive Type"] = "Soft Silicone"
    elif "acrylic adhesive" in full_text:
        facet_bag["Adhesive Type"] = "Acrylic"
    
    if "with border" in full_text or "bordered" in full_text or "adhesive border" in full_text:
        facet_bag["Border Style"] = "Bordered"
    elif "non-bordered" in full_text or "without border" in full_text or "non bordered" in full_text:
        facet_bag["Border Style"] = "Non-Bordered"

    # 13. Active Agents / Antimicrobial
    for ag in ["silver", "honey", "iodine", "cadexomer", "phmb", "chlorhexidine", "bacitracin", "mupirocin", "zinc"]:
        if re.search(r"\b" + re.escape(ag) + r"\b", full_text):
            facet_bag.setdefault("Active Agent", []).append(ag.title())

    # 14. Vendor
    if vendor:
        facet_bag["Vendor"] = vendor

    return facet_bag

KNOWN_STORES = {
    "emedicalkits": "https://emedicalkits.com",
    "tigermedical": "https://tigermedical.com",
}

# ==============================================================================
# STATION 1: SITEMAP & HIGH-SPEED FEED DISCOVERY WORKER (check_sitemap_q)
# ==============================================================================
@app.task(name="tasks.sync_source_catalog", bind=True)
def sync_source_catalog(self, source_id: str = "emedicalkits", base_url: str = None):
    """
    High-speed deterministic catalog ingestion across arbitrary Shopify stores:
    Paginates /products.json?limit=250 to ingest all products and extract facets in seconds.
    """
    db = get_mongo_db()
    source_products_col = db["source_products"]
    discovered_urls_col = db["discovered_urls"]
    sitemaps_col = db["sitemaps"]
    now = datetime.datetime.now(datetime.timezone.utc)

    if not base_url:
        base_url = KNOWN_STORES.get(source_id, f"https://{source_id}.com")
    base_url = base_url.rstrip("/")

    factory_log("SITEMAP", f"Starting high-speed catalog sync for {source_id} ({base_url})...")

    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        "Accept": "application/json"
    }

    page = 1
    total_ingested = 0

    while True:
        feed_url = f"{base_url}/products.json?limit=250&page={page}"
        factory_log("INGEST", f"GET /products.json?limit=250&page={page}")
        t0 = time.time()

        try:
            resp = requests.get(feed_url, headers=headers, timeout=20)
            if resp.status_code != 200:
                factory_log("ERROR", f"HTTP {resp.status_code} on page {page}")
                break

            products = resp.json().get("products", [])
            if not products:
                break

            dur = round(time.time() - t0, 2)
            kb = round(len(resp.content) / 1024, 1)
            factory_log("INGEST", f"200 OK  {len(products)} products ({kb} KB) in {dur}s")

            # Deterministic bulk ingestion & facet extraction in single pass
            for p in products:
                handle = p.get("handle", "")
                url = f"{base_url}/products/{handle}"
                title = p.get("title", "")
                vendor = p.get("vendor", "")
                desc = p.get("body_html", "") or ""
                raw_hash = hashlib.sha256(json.dumps(p, sort_keys=True).encode("utf-8")).hexdigest()

                # In-memory FacetBag extraction
                facet_bag = extract_facetbag_from_raw(title, desc, vendor)

                source_products_col.update_one(
                    {"source_url": url},
                    {
                        "$set": {
                            "source_id": source_id,
                            "source_url": url,
                            "handle": handle,
                            "title": title,
                            "vendor": vendor,
                            "product_type": p.get("product_type", ""),
                            "tags": p.get("tags", []),
                            "facet_bag": facet_bag,
                            "raw": p,
                            "raw_hash": raw_hash,
                            "ingested_at": now.isoformat(),
                            "status": "ingested"
                        }
                    },
                    upsert=True
                )

                discovered_urls_col.update_one(
                    {"_id": url},
                    {
                        "$set": {
                            "source_id": source_id,
                            "url": url,
                            "last_seen": now.isoformat(),
                            "status": "active"
                        }
                    },
                    upsert=True
                )

                # Route downstream to LLM Klass Classifier
                infer_klass.delay(url)
                total_ingested += 1

            page += 1
            time.sleep(0.05)

        except Exception as e:
            factory_log("ERROR", f"Error syncing page {page}: {e}")
            break

    factory_log("DONE", f"High-speed sync complete: {total_ingested:,} products ingested & faceted\n")
    return {"source_id": source_id, "total_ingested": total_ingested}

@app.task(name="tasks.check_sitemap", bind=True, max_retries=3, default_retry_delay=60)
def check_sitemap(self, source_id: str):
    db = get_mongo_db()
    sitemaps_col = db["sitemaps"]
    discovered_urls_col = db["discovered_urls"]

    source_doc = sitemaps_col.find_one({"_id": source_id})
    if not source_doc:
        factory_log("ERROR", f"Sitemap source '{source_id}' not found in database")
        return {"error": f"Source {source_id} not found"}

    sitemap_url = source_doc.get("sitemap_url")
    now = datetime.datetime.now(datetime.timezone.utc)

    factory_log("SITEMAP", f"checking {source_id} sitemap: {sitemap_url}...")

    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        "Accept": "application/xml, text/xml, */*"
    }
    try:
        resp = requests.get(sitemap_url, headers=headers, timeout=30)
        resp.raise_for_status()
    except Exception as e:
        factory_log("ERROR", f"Sitemap request failed: {e}")
        sitemaps_col.update_one({"_id": source_id}, {"$set": {"last_error": str(e), "last_checked_at": now.isoformat()}})
        raise self.retry(exc=e)

    root = parse_xml_elements(resp.text)
    sub_sitemaps = []
    product_urls = []

    if root.tag.endswith("sitemapindex"):
        for sm in root.findall(".//sitemap"):
            loc = sm.find("loc")
            if loc is not None and loc.text:
                sub_sitemaps.append(loc.text.strip())
    else:
        sub_sitemaps.append(sitemap_url)

    for sm_url in sub_sitemaps:
        if "products" not in sm_url.lower() and len(sub_sitemaps) > 1 and "sitemap" in sm_url.lower():
            has_product_sub = any("product" in s.lower() for s in sub_sitemaps)
            if has_product_sub and "product" not in sm_url.lower():
                continue

        try:
            sub_resp = requests.get(sm_url, headers=headers, timeout=30)
            sub_resp.raise_for_status()
            sub_root = parse_xml_elements(sub_resp.text)
            for url_elem in sub_root.findall(".//url"):
                loc = url_elem.find("loc")
                lastmod = url_elem.find("lastmod")
                if loc is not None and loc.text:
                    url_text = loc.text.strip()
                    lastmod_text = lastmod.text.strip() if lastmod is not None and lastmod.text else None
                    if "/products/" in url_text:
                        product_urls.append((url_text, lastmod_text))
        except Exception:
            pass

    new_enqueued = 0
    updated_enqueued = 0
    unchanged_touched = 0

    for url, lastmod in product_urls:
        existing = discovered_urls_col.find_one({"_id": url})
        if not existing:
            discovered_urls_col.update_one(
                {"_id": url},
                {
                    "$set": {
                        "source_id": source_id,
                        "url": url,
                        "first_seen": now.isoformat(),
                        "last_seen": now.isoformat(),
                        "sitemap_lastmod": lastmod,
                        "status": "active"
                    }
                },
                upsert=True
            )
            ingest_product_url.delay(source_id, url)
            new_enqueued += 1
        else:
            if lastmod and existing.get("sitemap_lastmod") != lastmod:
                discovered_urls_col.update_one(
                    {"_id": url},
                    {
                        "$set": {
                            "last_seen": now.isoformat(),
                            "sitemap_lastmod": lastmod,
                            "status": "active"
                        }
                    }
                )
                ingest_product_url.delay(source_id, url)
                updated_enqueued += 1
            else:
                discovered_urls_col.update_one(
                    {"_id": url},
                    {"$set": {"last_seen": now.isoformat(), "status": "active"}}
                )
                unchanged_touched += 1

    factory_log("SITEMAP", f"found {len(product_urls):,} URLs ({new_enqueued:,} new / {updated_enqueued:,} changed / {unchanged_touched:,} unchanged)")
    if new_enqueued + updated_enqueued > 0:
        factory_log("QUEUE", f"-> ingest_q +{new_enqueued + updated_enqueued:,}")

    sitemaps_col.update_one(
        {"_id": source_id},
        {
            "$set": {
                "last_checked_at": now.isoformat(),
                "last_success_at": now.isoformat(),
                "total_urls_discovered": len(product_urls),
                "last_run_new_urls": new_enqueued,
                "last_run_updated_urls": updated_enqueued,
                "status": "active"
            }
        }
    )

    return {
        "source_id": source_id,
        "total_discovered": len(product_urls),
        "new_enqueued": new_enqueued,
        "updated_enqueued": updated_enqueued,
        "unchanged_touched": unchanged_touched
    }

# ==============================================================================
# STATION 2: SINGLE-PASS INGESTION & FACET EXTRACTION (ingest_q)
# ==============================================================================
@app.task(name="tasks.ingest_product_url", bind=True, max_retries=3, default_retry_delay=5)
def ingest_product_url(self, source_id: str, url: str):
    import random
    import time

    db = get_mongo_db()
    source_products_col = db["source_products"]
    now = datetime.datetime.now(datetime.timezone.utc)

    path_snippet = url.replace("https://emedicalkits.com", "").replace("http://emedicalkits.com", "")
    factory_log("INGEST", f"GET {path_snippet}")

    json_url = f"{url.rstrip('/')}.js"
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        "Accept": "application/json, text/html, */*"
    }

    raw_payload = None

    for attempt in range(3):
        try:
            resp = requests.get(json_url, headers=headers, timeout=15)
            if resp.status_code == 429:
                sleep_sec = 1.5 + (attempt * 1.5) + random.uniform(0.2, 0.8)
                factory_log("ERROR", f"429 Rate limited on {path_snippet}. Pausing {sleep_sec:.1f}s...")
                time.sleep(sleep_sec)
                continue
            elif resp.status_code == 200:
                raw_payload = resp.json()
                payload_kb = round(len(resp.content) / 1024, 1)
                factory_log("INGEST", f"200 OK  {payload_kb} KB")
                break
            elif resp.status_code == 404:
                factory_log("ERROR", f"404 Not Found on {path_snippet}")
                return {"url": url, "status": "404_not_found"}
            else:
                resp_html = requests.get(url, headers=headers, timeout=15)
                if resp_html.status_code == 200:
                    raw_payload = {"html": resp_html.text, "url": url}
                    payload_kb = round(len(resp_html.content) / 1024, 1)
                    factory_log("INGEST", f"200 OK (HTML)  {payload_kb} KB")
                    break
        except Exception as e:
            if attempt < 2:
                time.sleep(1.0)
            else:
                factory_log("ERROR", f"Failed ingest on {path_snippet}: {e}")
                return {"url": url, "status": "failed", "error": str(e)}

    if not raw_payload:
        factory_log("ERROR", f"Empty payload for {path_snippet}")
        return {"url": url, "status": "empty_payload"}

    time.sleep(0.1)

    raw_hash = hashlib.sha256(json.dumps(raw_payload, sort_keys=True).encode("utf-8")).hexdigest()

    title = raw_payload.get("title", "") if isinstance(raw_payload, dict) else ""
    vendor = raw_payload.get("vendor", "") if isinstance(raw_payload, dict) else ""
    desc = str(raw_payload.get("description") or raw_payload.get("body_html") or "") if isinstance(raw_payload, dict) else ""
    ptype = raw_payload.get("type", "") if isinstance(raw_payload, dict) else ""
    tags = raw_payload.get("tags", []) if isinstance(raw_payload, dict) else []
    handle = raw_payload.get("handle", "") if isinstance(raw_payload, dict) else ""

    # In-memory single-pass FacetBag extraction
    facet_bag = extract_facetbag_from_raw(title, desc, vendor)

    update_fields = {
        "source_id": source_id,
        "source_url": url,
        "handle": handle,
        "title": title,
        "vendor": vendor,
        "product_type": ptype,
        "tags": tags,
        "facet_bag": facet_bag,
        "raw": raw_payload,
        "raw_hash": raw_hash,
        "ingested_at": now.isoformat(),
        "status": "ingested"
    }

    source_products_col.update_one(
        {"source_url": url},
        {"$set": update_fields},
        upsert=True
    )

    factory_log("INGEST", f"saved \"{title}\" (SHA: {raw_hash[:8]})")
    factory_log("QUEUE", f"-> klass_q url={path_snippet}")

    infer_klass.delay(url)
    return {"url": url, "title": title, "status": "ingested"}

# ==============================================================================
# STATION 3: LLM KLASS CLASSIFIER WORKER (klass_q)
# ==============================================================================
@app.task(name="tasks.infer_klass", bind=True, max_retries=2, default_retry_delay=15)
def infer_klass(self, url: str):
    db = get_mongo_db()
    source_products_col = db["source_products"]
    klass_metadata_col = db["klass_metadata"]

    product = source_products_col.find_one({"source_url": url})
    if not product:
        factory_log("ERROR", f"Product not found for {url}")
        return {"error": "Product not found"}

    title = product.get("title", "")
    ptype = product.get("product_type", "")
    vendor = product.get("vendor", "")
    tags = product.get("tags", [])
    raw = product.get("raw", {})
    body = raw.get("body_html", "") or ""
    clean_body = re.sub(r'<[^>]+>', ' ', body).strip()
    clean_body = re.sub(r'\s+', ' ', clean_body)[:300]
    
    inferred_klass = None

    # ==========================================================================
    # STATION 3: LLM TAXONOMIC CLASSIFICATION (Granite 4.1:8B + Full Shopify Context)
    # ==========================================================================
    system_prompt = """You are an expert healthcare product taxonomy classifier.
Your task is to classify medical and clinical products into a clean, standardized canonical product category (1 to 3 words, singular snake_case).

CRITICAL TAXONOMY RULES:
1. Identify the core noun of the physical object or substance (e.g. activated_charcoal, exam_table, exam_glove, hypodermic_needle, ultrasound_gel).
2. DO NOT append functional marketing phrases (remove: "poison absorbent", "skin protectant", "odor eliminator", "pain relief", "moisture barrier"). Output the core substance/item (e.g. "activated_charcoal", "petroleum_jelly", "barrier_cream").
3. DO NOT output single generic words without qualification (NEVER output: gel, paper, tray, bag, pump, lock, rack, device, system, pad, part).
4. DO NOT output 5-word attribute descriptions (NEVER include: sterile, non-sterile, luer_slip, reusable, 4x4, 100_pack, blue, extra_large).
5. Use the Shopify Category hierarchy, Vendor, and Product Description to ground the true product type.
6. Output ONLY the single snake_case category term.

FEW-SHOT EXAMPLES:
- "Actidose-Aqua Activated Charcoal Poison Absorbent" -> activated_charcoal
- "BD Eclipse Hypodermic Needle with Safety Cover 21G" -> hypodermic_needle
- "3M Tegaderm Transparent Film Dressing 4x4" -> transparent_film_dressing
- "Clinton Industries Power Hi-Lo Bariatric Exam Table with Stirrups" -> exam_table
- "Welch Allyn Green Series 777 Wall Transformer Diagnostic System" -> diagnostic_wall_transformer
- "McKesson Confiderm Nitrile Exam Gloves Powder-Free Large" -> exam_glove"""

    user_prompt = f"""PRODUCT DETAILS:
Title: {title}
Category: {ptype}
Vendor: {vendor}
Tags: {', '.join(tags) if isinstance(tags, list) else tags}
Description: {clean_body}

Canonical Product Category:"""

    try:
        resp = requests.post(
            f"{OLLAMA_HOST}/api/generate",
            json={
                "model": OLLAMA_MODEL,
                "system": system_prompt,
                "prompt": user_prompt,
                "options": {
                    "temperature": 0.0,
                    "num_predict": 15,
                    "num_ctx": 2048
                },
                "keep_alive": "1h",
                "stream": False
            },
            timeout=60
        )
        if resp.status_code == 200:
            raw_out = resp.json().get("response", "").strip()
            slug = raw_out.lower().replace(" ", "_").replace("-", "_")
            slug = slug.split("\n")[0].split(":")[-1].strip(". '\"`")
            slug = re.sub(r'[^a-z0-9_]', '', slug)
            if slug and len(slug) >= 3:
                inferred_klass = slug
    except Exception as e:
        factory_log("ERROR", f"LLM inference failed for {title}: {e}")

    if not inferred_klass:
        # Fallback slug if LLM request failed
        inferred_klass = slugify(title.split()[0]) if title else "unclassified"

    now = datetime.datetime.now(datetime.timezone.utc)
    source_products_col.update_one(
        {"source_url": url},
        {
            "$set": {
                "inferred_klass": inferred_klass,
                "inferred_at": now.isoformat(),
                "status": "done"
            }
        }
    )

    factory_log("KLASS", f"\"{title}\" -> {inferred_klass}")
    factory_log("DONE", f"product={url} complete", blank_after=True)

    # Record discovered Klass metadata without triggering blurb generation
    klass_slug = slugify(inferred_klass)
    if klass_slug not in ["unclassified", "medical-supply"]:
        klass_metadata_col.update_one(
            {"_id": klass_slug},
            {
                "$setOnInsert": {
                    "_id": klass_slug,
                    "klass": inferred_klass,
                    "title": format_title(inferred_klass),
                    "status": "pending",
                    "discovered_at": now.isoformat()
                }
            },
            upsert=True
        )

    return {"url": url, "inferred_klass": inferred_klass, "status": "done"}

# ==============================================================================
# STATION 3B: TAXONOMY RECONCILIATION & RESOLUTION WORKER (klass_reconcile_q)
# ==============================================================================
import difflib

def resolve_canonical_root(db, target_slug: str, max_hops: int = 10) -> str:
    """
    Traverses klass_metadata to ensure aliases always point to the ultimate canonical root.
    Prevents chains (A -> B -> C becomes A -> C) and circular references.
    """
    curr = target_slug
    visited = {curr}
    for _ in range(max_hops):
        doc = db["klass_metadata"].find_one({"_id": curr})
        if not doc or doc.get("status") != "alias":
            break
        canonical = doc.get("canonical_klass")
        if not canonical or canonical in visited:
            break
        visited.add(canonical)
        curr = canonical
    return curr

_candidate_index_cache = None
_candidate_index_time = 0

def get_reconcile_candidate_index(db, max_age_secs: int = 60):
    global _candidate_index_cache, _candidate_index_time
    now = time.time()
    if _candidate_index_cache is None or (now - _candidate_index_time) > max_age_secs:
        pipeline = [
            {"$match": {"inferred_klass": {"$nin": [None, ""]}}},
            {"$group": {"_id": "$inferred_klass", "count": {"$sum": 1}}},
            {"$sort": {"count": -1}}
        ]
        raw_counts = list(db["source_products"].aggregate(pipeline))
        metadata_map = {doc["_id"]: doc for doc in db["klass_metadata"].find({}, {"_id": 1, "status": 1, "canonical_klass": 1})}
        
        candidates = []
        for item in raw_counts:
            slug = item["_id"]
            count = item["count"]
            meta = metadata_map.get(slug, {})
            if meta.get("status") == "alias":
                continue
            tokens = slug.replace("-", "_").split("_")
            head_noun = tokens[-1] if tokens else ""
            candidates.append({
                "slug": slug,
                "count": count,
                "tokens": set(tokens),
                "head_noun": head_noun,
                "status": meta.get("status", "canonical" if count >= 10 else "provisional")
            })
        _candidate_index_cache = candidates
        _candidate_index_time = now
    return _candidate_index_cache

def find_nearest_candidates(proposed_slug: str, candidate_index: list, top_n: int = 6, min_score: float = 0.40) -> list:
    prop_clean = proposed_slug.replace("-", "_")
    prop_tokens = set(prop_clean.split("_"))
    prop_head = prop_clean.split("_")[-1]
    
    scored = []
    for cand in candidate_index:
        cand_slug = cand["slug"]
        if cand_slug == prop_clean:
            continue
            
        cand_tokens = cand["tokens"]
        cand_head = cand["head_noun"]
        overlap = len(prop_tokens & cand_tokens)
        
        jaccard = overlap / len(prop_tokens | cand_tokens) if (prop_tokens | cand_tokens) else 0
        sub_bonus = 0.4 if (cand_slug in prop_clean or prop_clean in cand_slug) else 0
        seq_ratio = difflib.SequenceMatcher(None, prop_clean, cand_slug).ratio()
        head_bonus = 0.25 if (prop_head == cand_head or prop_head.rstrip('s') == cand_head.rstrip('s')) else 0.0
        
        import math
        vol_score = math.log10(cand["count"] + 1) * 0.04
        total_score = (jaccard * 0.35) + sub_bonus + (seq_ratio * 0.30) + head_bonus + vol_score
        
        if total_score >= min_score:
            scored.append((total_score, cand))
            
    scored.sort(key=lambda x: x[0], reverse=True)
    return [item[1] for item in scored[:top_n]]

@app.task(name="tasks.reconcile_klass", bind=True, max_retries=2, default_retry_delay=30)
def reconcile_klass(self, proposed_klass_slug: str):
    db = get_mongo_db()
    source_products_col = db["source_products"]
    klass_metadata_col = db["klass_metadata"]
    
    proposed_clean = proposed_klass_slug.replace("-", "_").strip()
    
    # 1. Gather product count & rich samples for this proposed klass
    matching_docs = list(source_products_col.find(
        {"$or": [{"inferred_klass": proposed_clean}, {"inferred_klass": proposed_klass_slug}, {"proposed_klass": proposed_clean}]},
        {"_id": 1, "title": 1, "product_type": 1, "tags": 1, "facet_bag": 1, "raw.body_html": 1, "proposed_klass": 1}
    ))
    total_count = len(matching_docs)
    
    sample_descriptors = []
    for d in matching_docs[:3]:
        t = d.get("title", "")
        if not t:
            continue
        ptype = d.get("product_type", "")
        tags = d.get("tags", [])
        tags_str = ", ".join(tags[:4]) if isinstance(tags, list) else str(tags)
        
        fb = d.get("facet_bag", {})
        key_facets = []
        for k in ["Material", "Grade", "Safety Rating", "Absorbency", "Sterility", "Cuff", "Vendor"]:
            if k in fb:
                val = fb[k]
                if isinstance(val, list):
                    val = ", ".join(val)
                key_facets.append(f"{k}: {val}")
        
        meta_parts = []
        if ptype:
            meta_parts.append(f"Shopify Type: {ptype}")
        if tags_str:
            meta_parts.append(f"Tags: [{tags_str}]")
        if key_facets:
            meta_parts.append(f"Specs: {', '.join(key_facets)}")
            
        meta_suffix = f" ({' | '.join(meta_parts)})" if meta_parts else ""
        sample_descriptors.append(f"{t}{meta_suffix}")

    first_title = matching_docs[0].get("title", proposed_clean) if matching_docs else proposed_clean
    sample_str = f"\"{first_title}\""
    
    factory_log("RECONCILE", f"{proposed_clean} [{total_count}]")
    if sample_descriptors:
        factory_log("RECONCILE", f"samples: {sample_str}")
        
    cand_index = get_reconcile_candidate_index(db)
    candidates = find_nearest_candidates(proposed_clean, cand_index, top_n=5, min_score=0.40)
    
    decision = "KEEP"
    target_canonical = proposed_clean
    
    if not candidates:
        factory_log("RECONCILE", f"nearest candidates weak -> KEEP")
    else:
        cand_summary = ", ".join([f"{c['slug']} [{c['count']}]" for c in candidates[:3]])
        factory_log("RECONCILE", f"candidates: {cand_summary}")
        
        cand_lines = "\n".join([f"  • {c['slug']} ({c['count']} products)" for c in candidates])
        valid_slugs = set(c['slug'] for c in candidates)
        
        prompt = f"""You are an expert clinical taxonomy resolver for an industrial healthcare catalog.

PROPOSED CATEGORY TO EVALUATE:
"{proposed_clean}"

SAMPLE PRODUCTS (WITH SHOPIFY METADATA & SPECS):
{chr(10).join(f"- {s}" for s in sample_descriptors)}

CANDIDATE CATEGORIES:
{cand_lines}

TASK:
Determine if "{proposed_clean}" is a synonym, sub-variant, or specific wording of ONE of the candidate categories above, OR if it is a GENUINELY DISTINCT category that must remain separate.

CRITICAL RULES:
1. MERGE only if the item is genuinely the same clinical product type or a direct sub-type (e.g. "powered_exam_table" -> "exam_table", "nitrile_examination_glove" -> "exam_glove").
2. If NONE of the candidates match the true nature of this item (e.g. an "incubator" is NOT a "cart" or "table", a "microscope" is NOT a "scale"), you MUST output: KEEP.
3. Output EXACTLY one line: "MERGE <candidate_slug>" (using a slug from the candidate list) OR "KEEP".
"""
        try:
            resp = requests.post(
                f"{OLLAMA_HOST}/api/generate",
                json={
                    "model": OLLAMA_MODEL,
                    "prompt": prompt,
                    "stream": False,
                    "options": {"temperature": 0.0, "num_predict": 30}
                },
                timeout=30
            )
            raw_out = resp.json().get("response", "").strip()
            
            match = re.search(r'\bMERGE\s+([a-zA-Z0-9_\-]+)', raw_out, re.IGNORECASE)
            if match:
                chosen_slug = match.group(1).lower().replace("-", "_")
                if chosen_slug in valid_slugs:
                    target_canonical = resolve_canonical_root(db, chosen_slug)
                    decision = f"MERGE {target_canonical}"
                else:
                    decision = "KEEP"
                    target_canonical = proposed_clean
            elif "KEEP" in raw_out.upper():
                decision = "KEEP"
                target_canonical = proposed_clean
        except Exception as e:
            factory_log("ERROR", f"Resolution LLM error for {proposed_clean}: {e}")
            decision = "KEEP"
            target_canonical = proposed_clean

    factory_log("RECONCILE", f"Granite -> {decision}")
    
    now = datetime.datetime.now(datetime.timezone.utc).isoformat()
    
    # Update klass_metadata authority
    if target_canonical != proposed_clean:
        klass_metadata_col.update_one(
            {"_id": proposed_clean},
            {
                "$set": {
                    "canonical_klass": target_canonical,
                    "status": "alias",
                    "product_count_at_decision": total_count,
                    "resolved_at": now,
                    "model": OLLAMA_MODEL,
                    "resolver_version": 1
                }
            },
            upsert=True
        )
        klass_metadata_col.update_one(
            {"_id": target_canonical},
            {"$set": {"canonical_klass": target_canonical, "status": "canonical"}},
            upsert=True
        )
        factory_log("RECONCILE", f"{proposed_clean} -> {target_canonical}")
    else:
        klass_metadata_col.update_one(
            {"_id": proposed_clean},
            {
                "$set": {
                    "canonical_klass": proposed_clean,
                    "status": "canonical",
                    "resolved_at": now,
                    "model": OLLAMA_MODEL,
                    "resolver_version": 1
                }
            },
            upsert=True
        )
        
    # Update matching source_products: preserve proposed_klass, set canonical_klass & inferred_klass
    if matching_docs:
        source_products_col.update_many(
            {"_id": {"$in": [d["_id"] for d in matching_docs]}},
            {
                "$set": {
                    "canonical_klass": target_canonical,
                    "inferred_klass": target_canonical
                }
            }
        )
        # Ensure proposed_klass is recorded if it wasn't there before
        source_products_col.update_many(
            {"_id": {"$in": [d["_id"] for d in matching_docs]}, "proposed_klass": {"$exists": False}},
            {"$set": {"proposed_klass": proposed_clean}}
        )
        
    factory_log("RECONCILE", f"updated {len(matching_docs)} product(s)")
    factory_log("DONE", f"canonical={target_canonical}", blank_after=True)
    
    return {"proposed": proposed_clean, "canonical": target_canonical, "decision": decision, "updated": len(matching_docs)}

# ==============================================================================
# STATION 4: LLM KLASS BLURB SYNTHESIZER WORKER (klass_blurb_q)
# ==============================================================================
@app.task(name="tasks.generate_klass_blurb", bind=True, max_retries=2, default_retry_delay=30)
def generate_klass_blurb(self, klass_slug: str):
    db = get_mongo_db()
    klass_metadata_col = db["klass_metadata"]

    klass_snake = klass_slug.replace("-", "_")
    klass_title = format_title(klass_snake)
    plural_name = to_plural_name(klass_title)
    now = datetime.datetime.now(datetime.timezone.utc)

    factory_log("BLURB", f"[{OLLAMA_MODEL}] synthesizing overview for \"{plural_name}\"...")

    # Use the engine's dynamic facet extraction across all available products
    from web.server import engine
    klass_data = engine.get_klass_facetbag(klass_slug)
    EXCLUDED_PROMPT_GROUPS = {"vendor", "brand", "manufacturer", "distributor", "size", "color", "quantity", "pack", "count", "weight", "volume", "dimension"}
    variant_axes = {}
    if klass_data and "facet_groups" in klass_data:
        for g in klass_data["facet_groups"]:
            label = g.get("label") or g.get("key")
            if label.lower() in EXCLUDED_PROMPT_GROUPS:
                continue
            vals = [v.get("value") for v in g.get("values", [])[:5] if v.get("value")]
            if vals:
                variant_axes[label] = vals

    dim_keys = list(variant_axes.keys())
    if not dim_keys or klass_slug in ["unclassified", "medical-supply"]:
        factory_log("BLURB", f"skipped \"{klass_slug}\" (0 dimensions / generic supply)")
        return {"klass_slug": klass_slug, "status": "skipped", "reason": "0_dimensions"}

    factory_log("BLURB", f"context: {len(dim_keys)} dimensions ({', '.join(dim_keys[:5])})")

    system_prompt = """You are an encyclopedia editor writing a crisp, clear, Wikipedia-style overview of a medical product Klass.

Guidelines:
- Write in clean, concise, authoritative English. Keep sentences punchy (under 20 words) and varied.
- NEVER write run-on sentences with endless "and... and... with...".
- NEVER use parenthetical lists like "(small, medium, large)" or "(blue, pink, green)".
- Avoid dense latin medical jargon (e.g. use "allows the wound to naturally clear dead tissue while preventing skin breakdown" instead of "autolytic debridement" or "maceration").
- Do not mention specific store vendors, brand names, or distributors. Focus on practical clinical purpose, primary barrier materials, and patient protection.
- Target exactly 2–3 crisp sentences. Output raw paragraph text directly with zero introductory remarks."""

    user_prompt = f"""Product Class: {plural_name}
Discovered Variant Attributes:
{json.dumps(variant_axes, indent=2)}

Write the 2–3 crisp sentence overview for {plural_name}:"""

    blurb_content = ""
    t0 = time.time()
    try:
        payload = {
            "model": OLLAMA_MODEL,
            "system": system_prompt,
            "prompt": user_prompt,
            "options": {
                "temperature": 0.2
            },
            "keep_alive": "1h",
            "stream": False
        }
        resp = requests.post(f"{OLLAMA_HOST}/api/generate", json=payload, timeout=90)
        dur = round(time.time() - t0, 1)
        if resp.status_code == 200:
            raw_response = resp.json().get("response", "").strip()
            blurb_content = re.sub(r'^(Here is|Here are|Here\'s|Below is).*?:\s*', '', raw_response, flags=re.IGNORECASE).strip()
            factory_log("BLURB", f"-> \"{blurb_content}\" ({dur}s)")
        else:
            factory_log("ERROR", f"Ollama HTTP error {resp.status_code}: {resp.text}")
    except Exception as e:
        factory_log("ERROR", f"Ollama request exception: {e}")

    if not blurb_content:
        blurb_content = f"{plural_name.capitalize()} are protective and functional clinical products characterized by standardized specifications across healthcare settings."

    summary_content = f"Discovered specifications and variant attributes for {klass_title.lower()}."

    klass_metadata_col.update_one(
        {"_id": klass_slug},
        {
            "$set": {
                "_id": klass_slug,
                "klass": klass_snake,
                "title": klass_title,
                "blurb": blurb_content,
                "summary": summary_content,
                "status": "ready",
                "generated_at": now.isoformat()
            }
        },
        upsert=True
    )

    factory_log("BLURB", f"saved klass_metadata for \"{klass_slug}\"")

    # Cascade: Automatically enqueue clinical facet values for this Klass into facet_blurb_q!
    has_cascaded = False
    try:
        from web.server import engine
        EXCLUDED_KEYS = {"color", "size", "vendor", "quantity", "pack", "count", "weight", "volume", "dimension"}
        data = engine.get_klass_facetbag(klass_slug)
        enqueued_facets = 0
        for g in data.get("facet_groups", [])[:4]:
            key = g.get("label") or g.get("key")
            if key.lower() in EXCLUDED_KEYS:
                continue
            for v in g.get("values", [])[:6]:
                val = v.get("value")
                cnt = v.get("count", 0)
                pct = v.get("percentage", 0)
                if val and (cnt >= 3 or pct >= 5.0):
                    generate_facet_value_blurb.delay(klass_slug, key, str(val))
                    enqueued_facets += 1
        if enqueued_facets > 0:
            has_cascaded = True
            factory_log("BLURB", f"-> cascaded {enqueued_facets} clinical facet tasks to facet_blurb_q for \"{klass_slug}\"", blank_after=True)
    except Exception as e:
        factory_log("ERROR", f"Failed cascading facet blurbs for {klass_slug}: {e}")

    if not has_cascaded:
        factory_log("BLURB", f"done with {klass_slug}", blank_after=True)

    return {"klass_slug": klass_slug, "status": "ready", "blurb": blurb_content}

# ==============================================================================
# STATION 5: CONTEXTUAL FACET-VALUE BLURB SYNTHESIZER (facet_blurb_q)
# ==============================================================================
@app.task(name="tasks.generate_facet_value_blurb", bind=True, max_retries=2, default_retry_delay=15)
def generate_facet_value_blurb(self, klass_slug: str, facet_key: str, facet_val: str):
    db = get_mongo_db()
    klass_metadata_col = db["klass_metadata"]

    klass_snake = klass_slug.replace("-", "_")
    klass_title = format_title(klass_snake)
    now = datetime.datetime.now(datetime.timezone.utc)

    meta = klass_metadata_col.find_one({"_id": klass_slug})
    if not meta or not meta.get("blurb"):
        factory_log("FACET", f"skipped {klass_slug} (no Klass blurb present yet)", blank_after=True)
        return {"klass_slug": klass_slug, "status": "skipped", "reason": "no_parent_blurb"}

    klass_blurb = meta.get("blurb", "")
    factory_log("FACET", f"[{klass_title}] {facet_key}: \"{facet_val}\"...")

    system_prompt = """You are a technical medical product specialist. Write a concise, direct, 1-2 sentence explanation of the specific facet value in the context of the product Klass.
Rules:
1. Start directly with the subject (e.g. "Nitrile provides...", "Latex offers...", "Silicone is...", "Sterile options are...").
2. Write in plain, clear, accessible English. Avoid dense latin medical jargon (e.g. avoid unexplained terms like "autolytic debridement" or "maceration").
3. DO NOT use conversational fluff, robotic preamble, or phrases like "A healthcare clinician selects" or "Clinicians choose".
4. Focus directly on physical properties, barrier protection, patient comfort, skin sensitivity, and practical clinical advantages.
5. Output ONLY the raw 1-2 sentences with zero preamble."""

    user_prompt = f"""Product Class: {klass_title}
Class Overview: {klass_blurb if klass_blurb else klass_title}
Facet Dimension: {facet_key}
Specific Value: {facet_val}

Clinical Guidance (1-2 sentences):"""

    t0 = time.time()
    value_blurb = ""
    try:
        resp = requests.post(
            f"{OLLAMA_HOST}/api/generate",
            json={
                "model": OLLAMA_MODEL,
                "system": system_prompt,
                "prompt": user_prompt,
                "options": {
                    "temperature": 0.1,
                    "num_predict": 75,
                    "num_ctx": 2048
                },
                "keep_alive": "1h",
                "stream": False
            },
            timeout=30
        )
        dur = round(time.time() - t0, 1)
        if resp.status_code == 200:
            raw = resp.json().get("response", "").strip()
            value_blurb = re.sub(r'^(Here is|Here are|Here\'s|Below is).*?:\s*', '', raw, flags=re.IGNORECASE).strip()
            factory_log("FACET", f"-> \"{value_blurb}\" ({dur}s)", blank_after=True)
        else:
            factory_log("ERROR", f"Ollama HTTP {resp.status_code}", blank_after=True)
    except Exception as e:
        factory_log("ERROR", f"Facet blurb failed for {klass_slug} {facet_key}:{facet_val}: {e}", blank_after=True)

    if not value_blurb:
        value_blurb = f"Indicated for standard {klass_title.lower()} applications requiring {facet_val.lower()} specifications."

    facet_field = f"facet_value_blurbs.{facet_key}:{facet_val}"
    klass_metadata_col.update_one(
        {"_id": klass_slug},
        {
            "$set": {
                facet_field: value_blurb,
                "updated_at": now.isoformat()
            }
        },
        upsert=True
    )

    return {"klass_slug": klass_slug, "facet": f"{facet_key}:{facet_val}", "blurb": value_blurb}
