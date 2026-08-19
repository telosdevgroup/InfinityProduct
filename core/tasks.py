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

    for m in ["nitrile", "vinyl", "latex", "silicone", "foam", "cotton", "gauze", "silver", "alginate", "polyurethane", "stainless steel", "polychloroprene", "rubber"]:
        if re.search(r"\b" + re.escape(m) + r"\b", full_text):
            facet_bag.setdefault("Material", []).append(m.title())

    for sz in ["X-Small", "Small", "Medium", "Large", "Extra Large", "2XL", "Pediatric", "Adult"]:
        if re.search(r"\b" + re.escape(sz.lower()) + r"\b", full_text):
            facet_bag["Size"] = sz
            break

    for c in ["blue", "pink", "black", "green", "clear", "white", "purple", "yellow"]:
        if re.search(r"\b" + re.escape(c) + r"\b", full_text):
            facet_bag["Color"] = c.title()
            break

    if "non-sterile" in full_text or "nonsterile" in full_text or "non sterile" in full_text:
        facet_bag["Sterility"] = "Non-Sterile"
    elif "sterile" in full_text:
        facet_bag["Sterility"] = "Sterile"

    for cuff in ["beaded cuff", "extended cuff", "standard cuff", "adhesive border"]:
        if cuff in full_text:
            facet_bag["Cuff"] = cuff.title()
            break

    for tex in ["textured fingertips", "textured fingers", "fully textured", "micro-textured", "smooth"]:
        if tex in full_text:
            facet_bag["Texture"] = "Textured Fingertips" if "textured finger" in tex else tex.title()
            break

    if vendor:
        facet_bag["Vendor"] = vendor

    return facet_bag

# ==============================================================================
# STATION 1: SITEMAP & HIGH-SPEED FEED DISCOVERY WORKER (check_sitemap_q)
# ==============================================================================
@app.task(name="tasks.sync_source_catalog", bind=True)
def sync_source_catalog(self, source_id: str = "emedicalkits"):
    """
    High-speed deterministic catalog ingestion:
    Paginates /products.json?limit=250 to ingest all products and extract facets in seconds.
    """
    db = get_mongo_db()
    source_products_col = db["source_products"]
    discovered_urls_col = db["discovered_urls"]
    sitemaps_col = db["sitemaps"]
    now = datetime.datetime.now(datetime.timezone.utc)

    factory_log("SITEMAP", f"Starting high-speed catalog sync for {source_id}...")

    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        "Accept": "application/json"
    }

    base_url = "https://emedicalkits.com"
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
    title_lower = title.lower()
    inferred_klass = None

    # ==========================================================================
    # STATION 3: LLM TAXONOMIC CLASSIFICATION (Granite 4.1:8B)
    # ==========================================================================
    vendor = product.get("vendor", "")
    system_prompt = """You are a medical device taxonomist. Classify the product into a generic canonical product class (singular snake_case).
Rules:
1. Remove all brand names, manufacturers, and trademarks (e.g. McKesson, BD, Stifneck, DuoDerm, SharpSafety, Bactoshield, 3M, Covidien).
2. Remove all sizes, gauges, dimensions, and colors (e.g. adult, 21G, 18 inch, 4x4, large, blue, 10 quart).
3. Return only the generic device/supply category slug. Examples: stethoscope, sphygmomanometer, cervical_collar, sharps_container, surgical_gown, grab_bar, hypodermic_needle, exam_glove, alginate_dressing, foam_dressing, surgical_scrub.
4. Output ONLY the lowercase snake_case slug, with no extra text or explanation."""

    inferred_klass = None
    try:
        resp = requests.post(
            f"{OLLAMA_HOST}/api/generate",
            json={
                "model": OLLAMA_MODEL,
                "system": system_prompt,
                "prompt": f"Product: {title}\nVendor: {vendor}\n\nCanonical Klass:",
                "options": {
                    "temperature": 0.0,
                    "num_predict": 10,
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

    factory_log("KLASS", f"\"{title}\"")
    factory_log("KLASS", f"-> inferred: {inferred_klass}")
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
    EXCLUDED_PROMPT_GROUPS = {"vendor", "brand", "manufacturer", "distributor"}
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

    system_prompt = """You are an encyclopedia editor writing a short, neutral, Wikipedia-style clinical overview of a medical product Klass using clinical taxonomy evidence.

Guidelines:
- Do not mention specific store vendors, brand names, or distributors. Focus entirely on clinical purpose, medical indications, materials, active agents, and functional mechanisms.
- Do not mechanically enumerate facet groups. The FacetBag is source material, not an outline.
- Describe what the product class is, what it is generally used for, and mention only a few characteristics that are genuinely useful for understanding the class. Vary sentence structure naturally between Klasses.
- Do not use filler phrases such as “are available in various”, “exhibit variability in”, or “primarily acts as”.
- Target roughly 2–4 sentences. Output raw paragraph text directly with zero introductory remarks."""

    user_prompt = f"""Product Class: {plural_name}
Discovered Variant Attributes:
{json.dumps(variant_axes, indent=2)}

Write the 2–4 sentence Wikipedia-style overview for {plural_name}:"""

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

    system_prompt = """You are a technical medical device encyclopedia writer. Write a concise, direct, 1-2 sentence clinical explanation of the specific facet value in the context of the product Klass.
Rules:
1. Start directly with the subject (e.g. "Nitrile provides...", "Latex offers...", "Vinyl is...").
2. DO NOT use conversational fluff, filler, or phrases like "A healthcare clinician selects", "Clinicians choose", "Chosen by clinicians", or "In clinical settings".
3. Focus directly on material properties, barrier performance, tactile sensitivity, allergy considerations, and practical trade-offs.
4. Output ONLY the raw 1-2 sentences with zero preamble."""

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
