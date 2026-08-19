import json
import re
import urllib.parse
from http.server import ThreadingHTTPServer, BaseHTTPRequestHandler
from pymongo import MongoClient
import os
import sys

MONGO_URI = "mongodb://localhost:27017/"
DB_NAME = "infinityproduct_dev"
PORT = 8765

def slugify(text: str) -> str:
    if not text:
        return ""
    text = str(text).lower().replace("_", "-").replace(" ", "-").replace("/", "-")
    return re.sub(r"[^a-z0-9\-]+", "", text).strip("-")

def unslugify_to_snake(slug: str) -> str:
    if not slug:
        return ""
    return str(slug).lower().replace("-", "_")

def format_title(text: str) -> str:
    if not text:
        return ""
    return " ".join(w.capitalize() for w in str(text).replace("-", " ").replace("_", " ").split())

class InfinityProductEngine:
    def __init__(self):
        self.client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=3000)
        self.db = self.client[DB_NAME]

    def get_all_products(self):
        """Returns all products merging source_products and emedicalkits_raw_products."""
        raw_prods = list(self.db["emedicalkits_raw_products"].find({"inferred_klass": {"$exists": True, "$ne": None}}))
        sp_prods = list(self.db["source_products"].find({"inferred_klass": {"$exists": True, "$ne": None}}))
        merged = {}
        for p in raw_prods:
            u = p.get("source_url") or p.get("handle") or str(p["_id"])
            merged[u] = p
        for p in sp_prods:
            u = p.get("source_url") or p.get("handle") or str(p["_id"])
            merged[u] = p
        return list(merged.values())

    def get_stats(self):
        prods = self.get_all_products()
        unique_klasses = len(set(p.get("inferred_klass") for p in prods if p.get("inferred_klass")))
        return {
            "total_products": len(prods),
            "classified_products": len(prods),
            "unique_klasses": unique_klasses
        }

    def get_klasses_list(self, query=""):
        prods = self.get_all_products()
        klass_counts = {}
        for p in prods:
            k = p.get("inferred_klass")
            if k:
                klass_counts[k] = klass_counts.get(k, 0) + 1

        items = []
        for k, count in sorted(klass_counts.items(), key=lambda x: x[1], reverse=True):
            title = format_title(k)
            slug = slugify(k)
            if not query or query.lower() in title.lower() or query.lower() in k.lower():
                items.append({
                    "klass": k,
                    "slug": slug,
                    "title": title,
                    "count": count
                })
        return {"items": items, "total": len(items)}

    def get_klass_facetbag(self, klass_slug: str):
        prods = self.get_all_products()
        target_prods = [
            p for p in prods
            if slugify(p.get("inferred_klass", "")) == klass_slug
        ]

        if not target_prods:
            # Fallback to direct MongoDB query on source_products
            target_prods = list(self.db["source_products"].find({
                "$or": [
                    {"inferred_klass": klass_slug},
                    {"inferred_klass": unslugify_to_snake(klass_slug)}
                ]
            }))

        total_items = len(target_prods)
        canonical_name = target_prods[0].get("inferred_klass") if target_prods else unslugify_to_snake(klass_slug)
        title = format_title(canonical_name)

        # Retrieve blurb, summary, and facet_value_blurbs from klass_metadata
        meta = self.db["klass_metadata"].find_one({"_id": klass_slug})
        blurb_text = meta.get("blurb") if meta else None
        summary_text = meta.get("summary") if meta else None
        facet_value_blurbs = meta.get("facet_value_blurbs", {}) if meta else {}

        # Build dynamic FacetBag from the product properties
        facet_counters = {}
        for p in target_prods:
            fb = p.get("facet_bag") or {}
            for k, v in fb.items():
                if not v:
                    continue
                facet_counters.setdefault(k, {})
                if isinstance(v, list):
                    for item in v:
                        if item:
                            facet_counters[k][str(item)] = facet_counters[k].get(str(item), 0) + 1
                else:
                    facet_counters[k][str(v)] = facet_counters[k].get(str(v), 0) + 1

            # Also check raw options if available
            raw = p.get("raw") or {}
            if isinstance(raw, dict):
                for opt in raw.get("options", []):
                    opt_name = opt.get("name")
                    if opt_name and opt_name.lower() not in ["title", "default title"]:
                        facet_counters.setdefault(opt_name, {})
                        for val in opt.get("values", []):
                            if val:
                                facet_counters[opt_name][str(val)] = facet_counters[opt_name].get(str(val), 0) + 1

        facet_groups = []
        for group_name, val_map in facet_counters.items():
            if not val_map:
                continue
            total_count = sum(val_map.values())
            coverage_pct = round((total_count / max(total_items, 1)) * 100, 1)

            sorted_values = [
                {
                    "value": v,
                    "slug": slugify(v),
                    "count": cnt,
                    "percentage": round((cnt / max(total_items, 1)) * 100, 1),
                    "blurb": facet_value_blurbs.get(f"{group_name}:{v}")
                }
                for v, cnt in sorted(val_map.items(), key=lambda x: x[1], reverse=True)
            ]
            facet_groups.append({
                "key": group_name,
                "slug": slugify(group_name),
                "label": format_title(group_name),
                "total_coverage": total_count,
                "coverage_pct": coverage_pct,
                "values": sorted_values
            })

        facet_groups.sort(key=lambda g: (len(g["values"]) > 1, g["coverage_pct"]), reverse=True)

        return {
            "klass": canonical_name,
            "slug": klass_slug,
            "title": title,
            "total_items": total_items,
            "blurb": blurb_text,
            "summary": summary_text,
            "facet_value_blurbs": facet_value_blurbs,
            "facet_groups": facet_groups
        }

    def get_facet_inversion(self, facet_key_slug: str, facet_value_slug: str):
        prods = self.get_all_products()
        matched_klasses = {}
        total_products = 0

        target_v_slug = slugify(facet_value_slug)
        target_k_slug = slugify(facet_key_slug) if facet_key_slug else None
        exact_value_display = None
        exact_key_display = None

        for p in prods:
            fb = p.get("facet_bag") or {}
            k_name = p.get("inferred_klass")
            if not k_name:
                continue

            hit = False
            for fk, fv in fb.items():
                curr_k_slug = slugify(fk)
                if target_k_slug and curr_k_slug != target_k_slug:
                    continue

                if isinstance(fv, list):
                    for item in fv:
                        if slugify(item) == target_v_slug:
                            hit = True
                            if not exact_value_display:
                                exact_value_display = str(item)
                            if not exact_key_display:
                                exact_key_display = str(fk)
                            break
                else:
                    if slugify(fv) == target_v_slug:
                        hit = True
                        if not exact_value_display:
                            exact_value_display = str(fv)
                        if not exact_key_display:
                            exact_key_display = str(fk)

            if not hit:
                raw = p.get("raw") or {}
                if isinstance(raw, dict):
                    for opt in raw.get("options", []):
                        opt_name = opt.get("name", "")
                        if target_k_slug and slugify(opt_name) != target_k_slug:
                            continue
                        for v in opt.get("values", []):
                            if slugify(v) == target_v_slug:
                                hit = True
                                if not exact_value_display:
                                    exact_value_display = str(v)
                                if not exact_key_display:
                                    exact_key_display = str(opt_name)
                                break

            if hit:
                matched_klasses[k_name] = matched_klasses.get(k_name, 0) + 1
                total_products += 1

        connected_klasses = [
            {
                "klass": k,
                "slug": slugify(k),
                "title": format_title(k),
                "count": cnt
            }
            for k, cnt in sorted(matched_klasses.items(), key=lambda x: x[1], reverse=True)
        ]

        val_title = exact_value_display or format_title(unslugify_to_snake(facet_value_slug))
        key_title = exact_key_display or (format_title(unslugify_to_snake(facet_key_slug)) if facet_key_slug else None)

        return {
            "facet_key": key_title,
            "facet_key_slug": facet_key_slug,
            "facet_value": val_title,
            "facet_value_slug": facet_value_slug,
            "total_products": total_products,
            "connected_klasses_count": len(connected_klasses),
            "klasses": connected_klasses
        }

    def get_matching_products(self, klass_slug: str, facet_key: str = None, facet_val: str = None, limit: int = 50):
        query = {
            "$or": [
                {"inferred_klass": klass_slug},
                {"inferred_klass": unslugify_to_snake(klass_slug)}
            ]
        }
        if facet_key and facet_val:
            query[f"facet_bag.{facet_key}"] = {"$in": [facet_val, facet_val.title(), facet_val.lower()]}

        prods = list(self.db["source_products"].find(
            query,
            {"title": 1, "source_url": 1, "facet_bag": 1, "raw.vendor": 1, "inferred_klass": 1}
        ).limit(limit))

        results = []
        for p in prods:
            results.append({
                "id": str(p["_id"]),
                "title": p.get("title", ""),
                "url": p.get("source_url", ""),
                "vendor": p.get("raw", {}).get("vendor") or p.get("facet_bag", {}).get("Vendor", ""),
                "facet_bag": p.get("facet_bag", {})
            })
        return {
            "klass": klass_slug,
            "facet_key": facet_key,
            "facet_val": facet_val,
            "total": len(results),
            "products": results
        }

engine = InfinityProductEngine()

class ExplorerHandler(BaseHTTPRequestHandler):
    def log_message(self, format, *args):
        # Clean logging
        pass

    def do_GET(self):
        parsed = urllib.parse.urlparse(self.path)
        path = parsed.path
        query = urllib.parse.parse_qs(parsed.query)

        # 1. API routes
        if path == "/api/stats":
            self.respond_json(engine.get_stats())
            return

        if path == "/api/klasses":
            q = query.get("q", [""])[0]
            self.respond_json(engine.get_klasses_list(query=q))
            return

        if path.startswith("/api/klass/") and "/products" in path:
            klass_slug = path.replace("/api/klass/", "").replace("/products", "").strip("/")
            facet_key = query.get("key", [None])[0]
            facet_val = query.get("val", [None])[0]
            self.respond_json(engine.get_matching_products(klass_slug, facet_key, facet_val))
            return

        if path.startswith("/api/klass/"):
            klass_slug = path.replace("/api/klass/", "").strip("/")
            self.respond_json(engine.get_klass_facetbag(klass_slug))
            return

        if path.startswith("/api/facet/"):
            facet_path = path.replace("/api/facet/", "").strip("/")
            parts = facet_path.split("/")
            if len(parts) >= 2:
                key_slug = parts[0]
                val_slug = "/".join(parts[1:])
            else:
                key_slug = None
                val_slug = parts[0]
            self.respond_json(engine.get_facet_inversion(key_slug, val_slug))
            return

        # 2. Web Frontend SPA routing
        if path == "/" or path.startswith("/klass/") or path.startswith("/facet/"):
            self.serve_file("index.html", "text/html")
            return

        self.send_error(404, "Not found")

    def do_POST(self):
        parsed = urllib.parse.urlparse(self.path)
        path = parsed.path

        if path.startswith("/api/klass/") and path.endswith("/generate_blurb"):
            klass_slug = path.replace("/api/klass/", "").replace("/generate_blurb", "").strip("/")
            try:
                from core.tasks import generate_klass_blurb
                generate_klass_blurb.delay(klass_slug)
                self.respond_json({"status": "enqueued", "queue": "klass_blurb_q", "klass": klass_slug})
            except Exception as e:
                self.respond_json({"status": "error", "error": str(e)})
            return

        if path.startswith("/api/klass/") and path.endswith("/generate_facet_blurbs"):
            klass_slug = path.replace("/api/klass/", "").replace("/generate_facet_blurbs", "").strip("/")
            try:
                from core.tasks import generate_facet_value_blurb
                data = engine.get_klass_facetbag(klass_slug)
                if not data.get("blurb"):
                    self.respond_json({
                        "status": "skipped",
                        "reason": "missing_klass_blurb",
                        "message": f"Klass '{klass_slug}' must have a synthesized Klass Blurb before generating contextual facet blurbs."
                    })
                    return

                enqueued = 0
                EXCLUDED_KEYS = {"color", "size", "vendor", "quantity", "pack", "count", "weight", "volume", "dimension"}
                for g in data.get("facet_groups", []):
                    key = g.get("label") or g.get("key")
                    if key.lower() in EXCLUDED_KEYS:
                        continue
                    for v in g.get("values", []):
                        val = v.get("value")
                        cnt = v.get("count", 0)
                        pct = v.get("percentage", 0)
                        # Guardrail: Only meaningful observations (count >= 3 or percentage >= 5%)
                        if val and (cnt >= 3 or pct >= 5.0):
                            generate_facet_value_blurb.delay(klass_slug, key, str(val))
                            enqueued += 1
                self.respond_json({"status": "enqueued", "queue": "facet_blurb_q", "klass": klass_slug, "count": enqueued})
            except Exception as e:
                self.respond_json({"status": "error", "error": str(e)})
            return

        self.send_error(404, "Not found")

    def serve_file(self, filename, content_type):
        base_dir = os.path.dirname(os.path.abspath(__file__))
        candidates = [
            os.path.join(base_dir, filename),
            os.path.join(base_dir, "web", filename),
            os.path.join(os.getcwd(), "web", filename),
            os.path.join(os.getcwd(), filename),
        ]
        filepath = None
        for c in candidates:
            if os.path.exists(c):
                filepath = c
                break

        if not filepath:
            self.send_error(404, f"File not found: {filename}")
            return

        with open(filepath, "rb") as f:
            content = f.read()
        self.send_response(200)
        self.send_header("Content-Type", f"{content_type}; charset=utf-8")
        self.send_header("Content-Length", str(len(content)))
        self.send_header("Access-Control-Allow-Origin", "*")
        self.end_headers()
        self.wfile.write(content)

    def respond_json(self, data):
        body = json.dumps(data, ensure_ascii=False).encode("utf-8")
        self.send_response(200)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Access-Control-Allow-Origin", "*")
        self.end_headers()
        self.wfile.write(body)

def main():
    ThreadingHTTPServer.allow_reuse_address = True
    server = ThreadingHTTPServer(("127.0.0.1", PORT), ExplorerHandler)
    print(f"============================================================", flush=True)
    print(f" INFINITYPRODUCT GRAPH FACET EXPLORER ENGINE RUNNING", flush=True)
    print(f" Local URL: http://127.0.0.1:{PORT}", flush=True)
    print(f"============================================================", flush=True)
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print("\n[*] Shutting down server.", flush=True)
        server.server_close()

if __name__ == "__main__":
    main()
