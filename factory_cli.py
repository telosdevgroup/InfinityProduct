#!/usr/bin/env python3
"""
InfinityProduct Factory Floor -- Live Log Stream Viewer & Command Console
Tails live worker events in real time showing material moving through every station.
"""

import os
import sys
import time
import datetime
import redis
from pymongo import MongoClient

# Ensure current directory is on sys.path
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
if BASE_DIR not in sys.path:
    sys.path.insert(0, BASE_DIR)

from core import celery_app
from core.tasks import check_sitemap, sync_source_catalog, ingest_product_url, infer_klass, generate_klass_blurb, slugify, format_title

MONGO_URI = "mongodb://localhost:27017/"
DB_NAME = "infinityproduct_dev"
LOG_FILE_PATH = os.path.join(BASE_DIR, "factory_stream.log")

# ANSI Colors
CYAN = "\033[96m"
GREEN = "\033[92m"
YELLOW = "\033[93m"
RED = "\033[91m"
MAGENTA = "\033[95m"
BLUE = "\033[94m"
BOLD = "\033[1m"
DIM = "\033[2m"
RESET = "\033[0m"
CLEAR_SCREEN = "\033[2J\033[H"

STATION_COLORS = {
    "SITEMAP": MAGENTA,
    "INGEST": CYAN,
    "KLASS": BLUE,
    "FACET": YELLOW,
    "BLURB": GREEN,
    "QUEUE": DIM,
    "DONE": GREEN + BOLD,
    "ERROR": RED + BOLD,
}

def get_db():
    client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=3000)
    return client[DB_NAME]

def get_redis():
    return redis.Redis(host="localhost", port=6379, db=0, protocol=2)

def colorize_log_line(line: str) -> str:
    """Applies ANSI syntax highlighting to raw log lines."""
    import re
    line = line.rstrip("\r\n")
    if not line:
        return ""

    # Match: [HH:MM:SS] BADGE (N)   Message OR [HH:MM:SS] BADGE   Message
    m = re.match(r'^\[(\d{2}:\d{2}:\d{2})\]\s+([A-Z]+(?:\s+\([\d,]+\))?)\s+(.*)$', line)
    if m:
        ts, full_badge, msg = m.groups()
        base_badge = full_badge.split()[0]
        color = STATION_COLORS.get(base_badge, "")
        formatted_badge = f"{color}{full_badge:<15}{RESET}"
        
        if base_badge == "DONE":
            return f"{DIM}[{ts}]{RESET} {formatted_badge} {GREEN}{msg}{RESET}"
        elif base_badge == "ERROR":
            return f"{DIM}[{ts}]{RESET} {formatted_badge} {RED}{BOLD}{msg}{RESET}"
        elif base_badge == "QUEUE":
            return f"{DIM}[{ts}]{RESET} {formatted_badge} {DIM}{msg}{RESET}"
        else:
            return f"{DIM}[{ts}]{RESET} {formatted_badge} {msg}"

    return line

def tail_factory_stream(max_initial_lines=35):
    """
    Tails factory_stream.log in real time with colored formatting.
    """
    print(CLEAR_SCREEN)
    print(f"{BOLD}{CYAN}==============================================================================={RESET}")
    print(f"{BOLD}{CYAN} 🏭 INFINITYPRODUCT INDUSTRIAL FACTORY FLOOR -- LIVE EVENT STREAM {RESET}")
    print(f"{BOLD}{CYAN}==============================================================================={RESET}")
    print(f" {DIM}Watching live material moving through stations. Press {BOLD}Ctrl+C{RESET}{DIM} for menu.{RESET}")
    print(f"{DIM}-------------------------------------------------------------------------------{RESET}")

    # Read last N lines if file exists
    if os.path.exists(LOG_FILE_PATH):
        try:
            with open(LOG_FILE_PATH, "r", encoding="utf-8", errors="replace") as f:
                lines = f.readlines()
                for l in lines[-max_initial_lines:]:
                    print(colorize_log_line(l))
        except Exception:
            pass
    else:
        print(f" {DIM}Waiting for factory events... (Run Celery worker to start ingestion){RESET}")

    # Active tail loop
    last_size = os.path.getsize(LOG_FILE_PATH) if os.path.exists(LOG_FILE_PATH) else 0

    try:
        while True:
            if os.path.exists(LOG_FILE_PATH):
                curr_size = os.path.getsize(LOG_FILE_PATH)
                if curr_size > last_size:
                    with open(LOG_FILE_PATH, "r", encoding="utf-8", errors="replace") as f:
                        f.seek(last_size)
                        new_content = f.read()
                        last_size = f.tell()
                        for line in new_content.splitlines():
                            if line.strip():
                                print(colorize_log_line(line))
                elif curr_size < last_size:
                    last_size = 0  # file truncated/cleared

            time.sleep(0.2)

    except KeyboardInterrupt:
        print(f"\n{YELLOW}Stream paused. Opening Command Menu...{RESET}")
        time.sleep(0.4)

def action_sync_sitemap():
    print(f"\n{BOLD}{CYAN}[*] Launching High-Speed Catalog Sync (250 items/batch stream)...{RESET}")
    sync_source_catalog.delay("emedicalkits")
    print(f"{GREEN}[OK] Dispatched high-speed sync task to check_sitemap_q.{RESET}")
    time.sleep(1.0)

def action_enqueue_missing():
    db = get_db()
    ingested = set(doc["source_url"] for doc in db["source_products"].find({}, {"source_url": 1}))
    all_disc = list(db["discovered_urls"].find({}, {"source_id": 1, "url": 1}))
    unprocessed = [d for d in all_disc if d["url"] not in ingested]

    print(f"\nDiscovered: {len(all_disc):,} | Ingested: {len(ingested):,} | Remaining: {len(unprocessed):,}")
    if unprocessed:
        count = 0
        for doc in unprocessed:
            ingest_product_url.delay(doc.get("source_id", "emedicalkits"), doc["url"])
            count += 1
        print(f"{GREEN}[OK] Enqueued {count:,} product URLs into ingest_q!{RESET}")
    else:
        print(f"{GREEN}[OK] All discovered products are already ingested.{RESET}")
    time.sleep(1.2)

def action_synthesize_top_blurbs():
    db = get_db()
    min_count = 5
    pipeline = [
        {"$match": {"inferred_klass": {"$nin": [None, "", "unclassified", "medical_supply"]}}},
        {"$group": {"_id": "$inferred_klass", "count": {"$sum": 1}}},
        {"$match": {"count": {"$gte": min_count}}},
        {"$sort": {"count": -1}}
    ]
    top_klasses = list(db["source_products"].aggregate(pipeline))
    print(f"\nFound {len(top_klasses)} Klasses with >= {min_count} products.")
    for item in top_klasses:
        slug = slugify(item["_id"])
        generate_klass_blurb.delay(slug)
        print(f"  -> enqueued Klass Blurb: {slug} ({item['count']} prods)")
    print(f"{GREEN}[OK] Dispatched {len(top_klasses)} Klass Blurb tasks to 'klass_blurb_q'.{RESET}")
    time.sleep(1.5)

def action_synthesize_facet_blurbs():
    db = get_db()
    from core.tasks import generate_facet_value_blurb
    from web.server import engine
    min_count = 5
    pipeline = [
        {"$match": {"inferred_klass": {"$nin": [None, "", "unclassified", "medical_supply"]}}},
        {"$group": {"_id": "$inferred_klass", "count": {"$sum": 1}}},
        {"$match": {"count": {"$gte": min_count}}},
        {"$sort": {"count": -1}}
    ]
    top_klasses = list(db["source_products"].aggregate(pipeline))
    print(f"\nEnqueueing contextual Facet-Value blurbs for top {len(top_klasses)} Klasses...")
    total_enqueued = 0
    for item in top_klasses:
        slug = slugify(item["_id"])
        data = engine.get_klass_facetbag(slug)
        for g in data.get("facet_groups", [])[:4]:
            key = g.get("label") or g.get("key")
            for v in g.get("values", [])[:6]:
                val = v.get("value")
                if val:
                    generate_facet_value_blurb.delay(slug, key, str(val))
                    total_enqueued += 1
    print(f"{GREEN}[OK] Dispatched {total_enqueued} Facet-Value blurb tasks to 'facet_blurb_q'.{RESET}")
    time.sleep(1.5)

def action_clear_log():
    try:
        with open(LOG_FILE_PATH, "w", encoding="utf-8") as f:
            f.write("")
        print(f"\n{GREEN}[OK] Factory stream log cleared.{RESET}")
    except Exception as e:
        print(f"\n{RED}Error clearing log: {e}{RESET}")
    time.sleep(0.8)

def main():
    while True:
        try:
            # Launch directly into the Live Log Stream!
            tail_factory_stream()
        except KeyboardInterrupt:
            pass

        try:
            # Command Menu displayed when user presses Ctrl+C
            print(f"\n{BOLD}{CYAN}==============================================================================={RESET}")
            print(f"{BOLD}{CYAN} 🏭 FACTORY FLOOR CONTROLLER {RESET}")
            print(f"{BOLD}{CYAN}==============================================================================={RESET}")
            print(f" [{BOLD}1{RESET}] 📺 {BOLD}Resume Live Log Stream{RESET}")
            print(f" [{BOLD}2{RESET}] 🔍 {BOLD}Trigger Sitemap Discovery{RESET} (Station 1)")
            print(f" [{BOLD}3{RESET}] 🚀 {BOLD}Enqueue Remaining Products{RESET} (Station 2)")
            print(f" [{BOLD}4{RESET}] ✍️  {BOLD}Queue Blurbs for Top Klasses (>= 5 products){RESET} (Station 4)")
            print(f" [{BOLD}5{RESET}] 🧬 {BOLD}Queue Contextual Facet Blurbs for Top Klasses{RESET} (Station 5)")
            print(f" [{BOLD}6{RESET}] 🧹 {BOLD}Clear Log Screen / Reset Log File{RESET}")
            print(f" [{BOLD}0{RESET}] 🚪 {BOLD}Exit to Shell{RESET}")
            print(f"{BOLD}{CYAN}==============================================================================={RESET}")

            choice = input(f"{BOLD}Select an action [1-6, 0 to exit]: {RESET}").strip()

            if choice == "1" or choice == "":
                continue
            elif choice == "2":
                action_sync_sitemap()
            elif choice == "3":
                action_enqueue_missing()
            elif choice == "4":
                action_synthesize_top_blurbs()
            elif choice == "5":
                action_synthesize_facet_blurbs()
            elif choice == "6":
                action_clear_log()
            elif choice in ["0", "q", "exit", "quit"]:
                print(f"\n{GREEN}Exiting Factory Console.{RESET}\n")
                break
            else:
                time.sleep(0.5)
        except KeyboardInterrupt:
            print(f"\n{GREEN}Exiting Factory Console.{RESET}\n")
            break

if __name__ == "__main__":
    main()
