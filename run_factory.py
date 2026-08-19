#!/usr/bin/env python3
"""
InfinityProduct Factory Floor -- 24/7 Runtime Supervisor
Spawns and manages the 4 independent station worker scripts with non-blocking streaming.
"""

import subprocess
import sys
import os
import time
import signal
import threading

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
LOG_FILE_PATH = os.path.join(BASE_DIR, "factory_stream.log")

WORKERS = [
    {"name": "Station 1: Sitemap Discovery", "script": os.path.join("workers", "worker_sitemap.py"), "queue": "check_sitemap_q"},
    {"name": "Station 2: Ingestion & Facets", "script": os.path.join("workers", "worker_ingest.py"),  "queue": "ingest_q"},
    {"name": "Station 3: LLM Klass Classifier", "script": os.path.join("workers", "worker_klass.py"), "queue": "klass_q"},
    {"name": "Station 4: LLM Blurb Synthesizer", "script": os.path.join("workers", "worker_klass_blurb.py"), "queue": "klass_blurb_q"},
]

processes = []
is_running = True

def stream_worker_output(proc, name):
    """Background thread to read and display worker stdout without blocking."""
    import re
    for line in iter(proc.stdout.readline, ''):
        if not line:
            break
        clean = line.rstrip()
        if clean:
            # Strip Celery internal logging header [2026-08-18 21:11:42,509: WARNING/MainProcess]
            clean = re.sub(r'^\[\d{4}-\d{2}-\d{2}\s+[\d:,]+\s+[A-Z]+/[A-Za-z0-9_]+\]\s*', '', clean)
            if clean:
                print(clean, flush=True)
                try:
                    with open(LOG_FILE_PATH, "a", encoding="utf-8") as f:
                        f.write(clean + "\n")
                except Exception:
                    pass
    proc.stdout.close()

def start_worker(item):
    script_path = os.path.join(BASE_DIR, item["script"])
    cmd = [sys.executable, "-u", script_path]
    p = subprocess.Popen(
        cmd,
        cwd=BASE_DIR,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        bufsize=1
    )
    t = threading.Thread(target=stream_worker_output, args=(p, item["name"]), daemon=True)
    t.start()
    return p

def shutdown(sig=None, frame=None):
    global is_running
    if not is_running:
        return
    is_running = False
    print("\n[!] Stopping all 4 factory station workers...", flush=True)
    for item in processes:
        p = item.get("proc")
        if p and p.poll() is None:
            p.terminate()
    time.sleep(0.8)
    for item in processes:
        p = item.get("proc")
        if p and p.poll() is None:
            p.kill()
    print("[OK] All factory workers stopped cleanly.", flush=True)
    os._exit(0)

def main():
    signal.signal(signal.SIGINT, shutdown)
    signal.signal(signal.SIGTERM, shutdown)

    print("===============================================================================", flush=True)
    print(" 🏭 INFINITYPRODUCT INDUSTRIAL FACTORY FLOOR -- 24/7 RUNTIME SUPERVISOR", flush=True)
    print("===============================================================================", flush=True)
    print(" Spawning 4 independent station worker scripts:", flush=True)
    for w in WORKERS:
        print(f"  • {w['name']:<35} -> python {w['script']} ({w['queue']})", flush=True)
    print("-------------------------------------------------------------------------------", flush=True)
    print(" Press Ctrl+C to stop all workers.", flush=True)
    print("===============================================================================\n", flush=True)

    for w in WORKERS:
        proc = start_worker(w)
        processes.append({"config": w, "proc": proc})
        print(f" [OK] Started {w['name']} (PID {proc.pid})", flush=True)

    print("\n[OK] All 4 station worker scripts active and running 24/7.\n", flush=True)

    while is_running:
        for item in processes:
            p = item["proc"]
            w = item["config"]
            # Auto-restart if a worker crashed unexpectedly
            if is_running and p.poll() is not None:
                print(f"[!] {w['name']} died (exit code {p.returncode}). Restarting...", flush=True)
                new_proc = start_worker(w)
                item["proc"] = new_proc
                print(f"[OK] Restarted {w['name']} (PID {new_proc.pid})", flush=True)
        time.sleep(0.5)

if __name__ == "__main__":
    main()
