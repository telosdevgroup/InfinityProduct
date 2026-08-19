#!/usr/bin/env python3
"""
Station 5: Contextual Facet-Value Blurb Synthesizer Worker (facet_blurb_q)
Network-ready: Can run on secondary computers pointing to REDIS_HOST.
"""
import os
import sys

ROOT_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if ROOT_DIR not in sys.path:
    sys.path.insert(0, ROOT_DIR)

from core.celery_app import app

if __name__ == "__main__":
    app.worker_main([
        "worker",
        "-n", "facet_blurb_worker@%h",
        "-Q", "facet_blurb_q",
        "-P", "threads",
        "-c", "1",
        "--loglevel=warning"
    ])
