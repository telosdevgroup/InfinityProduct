#!/usr/bin/env python3
"""Station 1: Sitemap Discovery Worker (check_sitemap_q)"""
import os
import sys

ROOT_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if ROOT_DIR not in sys.path:
    sys.path.insert(0, ROOT_DIR)

from core.celery_app import app

if __name__ == "__main__":
    app.worker_main([
        "worker",
        "-n", "sitemap_worker@%h",
        "-Q", "check_sitemap_q",
        "-P", "threads",
        "-c", "1",
        "--loglevel=warning"
    ])
