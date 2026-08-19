#!/usr/bin/env python3
"""Station 3: Taxonomic Klass Classifier Worker (klass_q)"""
import os
import sys

ROOT_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if ROOT_DIR not in sys.path:
    sys.path.insert(0, ROOT_DIR)

from core.celery_app import app

if __name__ == "__main__":
    app.worker_main([
        "worker",
        "-n", "klass_worker@%h",
        "-Q", "klass_q",
        "-P", "threads",
        "-c", "2",
        "--loglevel=warning"
    ])
