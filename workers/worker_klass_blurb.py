#!/usr/bin/env python3
"""Station 5: Klass Blurb Synthesizer Worker (klass_blurb_q)"""
import os
import sys

ROOT_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if ROOT_DIR not in sys.path:
    sys.path.insert(0, ROOT_DIR)

from core.celery_app import app

if __name__ == "__main__":
    app.worker_main([
        "worker",
        "-n", "klass_blurb_worker@%h",
        "-Q", "klass_blurb_q",
        "-P", "threads",
        "-c", "1",
        "--loglevel=warning"
    ])
