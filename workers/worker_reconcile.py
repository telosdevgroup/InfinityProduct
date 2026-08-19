#!/usr/bin/env python3
"""
Station 3B: Taxonomy Reconciliation & Resolution Worker (klass_reconcile_q)
Network-ready: Can run across multiple machines pointing to REDIS_HOST.
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
        "-n", "reconcile_worker@%h",
        "-Q", "klass_reconcile_q",
        "-P", "threads",
        "-c", "2",
        "--loglevel=warning"
    ])
