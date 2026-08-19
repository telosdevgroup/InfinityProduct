import os
import sys

# Add project root to sys.path
ROOT_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if ROOT_DIR not in sys.path:
    sys.path.insert(0, ROOT_DIR)

import redis.connection
from celery import Celery

# Patch redis-py connection for RESP2 protocol compatibility on Redis < 6.0
redis.connection.Connection._configure_maintenance_notifications = lambda *args, **kwargs: None

_orig_init = redis.connection.Connection.__init__
def _patched_init(self, *args, **kwargs):
    kwargs["protocol"] = 2
    kwargs["maint_notifications_pool_handler"] = None
    _orig_init(self, *args, **kwargs)
redis.connection.Connection.__init__ = _patched_init

is_local = "--local" in sys.argv or os.environ.get("LOCAL", "0").lower() in ["1", "true"]
DEFAULT_HOST = "localhost" if is_local else "192.168.1.213"
REDIS_HOST = os.environ.get("REDIS_HOST", DEFAULT_HOST)
REDIS_URL = f"redis://{REDIS_HOST}:6379/0"

app = Celery("infinity_factory", broker=REDIS_URL, include=["core.tasks"])

app.conf.update(
    task_serializer="json",
    accept_content=["json"],
    result_serializer="json",
    timezone="UTC",
    enable_utc=True,
    task_ignore_result=True,
    task_acks_late=True,
    task_reject_on_worker_lost=True,
    worker_prefetch_multiplier=1,
    worker_log_format="%(message)s",
    worker_task_log_format="%(message)s",
    broker_transport_options={
        "visibility_timeout": 86400,  # 24 hours
        "max_connections": 50,
    },
    task_routes={
        "core.tasks.check_sitemap": {"queue": "check_sitemap_q"},
        "tasks.check_sitemap": {"queue": "check_sitemap_q"},
        "core.tasks.sync_source_catalog": {"queue": "check_sitemap_q"},
        "tasks.sync_source_catalog": {"queue": "check_sitemap_q"},
        "core.tasks.ingest_product_url": {"queue": "ingest_q"},
        "tasks.ingest_product_url": {"queue": "ingest_q"},
        "core.tasks.infer_klass": {"queue": "klass_q"},
        "tasks.infer_klass": {"queue": "klass_q"},
        "core.tasks.reconcile_klass": {"queue": "klass_reconcile_q"},
        "tasks.reconcile_klass": {"queue": "klass_reconcile_q"},
        "core.tasks.generate_klass_blurb": {"queue": "klass_blurb_q"},
        "tasks.generate_klass_blurb": {"queue": "klass_blurb_q"},
        "core.tasks.generate_facet_value_blurb": {"queue": "facet_blurb_q"},
        "tasks.generate_facet_value_blurb": {"queue": "facet_blurb_q"},
    },
    beat_schedule={
        "check-emedicalkits-sitemap-every-6h": {
            "task": "core.tasks.check_sitemap",
            "schedule": 21600.0,  # 6 hours
            "args": ("emedicalkits",),
        },
    },
)

if __name__ == "__main__":
    app.start()
