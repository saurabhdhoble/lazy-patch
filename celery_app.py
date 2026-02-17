import os
from celery import Celery
from kombu import Queue

# ------------------------------------------------------------------------------
# Environment configuration (12-factor style)
# ------------------------------------------------------------------------------

BROKER_URL = os.getenv("CELERY_BROKER_URL", "redis://localhost:6379/0")
RESULT_BACKEND = os.getenv("CELERY_RESULT_BACKEND", "redis://localhost:6379/1")

DEFAULT_QUEUE = os.getenv("CELERY_DEFAULT_QUEUE", "default")
TIMEZONE = os.getenv("CELERY_TIMEZONE", "UTC")

# ------------------------------------------------------------------------------
# Create Celery app
# ------------------------------------------------------------------------------

celery_app = Celery(
    "app",
    broker=BROKER_URL,
    backend=RESULT_BACKEND,
    include=["tasks"],  # change to your module path
)

# ------------------------------------------------------------------------------
# Production-ready configuration
# ------------------------------------------------------------------------------

celery_app.conf.update(
    # Serialization
    task_serializer="json",
    result_serializer="json",
    accept_content=["json"],

    # Time / reliability
    timezone=TIMEZONE,
    enable_utc=True,

    # Task behavior
    task_acks_late=True,
    task_reject_on_worker_lost=True,
    task_track_started=True,

    # Worker behavior
    worker_prefetch_multiplier=1,
    worker_max_tasks_per_child = 250,  # Recycle workers periodically to prevent memory leaks from accumulating

    # Result backend behavior
    result_expires=3600,

    # Queues
    task_default_queue=DEFAULT_QUEUE,
    task_queues=(
        Queue(DEFAULT_QUEUE),
    ),
)

# ------------------------------------------------------------------------------
# Optional: health check task
# ------------------------------------------------------------------------------

@celery_app.task(name="health_check")
def health_check():
    return {"status": "ok"}
