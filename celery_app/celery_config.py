from celery import Celery

# worker_with_metrics.py
import os
from celery import Celery
from prometheus_client import start_http_server, PROCESS_COLLECTOR, PLATFORM_COLLECTOR
from prometheus_client import (
    multiprocess,
    generate_latest,
    CONTENT_TYPE_LATEST,
    CollectorRegistry,
)


app = Celery(
    "etl_tasks",
    broker="redis://localhost:6379/0",  # Redis running in Docker
    backend="redis://localhost:6379/0",
    include=["celery_app.tasks", "celery_app.incremental"],  # Include your tasks module
)

# Optional: Set task settings
app.conf.update(
    task_serializer="json",
    result_serializer="json",
    accept_content=["json"],
    timezone="UTC",
    enable_utc=True,
    worker_send_task_events=True,
    task_send_sent_event=True,
)

# app.conf.beat_schedule = {
#  “run-me-every-ten-seconds”: {
#  “task”: “tasks.check”,
#  “schedule”: 10.0
#  }
# }


# Start Prometheus metrics server
# def setup_metrics():
#     # Start HTTP server for Prometheus metrics
#     port = int(os.environ.get("PROMETHEUS_PORT", 8000))
#     start_http_server(port)
#     print(f"Prometheus metrics available at http://localhost:{port}/metrics")


# # Worker ready signal
# @app.control.inspect().registered_tasks
# def worker_ready(sender=None, **kwargs):
#     setup_metrics()


if __name__ == "__main__":
    # setup_metrics()
    app.start()
