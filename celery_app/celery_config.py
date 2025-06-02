from celery import Celery

app = Celery(
    "etl_tasks",
    broker="redis://localhost:6379/0",  # Redis running in Docker
    backend="redis://localhost:6379/0",
    include=["celery_app.tasks", "timestamp_based"],  # Include your tasks module
)

# Optional: Set task settings
app.conf.update(
    task_serializer="json",
    result_serializer="json",
    accept_content=["json"],
    timezone="UTC",
    enable_utc=True,
)

if __name__ == "__main__":
    app.start()
