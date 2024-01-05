from celery_app import celery
import app_config

if __name__ == "__main__":
    worker = celery.Worker(
        concurrency=app_config.celery_workers,
        loglevel=app_config.celery_log_level
    )
    worker.start()
