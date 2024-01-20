from processor.celery_app import celery
from config.app_config import app_config
from run_migrations import run_migrations

if __name__ == "__main__":

    try:
        run_migrations()
    except Exception as e:
        print(f"Error running migrations: {e}")
        raise e

    worker = celery.Worker(
        concurrency=app_config.celery_workers,
        loglevel=app_config.celery_console_log_level
    )
    worker.start()
