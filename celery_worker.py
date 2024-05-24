import asyncio
from processor.celery_app import celery
from config.app_config import app_config
from run_migrations import run_migrations
from orm.orm_db import init as orm_init, close as orm_close

if __name__ == "__main__":

    try:
        run_migrations()
    except Exception as e:
        print(f"Error running migrations: {e}")
        raise e

    # Initialize the ORM
    loop = asyncio.get_event_loop()
    loop.run_until_complete(orm_init())

    worker = celery.Worker(
        concurrency=app_config.celery_workers,
        loglevel=app_config.celery_console_log_level
    )
    # Start the work and ensure ORM connections are closed when the worker stops
    try:
        worker.start()
    finally:
        loop.run_until_complete(orm_close())
