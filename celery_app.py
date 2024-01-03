from celery import Celery
from app_config import app_config

celery = Celery(__name__, broker=app_config.celery_broker_url, backend=app_config.celery_result_backend)

celery.conf.broker_connection_retry_on_startup = True
celery.conf.task_acks_late = True
celery.conf.worker_reject_on_worker_lost = True
