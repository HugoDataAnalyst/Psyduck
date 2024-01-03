from celery import Celery
from app_config import app_config

celery = Celery(__name__, broker=app_config.celery_broker_url, backend=app_config.celery_result_backend)