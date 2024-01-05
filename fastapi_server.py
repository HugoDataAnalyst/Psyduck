import logging
from logging.handlers import RotatingFileHandler
from fastapi import FastAPI, HTTPException, Depends, Header, Request
from fastapi_cache import FastAPICache
from fastapi_cache.backends.redis import RedisBackend
from fastapi_cache.decorator import cache
from redis import asyncio as aioredis
from celery.result import AsyncResult
from celery import Celery
from app_config import app_config
from tasks import query_daily_pokemon_stats, query_weekly_pokemon_stats, query_monthly_pokemon_stats


# Create a custom logger
logger = logging.getLogger("my_logger")
logger.setLevel(app_config.api_log_level)

# Create a file handler that logs even debug messages
handler = RotatingFileHandler(app_config.api_log_file, maxBytes=app_config.api_log_max_bytes, backupCount=app_config.api_max_log_files)
handler.setLevel(logging.INFO)

# Create and set the formatter
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
handler.setFormatter(formatter)

# Add the handler to the logger
logger.addHandler(handler)

app = FastAPI()

@app.on_event("startup")
async def startup():
    logger.info("Starting up the application")
    redis = aioredis.from_url(app_config.redis_url, encoding="utf8", decode_responses=True)
    FastAPICache.init(RedisBackend(redis), prefix="fastapi-cache")
    logger.info("FastAPI cache initialized with Redis backend")

def validate_secret_header(secret: str = Header(None, alias=app_config.api_header_name)):
    if not secret or secret != app_config.api_secret_header_key:
        logger.warning("Unauthorized access attempt with wrong secret header")
        raise HTTPException(status_code=403, detail="Unauthorized access")

def validate_secret(secret: str = None):
    if not secret or secret != app_config.api_secret_key:
        logger.warning("Unauthorized access attempt with wrong secret")
        raise HTTPException(status_code=403, detail="Unauthorized access")

def validate_ip(request: Request):
    client_host = request.client.host
    logger.info(f"Access attempt from IP: {client_host}")
    if app_config.api_ip_restriction and client_host not in app_config.api_allowed_ips:
        logger.info(f"Access denied for IP: {client_host}")
        raise HTTPException(status_code=403, detail="Access denied")

def get_task_result(task_function, *args, **kwargs):
    logger.info(f"Fetching task result for {task_function.__name__}")
    result = task_function.delay(*args, **kwargs)
    return result.get(timeout=50)

@app.get("/api/daily-area-pokemon-stats")
@cache(expire=app_config.api_daily_cache)
async def daily_pokemon_stats(request: Request, secret: str = Depends(validate_secret)):
    validate_ip(request)
    logger.info("Request received for daily Pokemon stats")
    return get_task_result(query_daily_pokemon_stats)

@app.get("/api/weekly-area-pokemon-stats")
@cache(expire=app_config.api_weekly_cache)
async def weekly_pokemon_stats(request: Request, secret: str = Depends(validate_secret)):
    validate_ip(request)
    logger.info("Request received for weekly Pokemon stats")
    return get_task_result(query_weekly_pokemon_stats)

@app.get("/api/monthly-area-pokemon-stats")
@cache(expire=app_config.api_monthly_cache)
async def monthly_pokemon_stats(request: Request, secret: str = Depends(validate_secret)):
    validate_ip(request)
    logger.info("Request received for monthly Pokemon stats")
    return get_task_result(query_monthly_pokemon_stats)
