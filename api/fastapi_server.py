import logging
from logging.handlers import RotatingFileHandler
from logging import StreamHandler
from fastapi import FastAPI, HTTPException, Depends, Header, Request
from starlette.responses import JSONResponse
from fastapi_cache import FastAPICache
from fastapi_cache.backends.redis import RedisBackend
from fastapi_cache.decorator import cache
from redis import asyncio as aioredis
from celery.result import AsyncResult
from celery import Celery
from config.app_config import app_config
from processor.tasks import query_daily_api_pokemon_stats, query_weekly_api_pokemon_stats, query_monthly_api_pokemon_stats, query_hourly_total_api_pokemon_stats, query_daily_total_api_pokemon_stats, query_total_api_pokemon_stats, query_daily_surge_api_pokemon_stats, query_weekly_surge_api_pokemon_stats, query_monthly_surge_api_pokemon_stats


# Create a custom logger
logger = logging.getLogger("my_logger")

log_level_str = app_config.api_log_level.upper()

# Console logger
console_log_level_str = app_config.api_console_log_level.upper()
if console_log_level_str == "OFF":
    logger.setLevel(logging.NOTSET)
else:
    console_log_level = getattr(logging, console_log_level_str, logging.INFO)
    logger.setLevel(console_log_level)

# Log file logger
if log_level_str == "OFF":
    logger.setLevel(logging.NOTSET)
else:
    log_level = getattr(logging, log_level_str, logging.INFO)
    logger.setLevel(log_level)

# Create a file handler that logs even debug messages
file_handler = RotatingFileHandler(app_config.api_log_file, maxBytes=app_config.api_log_max_bytes, backupCount=app_config.api_max_log_files)
file_handler.setLevel(log_level)

# Console logger
console_handler = StreamHandler()
console_handler.setLevel(console_log_level)

# Create and set the formatter
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
file_handler.setFormatter(formatter)
console_handler.setFormatter(formatter)

# Add the handler to the logger
logger.addHandler(file_handler)
logger.addHandler(console_handler)

ALLOWED_PATHS = [
    "/api/daily-area-pokemon-stats",
    "/api/weekly-area-pokemon-stats",
    "/api/monthly-area-pokemon-stats",
    "/api/hourly-total-pokemon-stats",
    "/api/daily-total-pokemon-stats",
    "/api/total-pokemon-stats",
    "/api/surge-daily-stats",
    "/api/surge-weekly-stats",
    "/api/surge-monthly-stats"
]

async def check_path_middleware(request: Request, call_next):
    if app_config.api_path_restriction:
        if request.url.path not in ALLOWED_PATHS:
            logger.warning(f"Access denied for path: {request.url.path}")
            return JSONResponse(status_code=403, content={"detail": "Forbidden"})
    
    return await call_next(request)

async def check_ip_middleware(request: Request, call_next):
    if app_config.api_ip_restriction:
        client_host = request.client.host
        if app_config.api_ip_restriction and client_host not in app_config.api_allowed_ips:
            logger.info(f"Access denied for IP: {client_host}")
            # Return a 403 Forbidden response
            return JSONResponse(status_code=403, content={"detail": "Access denied"})

        logger.info(f"Access from IP: {client_host} allowed.")
    return await call_next(request)

fastapi = FastAPI()

fastapi.middleware('http')(check_ip_middleware)
fastapi.middleware('http')(check_path_middleware)

@fastapi.on_event("startup")
async def startup():
    logger.info("Starting up the application")
    redis = aioredis.from_url(app_config.redis_url, encoding="utf8", decode_responses=True)
    FastAPICache.init(RedisBackend(redis), prefix="fastapi-cache")
    logger.info("FastAPI cache initialized with Redis backend")



async def validate_secret_header(secret: str = Header(None, alias=app_config.api_header_name)):
    if not secret or secret != app_config.api_secret_header_key:
        logger.warning("Unauthorized access attempt with wrong secret header")
        raise HTTPException(status_code=403, detail="Unauthorized access")
    logger.info("Secret header validated successfully.")

async def validate_secret(secret: str = None):
    if not secret or secret != app_config.api_secret_key:
        logger.warning("Unauthorized access attempt with wrong secret")
        raise HTTPException(status_code=403, detail="Unauthorized access")
    logger.info("Secret validated successfully.")

async def validate_ip(request: Request):
    client_host = request.client.host
    if app_config.api_ip_restriction and client_host not in app_config.api_allowed_ips:
        logger.info(f"Access denied for IP: {client_host}")
        raise HTTPException(status_code=403, detail="Access denied")
    logger.info(f"Access from IP: {client_host} allowed.")

def get_task_result(task_function, *args, **kwargs):
    logger.info(f"Fetching task result for {task_function.__name__}")
    result = task_function.delay(*args, **kwargs)
    return result.get(timeout=50)

# API Grouped
@fastapi.get("/api/daily-area-pokemon-stats")
@cache(expire=app_config.api_daily_pokemon_cache)
async def daily_area_pokemon_stats(request: Request, secret: str = Depends(validate_secret), _ip = Depends(validate_ip), _header = Depends(validate_secret_header)):
    logger.info("Request received for daily Pokemon stats")
    return get_task_result(query_daily_api_pokemon_stats)

@fastapi.get("/api/weekly-area-pokemon-stats")
@cache(expire=app_config.api_weekly_pokemon_cache)
async def weekly_area_pokemon_stats(request: Request, secret: str = Depends(validate_secret), _ip = Depends(validate_ip), _header = Depends(validate_secret_header)):
    logger.info("Request received for weekly Pokemon stats")
    return get_task_result(query_weekly_api_pokemon_stats)

@fastapi.get("/api/monthly-area-pokemon-stats")
@cache(expire=app_config.api_monthly_pokemon_cache)
async def monthly_area_pokemon_stats(request: Request, secret: str = Depends(validate_secret), _ip = Depends(validate_ip), _header = Depends(validate_secret_header)):
    logger.info("Request received for monthly Pokemon stats")
    return get_task_result(query_monthly_api_pokemon_stats)

# API Totals
@fastapi.get("/api/hourly-total-pokemon-stats")
@cache(expire=app_config.api_hourly_total_pokemon_cache)
async def hourly_total_pokemon_stats(request: Request, secret: str = Depends(validate_secret), _ip = Depends(validate_ip), _header = Depends(validate_secret_header)):
    logger.info("Request received for hourly total Pokemon stats")
    return get_task_result(query_hourly_total_api_pokemon_stats)

@fastapi.get("/api/daily-total-pokemon-stats")
@cache(expire=app_config.api_daily_total_pokemon_cache)
async def daily_total_pokemon_stats(request: Request, secret: str= Depends(validate_secret), _ip = Depends(validate_ip), _header = Depends(validate_secret_header)):
    logger.info("Request received for daily total Pokemon stats")
    return get_task_result(query_daily_total_api_pokemon_stats)

@fastapi.get("/api/total-pokemon-stats")
@cache(expire=app_config.api_total_pokemon_cache)
async def total_pokemon_stats(request: Request, secret: str= Depends(validate_secret), _ip = Depends(validate_ip), _header = Depends(validate_secret_header)):
    logger.info("Request received for total Pokemon stats")
    return get_task_result(query_total_api_pokemon_stats)

# API Surge's
@fastapi.get("/api/surge-daily-stats")
@cache(expire=app_config.api_surge_daily_cache)
async def surge_daily_pokemon_stats(request: Request, secret: str= Depends(validate_secret), _ip = Depends(validate_ip), _header = Depends(validate_secret_header)):
    logger.info("Request received for Surge Pokemon Daily Stats")
    return get_task_result(query_daily_surge_api_pokemon_stats)

@fastapi.get("/api/surge-weekly-stats")
@cache(expire=app_config.api_surge_weekly_cache)
async def surge_weekly_pokemon_stats(request: Request, secret: str= Depends(validate_secret), _ip = Depends(validate_ip), _header = Depends(validate_secret_header)):
    logger.info("Request received for Surge Pokemon Weekly Stats")
    return get_task_result(query_weekly_surge_api_pokemon_stats)

@fastapi.get("/api/surge-monthly-stats")
@cache(expire=app_config.api_surge_monthly_cache)
async def surge_monthly_pokemon_stats(request: Request, secret: str= Depends(validate_secret), _ip = Depends(validate_ip), _header = Depends(validate_secret_header)):
    logger.info("Request received for Surge Pokemon Monthly Stats")
    return get_task_result(query_monthly_surge_api_pokemon_stats)