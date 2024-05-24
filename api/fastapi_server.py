import logging
from logging.handlers import RotatingFileHandler
from logging import StreamHandler
from fastapi import FastAPI, HTTPException, Depends, Header, Request, Response
from starlette.responses import JSONResponse
from fastapi_cache import FastAPICache
from fastapi_cache.backends.redis import RedisBackend
from fastapi_cache.decorator import cache
import redis
from redis import asyncio as aioredis
from celery.result import AsyncResult
from celery import Celery
from config.app_config import app_config
#from processor.tasks import query_daily_pokemon_grouped_stats, query_weekly_pokemon_grouped_stats, query_monthly_pokemon_grouped_stats, query_hourly_pokemon_total_stats, query_daily_pokemon_total_stats, query_pokemon_total_stats, query_daily_surge_api_pokemon_stats, query_weekly_surge_api_pokemon_stats, query_monthly_surge_api_pokemon_stats, query_daily_quest_grouped_stats_api, query_weekly_quest_grouped_stats_api, query_monthly_quest_grouped_stats_api, query_daily_quest_total_stats_api, query_total_quest_total_stats_api, query_daily_raid_grouped_stats_api, query_weekly_raid_grouped_stats_api, query_monthly_raid_grouped_stats_api, query_hourly_raid_total_stats_api, query_daily_raid_total_stats_api, query_total_raid_total_stats_api, query_daily_invasion_grouped_stats_api, query_weekly_invasion_grouped_stats_api, query_monthly_invasion_grouped_stats_api, query_hourly_invasions_total_stats_api, query_daily_invasions_total_stats_api, query_total_invasions_total_stats_api, query_hourly_pokemon_tth_stats_api, query_daily_pokemon_tth_stats_api
from processor.tasks import CeleryTasks
from utils.time_utils import seconds_until_next_hour, seconds_until_midnight, seconds_until_next_week, seconds_until_next_month, seconds_until_fourpm, seconds_until_next_week_fourpm, seconds_until_next_month_fourpm
import os
import json
from datetime import date, datetime

# CeleryTasks Initialiazer
query_tasks = CeleryTasks()

# Configuration values
console_log_level_str = app_config.api_console_log_level.upper()
log_level_str = app_config.api_log_level.upper()
log_file = app_config.api_log_file
max_bytes = app_config.api_log_max_bytes
backup_count = app_config.api_max_log_files

# Console logger
console_logger = logging.getLogger("api_console_logger")
if console_log_level_str == "OFF":
    console_logger.disabled = True
else:
    console_log_level = getattr(logging, console_log_level_str, logging.INFO)
    console_logger.setLevel(console_log_level)
    #handler
    console_handler = logging.StreamHandler()
    console_handler.setLevel(console_log_level)
    # Formatter
    console_formatter = logging.Formatter('[%(asctime)s] %(levelname)s in %(module)s: %(message)s')
    console_handler.setFormatter(console_formatter)
    console_logger.addHandler(console_handler)

# File logger
file_logger = logging.getLogger("api_file_logger")
if log_level_str == "OFF":
    log_level = logging.NOTSET
else:
    log_level = getattr(logging, log_level_str, logging.INFO)

file_logger.setLevel(log_level)
if not os.path.exists(os.path.dirname(log_file)):
    os.makedirs(os.path.dirname(log_file))
file_handler = RotatingFileHandler(log_file, maxBytes=max_bytes, backupCount=backup_count)
file_handler.setLevel(log_level)
file_formatter = logging.Formatter('[%(asctime)s] %(levelname)s in %(module)s: %(message)s')
file_handler.setFormatter(file_formatter)
file_logger.addHandler(file_handler)

ALLOWED_PATHS = [
    "/api/daily-area-pokemon-stats",
    "/api/weekly-area-pokemon-stats",
    "/api/monthly-area-pokemon-stats",
    "/api/hourly-total-pokemon-stats",
    "/api/daily-total-pokemon-stats",
    "/api/total-pokemon-stats",
    "/api/surge-daily-stats",
    "/api/surge-weekly-stats",
    "/api/surge-monthly-stats",
    "/api/daily-quest-grouped-stats",
    "/api/weekly-quest-grouped-stats",
    "/api/monthly-quest-grouped-stats",
    "/api/daily-quest-total-stats",
    "/api/total-quest-total-stats",
    "/api/daily-raid-grouped-stats",
    "/api/weekly-raid-grouped-stats",
    "/api/monthly-raid-grouped-stats",
    "/api/hourly-raid-total-stats",
    "/api/daily-raid-total-stats",
    "/api/total-raid-total-stats",
    "/api/daily-invasion-grouped-stats",
    "/api/weekly-invasion-grouped-stats",
    "/api/monthly-invasion-grouped-stats",
    "/api/hourly-invasion-total-stats",
    "/api/daily-invasion-total-stats",
    "/api/total-invasion-total-stats",
    "/api/hourly-pokemon-tth-stats",
    "/api/daily-pokemon-tth-stats",
    "/metrics/daily-area-pokemon",
    "/metrics/weekly-area-pokemon",
    "/metrics/monthly-area-pokemon",
    "/metrics/total-hourly-pokemon",
    "/metrics/total-daily-pokemon",
    "/metrics/total-pokemon",
    "/metrics/surge-daily-stats",
    "/metrics/surge-weekly-stats",
    "/metrics/surge-monthly-stats",
    "/metrics/daily-quest-grouped-stats",
    "/metrics/weekly-quest-grouped-stats",
    "/metrics/monthly-quest-grouped-stats",
    "/metrics/daily-quest-total-stats",
    "/metrics/total-quest-total-stats",
    "/metrics/daily-raid-grouped-stats",
    "/metrics/weekly-raid-grouped-stats",
    "/metrics/monthly-raid-grouped-stats",
    "/metrics/hourly-raid-total-stats",
    "/metrics/daily-raid-total-stats",
    "/metrics/total-raid-total-stats",
    "/metrics/daily-invasion-grouped-stats",
    "/metrics/weekly-invasion-grouped-stats",
    "/metrics/monthly-invasion-grouped-stats",
    "/metrics/hourly-invasion-total-stats",
    "/metrics/daily-invasion-total-stats",
    "/metrics/total-invasion-total-stats",
    "/metrics/hourly-pokemon-tth-stats",
    "/metrics/daily-pokemon-tth-stats"
]

async def check_path_middleware(request: Request, call_next):
    if app_config.api_path_restriction:
        if request.url.path not in ALLOWED_PATHS:
            console_logger.warning(f"Access denied for path: {request.url.path}")
            file_logger.warning(f"Access denied for path: {request.url.path}")
            return JSONResponse(status_code=403, content={"detail": "Forbidden"})
    return await call_next(request)

async def check_ip_middleware(request: Request, call_next):
    if app_config.api_ip_restriction:
        client_host = request.client.host
        if app_config.api_ip_restriction and client_host not in app_config.api_allowed_ips:
            console_logger.warning(f"Access denied for IP: {client_host}")
            file_logger.warning(f"Access denied for IP: {client_host}")
            # Return a 403 Forbidden response
            return JSONResponse(status_code=403, content={"detail": "Access denied"})

        console_logger.info(f"Access from IP: {client_host} allowed.")
        file_logger.info(f"Access from IP: {client_host} allowed.")
    return await call_next(request)

fastapi = FastAPI()

fastapi.middleware('http')(check_ip_middleware)
fastapi.middleware('http')(check_path_middleware)

# Initiliaze Redis
redis_client = redis.StrictRedis.from_url(app_config.redis_url)

@fastapi.on_event("startup")
async def startup():
    console_logger.info("Starting up the application")
    file_logger.info("Starting up the application")

    # Conditionally clear Redis cache based on configuration
    if app_config.redis_clean:
        redis_client.flushdb()
        console_logger.info("Previous cache cleared on startup")
        file_logger.info("Previous cache cleared on startup")
    else:
        console_logger.info("Redis cache not cleared on startup")
        file_logger.info("Redis cache not cleared on startup")


async def validate_secret_header(secret: str = Header(None, alias=app_config.api_header_name)):
    if app_config.api_secret_header_key:
        if secret != app_config.api_secret_header_key:
    #if not secret or secret != app_config.api_secret_header_key:
            console_logger.warning("Unauthorized access attempt with wrong secret header")
            file_logger.warning("Unauthorized access attempt with wrong secret header")
            raise HTTPException(status_code=403, detail="Unauthorized access")
        console_logger.debug("Secret header validated successfully.")
        file_logger.debug("Secret header validated successfully.")
    else:
        console_logger.debug("No API secret header key set, skipping secret validation.")
        file_logger.debug("No API secret header key set, skipping secret validation.")

async def validate_secret(secret: str = None):
    if app_config.api_secret_key:
        if secret != app_config.api_secret_key:
            console_logger.warning("Unauthorized access attempt with wrong secret")
            file_logger.warning("Unauthorized access attempt with wrong secret")
            raise HTTPException(status_code=403, detail="Unauthorized access")
        console_logger.debug("Secret validated successfully.")
        file_logger.debug("Secret validated successfully.")
    else:
        console_logger.debug("No API secret key set, skipping secret validation.")
        file_logger.debug("No API secret key set, skipping secret validation.")

async def validate_ip(request: Request):
    client_host = request.client.host
    if app_config.api_ip_restriction and client_host not in app_config.api_allowed_ips:
        console_logger.warning(f"Access denied for IP: {client_host}")
        file_logger.warning(f"Access denied for IP: {client_host}")
        raise HTTPException(status_code=403, detail="Access denied")
    console_logger.debug(f"Access from IP: {client_host} allowed.")
    file_logger.debug(f"Access from IP: {client_host} allowed.")

def get_task_result(task_function, *args, **kwargs):
    console_logger.debug(f"Fetching task result for {task_function.__name__}")
    file_logger.debug(f"Fetching task result for {task_function.__name__}")
    result = task_function.delay(*args, **kwargs)
    return result.get(timeout=50)

# API Grouped
@fastapi.get("/api/daily-area-pokemon-stats")
async def daily_area_pokemon_stats(request: Request, secret: str = Depends(validate_secret), _ip = Depends(validate_ip), _header = Depends(validate_secret_header)):
    console_logger.debug("Request received for daily Pokemon stats")
    file_logger.debug("Request received for daily Pokemon stats")

    cache_key = "daily-area-pokemon-stats"

    try:
        cached_result = redis_client.get(cache_key)
        if cached_result:
            # If cached result exists, decode it and return
            console_logger.info("Cache hit for daily Pokemon stats")
            file_logger.info("Cache hit for daily Pokemon stats")
            return JSONResponse(content=json.loads(cached_result.decode("utf-8")))
    except Exception as e:
        console_logger.error(f"Error accessing Redis cache for daily Pokemon stats: {e}")
        file_logger.error(f"Error accessing Redis cache for daily Pokemon stats: {e}")

    # If no Cache set
    console_logger.debug("Cache miss for daily Pokemon stats, fetching new data")
    file_logger.debug("Cache miss for daily Pokemon stats, fetching new data")
    result = get_task_result(query_tasks.query_daily_pokemon_grouped_stats)
    serialized_result = json.dumps(result)

    # Cache the new data with dynamic TTL
    ttl = seconds_until_midnight()
    try:
        redis_client.set(cache_key, serialized_result, ex=ttl)
        console_logger.info("Cache set with new daily Pokemon stats")
        file_logger.info("Cache set with new daily Pokemon stats")
    except Exception as e:
        console_logger.error(f"Error setting Redis cache for daily Pokemon stats: {e}")
        file_logger.error(f"Error setting Redis cache for daily Pokemon stats: {e}")

    console_logger.info("Successfully obtained daily Pokemon stats")
    file_logger.info("Sucessfully obtained daily Pokemon stats")
    return JSONResponse(content=result)

@fastapi.get("/api/weekly-area-pokemon-stats")
async def weekly_area_pokemon_stats(request: Request, secret: str = Depends(validate_secret), _ip = Depends(validate_ip), _header = Depends(validate_secret_header)):
    console_logger.debug("Request received for weekly Pokemon stats")
    file_logger.debug("Request received for weekly Pokemon stats")

    cache_key = "weekly-area-pokemon-stats"

    try:
        cached_result = redis_client.get(cache_key)
        if cached_result:
            # If cached result exists, decode it and return
            console_logger.info("Cache hit for weekly Pokemon stats")
            file_logger.info("Cache hit for weekly Pokemon stats")
            return JSONResponse(content=json.loads(cached_result.decode("utf-8")))
    except Exception as e:
        console_logger.error(f"Error accessing Redis cache for weekly Pokemon stats: {e}")
        file_logger.error(f"Error accessing Redis cache for weekly Pokemon stats: {e}")

    # If no Cache set
    console_logger.debug("Cache miss for weekly Pokemon stats, fetching new data")
    file_logger.debug("Cache miss for weekly Pokemon stats, fetching new data")
    result = get_task_result(query_tasks.query_weekly_pokemon_grouped_stats)
    serialized_result = json.dumps(result)

    # Cache the new data with dynamic TTL
    ttl = seconds_until_next_week()
    try:
        redis_client.set(cache_key, serialized_result, ex=ttl)
        console_logger.info("Cache set with new weekly Pokemon stats")
        file_logger.info("Cache set with new weekly Pokemon stats")
    except Exception as e:
        console_logger.error(f"Error setting Redis cache for weekly Pokemon stats: {e}")
        file_logger.error(f"Error setting Redis cache for weekly Pokemon stats: {e}")

    console_logger.info("Successfully obtained weekly Pokemon stats")
    file_logger.info("Sucessfully obtained weekly Pokemon stats")
    return JSONResponse(content=result)

@fastapi.get("/api/monthly-area-pokemon-stats")
async def monthly_area_pokemon_stats(request: Request, secret: str = Depends(validate_secret), _ip = Depends(validate_ip), _header = Depends(validate_secret_header)):
    console_logger.debug("Request received for monthly Pokemon stats")
    file_logger.debug("Request received for monthly Pokemon stats")

    cache_key = "monthly-area-pokemon-stats"

    try:
        cached_result = redis_client.get(cache_key)
        if cached_result:
            # If cached result exists, decode it and return
            console_logger.info("Cache hit for monthly Pokemon stats")
            file_logger.info("Cache hit for monthly Pokemon stats")
            return JSONResponse(content=json.loads(cached_result.decode("utf-8")))
    except Exception as e:
        console_logger.error(f"Error accessing Redis cache for monthly Pokemon stats: {e}")
        file_logger.error(f"Error accessing Redis cache for monthly Pokemon stats: {e}")

    # If no Cache set
    console_logger.debug("Cache miss for monthly Pokemon stats, fetching new data")
    file_logger.debug("Cache miss for monthly Pokemon stats, fetching new data")
    result = get_task_result(query_tasks.query_monthly_pokemon_grouped_stats)
    serialized_result = json.dumps(result)

    # Cache the new data with dynamic TTL
    ttl = seconds_until_next_month()
    try:
        redis_client.set(cache_key, serialized_result, ex=ttl)
        console_logger.info("Cache set with new monthly Pokemon stats")
        file_logger.info("Cache set with new monthly Pokemon stats")
    except Exception as e:
        console_logger.error(f"Error setting Redis cache for monthly Pokemon stats: {e}")
        file_logger.error(f"Error setting Redis cache for monthly Pokemon stats: {e}")

    console_logger.info("Successfully obtained monthly Pokemon stats")
    file_logger.info("Sucessfully obtained monthly Pokemon stats")
    return JSONResponse(content=result)

# API Totals
@fastapi.get("/api/hourly-total-pokemon-stats")
async def hourly_total_pokemon_stats(request: Request, secret: str = Depends(validate_secret), _ip = Depends(validate_ip), _header = Depends(validate_secret_header)):
    console_logger.debug("Request received for hourly total Pokemon stats")
    file_logger.debug("Request received for hourly total Pokemon stats")

    cache_key = "hourly-total-pokemon-stats"

    try:
        cached_result = redis_client.get(cache_key)
        if cached_result:
            # If cached result exists, decode it and return
            console_logger.info("Cache hit for hourly total Pokemon stats")
            file_logger.info("Cache hit for hourly total Pokemon stats")
            return JSONResponse(content=json.loads(cached_result.decode("utf-8")))
    except Exception as e:
        console_logger.error(f"Error accessing Redis cache for hourly total Pokemon stats: {e}")
        file_logger.error(f"Error accessing Redis cache for hourly total Pokemon stats: {e}")

    # If no Cache set
    console_logger.debug("Cache miss for hourly total Pokemon stats, fetching new data")
    file_logger.debug("Cache miss for hourly total Pokemon stats, fetching new data")
    result = get_task_result(query_tasks.query_hourly_pokemon_total_stats)
    serialized_result = json.dumps(result)

    # If cached result is None, update the cache
    ttl = seconds_until_next_hour()
    try:
        redis_client.set(cache_key, serialized_result, ex=ttl)
        console_logger.info("Cache set with new hourly total Pokemon stats")
        file_logger.info("Cache set with new hourly total Pokemon stats")
    except Exception as e:
        console_logger.error(f"Error setting Redis cache for hourly total Pokemon stats: {e}")
        file_logger.error(f"Error setting Redis cache for hourly total Pokemon stats: {e}")

    console_logger.info("Successfully obtained hourly total Pokemon stats")
    file_logger.info("Sucessfully obtained hourly total Pokemon stats")
    return JSONResponse(content=result)

@fastapi.get("/api/daily-total-pokemon-stats")
async def daily_total_pokemon_stats(request: Request, secret: str= Depends(validate_secret), _ip = Depends(validate_ip), _header = Depends(validate_secret_header)):
    console_logger.debug("Request received for daily total Pokemon stats")
    file_logger.debug("Request received for daily total Pokemon stats")

    cache_key = "daily-total-pokemon-stats"

    try:
        cached_result = redis_client.get(cache_key)
        if cached_result:
            # If cached result exists, decode it and return
            console_logger.info("Cache hit for daily total Pokemon stats")
            file_logger.info("Cache hit for daily total Pokemon stats")
            return JSONResponse(content=json.loads(cached_result.decode("utf-8")))
    except Exception as e:
        console_logger.error(f"Error accessing Redis cache for daily total Pokemon stats: {e}")
        file_logger.error(f"Error accessing Redis cache for daily total Pokemon stats: {e}")

    # If no Cache set
    console_logger.debug("Cache miss for daily total Pokemon stats, fetching new data")
    file_logger.debug("Cache miss for daily total Pokemon stats, fetching new data")
    result = get_task_result(query_tasks.query_daily_pokemon_total_stats)
    serialized_result = json.dumps(result)

    # If cached result is None, update the cache
    ttl = seconds_until_midnight()
    try:
        redis_client.set(cache_key, serialized_result, ex=ttl)
        console_logger.info("Cache set with new daily total Pokemon stats")
        file_logger.info("Cache set with new daily total Pokemon stats")
    except Exception as e:
        console_logger.error(f"Error setting Redis cache for daily total Pokemon stats: {e}")
        file_logger.error(f"Error setting Redis cache for daily total Pokemon stats: {e}")

    console_logger.info("Successfully obtained daily total Pokemon stats")
    file_logger.info("Sucessfully obtained daily total Pokemon stats")
    return JSONResponse(content=result)

@fastapi.get("/api/total-pokemon-stats")
async def total_pokemon_stats(request: Request, secret: str= Depends(validate_secret), _ip = Depends(validate_ip), _header = Depends(validate_secret_header)):
    console_logger.debug("Request received for total Pokemon stats")
    file_logger.debug("Request received for total Pokemon stats")

    cache_key = "total-pokemon-stats"

    try:
        cached_result = redis_client.get(cache_key)
        if cached_result:
            # If cached result exists, decode it and return
            console_logger.info("Cache hit for total Pokemon stats")
            file_logger.info("Cache hit for total Pokemon stats")
            return JSONResponse(content=json.loads(cached_result.decode("utf-8")))
    except Exception as e:
        console_logger.error(f"Error accessing Redis cache for total Pokemon stats: {e}")
        file_logger.error(f"Error accessing Redis cache for total Pokemon stats: {e}")

    # If no Cache set
    console_logger.debug("Cache miss for total Pokemon stats, fetching new data")
    file_logger.debug("Cache miss for total Pokemon stats, fetching new data")
    result = get_task_result(query_tasks.query_pokemon_total_stats)
    serialized_result = json.dumps(result)

    # If cached result is None, update the cache
    ttl = seconds_until_midnight()
    try:
        redis_client.set(cache_key, serialized_result, ex=ttl)
        console_logger.info("Cache set with new total Pokemon stats")
        file_logger.info("Cache set with new total Pokemon stats")
    except Exception as e:
        console_logger.error(f"Error setting Redis cache for total Pokemon stats: {e}")
        file_logger.error(f"Error setting Redis cache for total Pokemon stats: {e}")

    console_logger.info("Successfully obtained total Pokemon stats")
    file_logger.info("Sucessfully obtained total Pokemon stats")
    return JSONResponse(content=result)

# API Surge's
@fastapi.get("/api/surge-daily-stats")
async def surge_daily_pokemon_stats(request: Request, secret: str= Depends(validate_secret), _ip = Depends(validate_ip), _header = Depends(validate_secret_header)):
    console_logger.debug("Request received for Surge Daily Pokemon stats")
    file_logger.debug("Request received for Surge Daily Pokemon stats")

    cache_key = "surge-daily-pokemon-stats"

    try:
        cached_result = redis_client.get(cache_key)
        if cached_result:
            # If cached result exists, decode it and return
            console_logger.info("Cache hit for Surge Daily Pokemon stats")
            file_logger.info("Cache hit for Surge Daily Pokemon stats")
            return JSONResponse(content=json.loads(cached_result.decode("utf-8")))
    except Exception as e:
        console_logger.error(f"Error accessing Redis cache for Surge Daily Pokemon stats: {e}")
        file_logger.error(f"Error accessing Redis cache for Surge Daily Pokemon stats: {e}")

    # If no Cache set
    console_logger.debug("Cache miss for Surge Daily Pokemon stats, fetching new data")
    file_logger.debug("Cache miss for Surge Daily Pokemon stats, fetching new data")
    result = get_task_result(query_tasks.query_daily_surge_api_pokemon_stats)
    serialized_result = json.dumps(result)

    # If cached result is None, update the cache
    ttl = seconds_until_midnight()
    try:
        redis_client.set(cache_key, serialized_result, ex=ttl)
        console_logger.info("Cache set with new Surge Daily Pokemon stats")
        file_logger.info("Cache set with new Surge Daily Pokemon stats")
    except Exception as e:
        console_logger.error(f"Error setting Redis cache for Surge Daily Pokemon stats: {e}")
        file_logger.error(f"Error setting Redis cache for Surge Daily Pokemon stats: {e}")

    console_logger.info("Successfully obtained Surge Daily Pokemon stats")
    file_logger.info("Sucessfully obtained Surge Daily Pokemon stats")
    return JSONResponse(content=result)

@fastapi.get("/api/surge-weekly-stats")
async def surge_weekly_pokemon_stats(request: Request, secret: str= Depends(validate_secret), _ip = Depends(validate_ip), _header = Depends(validate_secret_header)):
    console_logger.debug("Request received for Surge Weekly Pokemon stats")
    file_logger.debug("Request received for Surge Weekly Pokemon stats")

    cache_key = "surge-weekly-pokemon-stats"

    try:
        cached_result = redis_client.get(cache_key)
        if cached_result:
            # If cached result exists, decode it and return
            console_logger.info("Cache hit for Surge Weekly Pokemon stats")
            file_logger.info("Cache hit for Surge Weekly Pokemon stats")
            return JSONResponse(content=json.loads(cached_result.decode("utf-8")))
    except Exception as e:
        console_logger.error(f"Error accessing Redis cache for Surge Weekly Pokemon stats: {e}")
        file_logger.error(f"Error accessing Redis cache for Surge Weekly Pokemon stats: {e}")

    # If no Cache set
    console_logger.debug("Cache miss for Surge Weekly Pokemon stats, fetching new data")
    file_logger.debug("Cache miss for Surge Weekly Pokemon stats, fetching new data")
    result = get_task_result(query_tasks.query_weekly_surge_api_pokemon_stats)
    serialized_result = json.dumps(result)

    # If cached result is None, update the cache
    ttl = seconds_until_next_week()
    try:
        redis_client.set(cache_key, serialized_result, ex=ttl)
        console_logger.info("Cache set with new Surge Weekly Pokemon stats")
        file_logger.info("Cache set with new Surge Weekly Pokemon stats")
    except Exception as e:
        console_logger.error(f"Error setting Redis cache for Surge Weekly Pokemon stats: {e}")
        file_logger.error(f"Error setting Redis cache for Surge Weekly Pokemon stats: {e}")

    console_logger.info("Successfully obtained Surge Weekly Pokemon stats")
    file_logger.info("Sucessfully obtained Surge Weekly Pokemon stats")
    return JSONResponse(content=result)

@fastapi.get("/api/surge-monthly-stats")
async def surge_monthly_pokemon_stats(request: Request, secret: str= Depends(validate_secret), _ip = Depends(validate_ip), _header = Depends(validate_secret_header)):
    console_logger.debug("Request received for Surge Monthly Pokemon stats")
    file_logger.debug("Request received for Surge Monthly Pokemon stats")

    cache_key = "surge-monthly-pokemon-stats"

    try:
        cached_result = redis_client.get(cache_key)
        if cached_result:
            # If cached result exists, decode it and return
            console_logger.info("Cache hit for Surge Monthly Pokemon stats")
            file_logger.info("Cache hit for Surge Monthly Pokemon stats")
            return JSONResponse(content=json.loads(cached_result.decode("utf-8")))
    except Exception as e:
        console_logger.error(f"Error accessing Redis cache for Surge Monthly Pokemon stats: {e}")
        file_logger.error(f"Error accessing Redis cache for Surge Monthly Pokemon stats: {e}")

    # If no Cache set
    console_logger.debug("Cache miss for Surge Monthly Pokemon stats, fetching new data")
    file_logger.debug("Cache miss for Surge Monthly Pokemon stats, fetching new data")
    result = get_task_result(query_tasks.query_monthly_surge_api_pokemon_stats)
    serialized_result = json.dumps(result)

    # If cached result is None, update the cache
    ttl = seconds_until_next_month()
    try:
        redis_client.set(cache_key, serialized_result, ex=ttl)
        console_logger.info("Cache set with new Surge Monthly Pokemon stats")
        file_logger.info("Cache set with new Surge Monthly Pokemon stats")
    except Exception as e:
        console_logger.error(f"Error setting Redis cache for Surge Monthly Pokemon stats: {e}")
        file_logger.error(f"Error setting Redis cache for Surge Monthly Pokemon stats: {e}")

    console_logger.info("Successfully obtained Surge Monthly Pokemon stats")
    file_logger.info("Sucessfully obtained Surge Monthly Pokemon stats")
    return JSONResponse(content=result)

# Quest Section
@fastapi.get("/api/daily-quest-grouped-stats")
async def daily_quest_grouped_stats(request: Request, secret: str= Depends(validate_secret), _ip = Depends(validate_ip), _header = Depends(validate_secret_header)):
    console_logger.debug("Request received for Daily Quest Grouped stats")
    file_logger.debug("Request received for Daily Quest Grouped stats")

    cache_key = "daily-quest-grouped-stats"

    try:
        cached_result = redis_client.get(cache_key)
        if cached_result:
            # If cached result exists, decode it and return
            console_logger.info("Cache hit for Daily Quest Grouped stats")
            file_logger.info("Cache hit for Daily Quest Grouped stats")
            return JSONResponse(content=json.loads(cached_result.decode("utf-8")))
    except Exception as e:
        console_logger.error(f"Error accessing Redis cache for Daily Quest Grouped stats: {e}")
        file_logger.error(f"Error accessing Redis cache for Daily Quest Grouped stats: {e}")

    # If no Cache set
    console_logger.debug("Cache miss for Daily Quest Grouped stats, fetching new data")
    file_logger.debug("Cache miss for Daily Quest Grouped stats, fetching new data")
    result = get_task_result(query_tasks.query_daily_quest_grouped_stats_api)
    serialized_result = json.dumps(result)

    # If cached result is None, update the cache
    ttl = seconds_until_fourpm()
    try:
        redis_client.set(cache_key, serialized_result, ex=ttl)
        console_logger.info("Cache set with new Daily Quest Grouped stats")
        file_logger.info("Cache set with new Daily Quest Grouped stats")
    except Exception as e:
        console_logger.error(f"Error setting Redis cache for Daily Quest Grouped stats: {e}")
        file_logger.error(f"Error setting Redis cache for Daily Quest Grouped stats: {e}")

    console_logger.info("Successfully obtained Daily Quest Grouped stats")
    file_logger.info("Sucessfully obtained Daily Quest Grouped stats")
    return JSONResponse(content=result)

@fastapi.get("/api/weekly-quest-grouped-stats")
async def weekly_quest_grouped_stats(request: Request, secret: str= Depends(validate_secret), _ip = Depends(validate_ip), _header = Depends(validate_secret_header)):
    console_logger.debug("Request received for Weekly Quest Grouped stats")
    file_logger.debug("Request received for Weekly Quest Grouped stats")

    cache_key = "weekly-quest-grouped-stats"

    try:
        cached_result = redis_client.get(cache_key)
        if cached_result:
            # If cached result exists, decode it and return
            console_logger.info("Cache hit for Weekly Quest Grouped stats")
            file_logger.info("Cache hit for Weekly Quest Grouped stats")
            return JSONResponse(content=json.loads(cached_result.decode("utf-8")))
    except Exception as e:
        console_logger.error(f"Error accessing Redis cache for Weekly Quest Grouped stats: {e}")
        file_logger.error(f"Error accessing Redis cache for Weekly Quest Grouped stats: {e}")

    # If no Cache set
    console_logger.debug("Cache miss for Weekly Quest Grouped stats, fetching new data")
    file_logger.debug("Cache miss for Weekly Quest Grouped stats, fetching new data")
    result = get_task_result(query_tasks.query_weekly_quest_grouped_stats_api)
    serialized_result = json.dumps(result)

    # If cached result is None, update the cache
    ttl = seconds_until_next_week_fourpm()
    try:
        redis_client.set(cache_key, serialized_result, ex=ttl)
        console_logger.info("Cache set with new Weekly Quest Grouped stats")
        file_logger.info("Cache set with new Weekly Quest Grouped stats")
    except Exception as e:
        console_logger.error(f"Error setting Redis cache for Weekly Quest Grouped stats: {e}")
        file_logger.error(f"Error setting Redis cache for Weekly Quest Grouped stats: {e}")

    console_logger.info("Successfully obtained Weekly Quest Grouped stats")
    file_logger.info("Sucessfully obtained Weekly Quest Grouped stats")
    return JSONResponse(content=result)

@fastapi.get("/api/monthly-quest-grouped-stats")
async def monthly_quest_grouped_stats(request: Request, secret: str= Depends(validate_secret), _ip = Depends(validate_ip), _header = Depends(validate_secret_header)):
    console_logger.debug("Request received for Monthly Quest Grouped stats")
    file_logger.debug("Request received for Monthly Quest Grouped stats")

    cache_key = "monthly-quest-grouped-stats"

    try:
        cached_result = redis_client.get(cache_key)
        if cached_result:
            # If cached result exists, decode it and return
            console_logger.info("Cache hit for Monthly Quest Grouped stats")
            file_logger.info("Cache hit for Monthly Quest Grouped stats")
            return JSONResponse(content=json.loads(cached_result.decode("utf-8")))
    except Exception as e:
        console_logger.error(f"Error accessing Redis cache for Monthly Quest Grouped stats: {e}")
        file_logger.error(f"Error accessing Redis cache for Monthly Quest Grouped stats: {e}")

    # If no Cache set
    console_logger.debug("Cache miss for Monthly Quest Grouped stats, fetching new data")
    file_logger.debug("Cache miss for Monthly Quest Grouped stats, fetching new data")
    result = get_task_result(query_tasks.query_monthly_quest_grouped_stats_api)
    serialized_result = json.dumps(result)

    # If cached result is None, update the cache
    ttl = seconds_until_next_month_fourpm()
    try:
        redis_client.set(cache_key, serialized_result, ex=ttl)
        console_logger.info("Cache set with new Monthly Quest Grouped stats")
        file_logger.info("Cache set with new Monthly Quest Grouped stats")
    except Exception as e:
        console_logger.error(f"Error setting Redis cache for Monthly Quest Grouped stats: {e}")
        file_logger.error(f"Error setting Redis cache for Monthly Quest Grouped stats: {e}")

    console_logger.info("Successfully obtained Monthly Quest Grouped stats")
    file_logger.info("Sucessfully obtained Monthly Quest Grouped stats")
    return JSONResponse(content=result)

@fastapi.get("/api/daily-quest-total-stats")
async def daily_quest_total_stats(request: Request, secret: str= Depends(validate_secret), _ip = Depends(validate_ip), _header = Depends(validate_secret_header)):
    console_logger.debug("Request received for Daily Quest Total stats")
    file_logger.debug("Request received for Daily Quest Total stats")

    cache_key = "daily-quest-total-stats"

    try:
        cached_result = redis_client.get(cache_key)
        if cached_result:
            # If cached result exists, decode it and return
            console_logger.info("Cache hit for Daily Quest Total stats")
            file_logger.info("Cache hit for Daily Quest Total stats")
            return JSONResponse(content=json.loads(cached_result.decode("utf-8")))
    except Exception as e:
        console_logger.error(f"Error accessing Redis cache for Daily Quest Total stats: {e}")
        file_logger.error(f"Error accessing Redis cache for Daily Quest Total stats: {e}")

    # If no Cache set
    console_logger.debug("Cache miss for Daily Quest Total stats, fetching new data")
    file_logger.debug("Cache miss for Daily Quest Total stats, fetching new data")
    result = get_task_result(query_tasks.query_daily_quest_total_stats_api)
    serialized_result = json.dumps(result)

    # If cached result is None, update the cache
    ttl = seconds_until_fourpm()
    try:
        redis_client.set(cache_key, serialized_result, ex=ttl)
        console_logger.info("Cache set with new Daily Quest Total stats")
        file_logger.info("Cache set with new Daily Quest Total stats")
    except Exception as e:
        console_logger.error(f"Error setting Redis cache for Daily Quest Total stats: {e}")
        file_logger.error(f"Error setting Redis cache for Daily Quest Total stats: {e}")

    console_logger.info("Successfully obtained Daily Quest Total stats")
    file_logger.info("Sucessfully obtained Daily Quest Total stats")
    return JSONResponse(content=result)

@fastapi.get("/api/total-quest-total-stats")
async def total_quest_total_stats(request: Request, secret: str= Depends(validate_secret), _ip = Depends(validate_ip), _header = Depends(validate_secret_header)):
    console_logger.debug("Request received for Total Quest Total stats")
    file_logger.debug("Request received for Total Quest Total stats")

    cache_key = "total-quest-total-stats"

    try:
        cached_result = redis_client.get(cache_key)
        if cached_result:
            # If cached result exists, decode it and return
            console_logger.info("Cache hit for Total Quest Total stats")
            file_logger.info("Cache hit for Total Quest Total stats")
            return JSONResponse(content=json.loads(cached_result.decode("utf-8")))
    except Exception as e:
        console_logger.error(f"Error accessing Redis cache for Total Quest Total stats: {e}")
        file_logger.error(f"Error accessing Redis cache for Total Quest Total stats: {e}")

    # If no Cache set
    console_logger.debug("Cache miss for Total Quest Total stats, fetching new data")
    file_logger.debug("Cache miss for Total Quest Total stats, fetching new data")
    result = get_task_result(query_tasks.query_total_quest_total_stats_api)
    serialized_result = json.dumps(result)

    # If cached result is None, update the cache
    ttl = seconds_until_fourpm()
    try:
        redis_client.set(cache_key, serialized_result, ex=ttl)
        console_logger.info("Cache set with new Total Quest Total stats")
        file_logger.info("Cache set with new Total Quest Total stats")
    except Exception as e:
        console_logger.error(f"Error setting Redis cache for Total Quest Total stats: {e}")
        file_logger.error(f"Error setting Redis cache for Total Quest Total stats: {e}")

    console_logger.info("Successfully obtained Total Quest Total stats")
    file_logger.info("Sucessfully obtained Total Quest Total stats")
    return JSONResponse(content=result)

# Raid Section
@fastapi.get("/api/daily-raid-grouped-stats")
async def daily_raid_grouped_stats(request: Request, secret: str= Depends(validate_secret), _ip = Depends(validate_ip), _header = Depends(validate_secret_header)):
    console_logger.debug("Request received for Daily Raid Grouped stats")
    file_logger.debug("Request received for Daily Raid Grouped stats")

    cache_key = "daily-raid-grouped-stats"

    try:
        cached_result = redis_client.get(cache_key)
        if cached_result:
            # If cached result exists, decode it and return
            console_logger.info("Cache hit for Daily Raid Grouped stats")
            file_logger.info("Cache hit for Daily Raid Grouped stats")
            return JSONResponse(content=json.loads(cached_result.decode("utf-8")))
    except Exception as e:
        console_logger.error(f"Error accessing Redis cache for Daily Raid Grouped stats: {e}")
        file_logger.error(f"Error accessing Redis cache for Daily Raid Grouped stats: {e}")

    # If no Cache set
    console_logger.debug("Cache miss for Daily Raid Grouped stats, fetching new data")
    file_logger.debug("Cache miss for Daily Raid Grouped stats, fetching new data")
    result = get_task_result(query_tasks.query_daily_raid_grouped_stats_api)
    serialized_result = json.dumps(result)

    # If cached result is None, update the cache
    ttl = seconds_until_midnight()
    try:
        redis_client.set(cache_key, serialized_result, ex=ttl)
        console_logger.info("Cache set with new Daily Raid Grouped stats")
        file_logger.info("Cache set with new Daily Raid Grouped stats")
    except Exception as e:
        console_logger.error(f"Error setting Redis cache for Daily Raid Grouped stats: {e}")
        file_logger.error(f"Error setting Redis cache for Daily Raid Grouped stats: {e}")

    console_logger.info("Successfully obtained Daily Raid Grouped stats")
    file_logger.info("Sucessfully obtained Daily Raid Grouped stats")
    return JSONResponse(content=result)

@fastapi.get("/api/weekly-raid-grouped-stats")
async def weekly_raid_grouped_stats(request: Request, secret: str= Depends(validate_secret), _ip = Depends(validate_ip), _header = Depends(validate_secret_header)):
    console_logger.debug("Request received for Weekly Raid Grouped stats")
    file_logger.debug("Request received for Weekly Raid Grouped stats")

    cache_key = "weekly-raid-grouped-stats"

    try:
        cached_result = redis_client.get(cache_key)
        if cached_result:
            # If cached result exists, decode it and return
            console_logger.info("Cache hit for Weekly Raid Grouped stats")
            file_logger.info("Cache hit for Weekly Raid Grouped stats")
            return JSONResponse(content=json.loads(cached_result.decode("utf-8")))
    except Exception as e:
        console_logger.error(f"Error accessing Redis cache for Weekly Raid Grouped stats: {e}")
        file_logger.error(f"Error accessing Redis cache for Weekly Raid Grouped stats: {e}")

    # If no Cache set
    console_logger.debug("Cache miss for Weekly Raid Grouped stats, fetching new data")
    file_logger.debug("Cache miss for Weekly Raid Grouped stats, fetching new data")
    result = get_task_result(query_tasks.query_weekly_raid_grouped_stats_api)
    serialized_result = json.dumps(result)

    # If cached result is None, update the cache
    ttl = seconds_until_next_week()
    try:
        redis_client.set(cache_key, serialized_result, ex=ttl)
        console_logger.info("Cache set with new Weekly Raid Grouped stats")
        file_logger.info("Cache set with new Weekly Raid Grouped stats")
    except Exception as e:
        console_logger.error(f"Error setting Redis cache for Weekly Raid Grouped stats: {e}")
        file_logger.error(f"Error setting Redis cache for Weekly Raid Grouped stats: {e}")

    console_logger.info("Successfully obtained Weekly Raid Grouped stats")
    file_logger.info("Sucessfully obtained Weekly Raid Grouped stats")
    return JSONResponse(content=result)

@fastapi.get("/api/monthly-raid-grouped-stats")
async def monthly_raid_grouped_stats(request: Request, secret: str= Depends(validate_secret), _ip = Depends(validate_ip), _header = Depends(validate_secret_header)):
    console_logger.debug("Request received for Monthly Raid Grouped stats")
    file_logger.debug("Request received for Monthly Raid Grouped stats")

    cache_key = "monthly-raid-grouped-stats"

    try:
        cached_result = redis_client.get(cache_key)
        if cached_result:
            # If cached result exists, decode it and return
            console_logger.info("Cache hit for Monthly Raid Grouped stats")
            file_logger.info("Cache hit for Monthly Raid Grouped stats")
            return JSONResponse(content=json.loads(cached_result.decode("utf-8")))
    except Exception as e:
        console_logger.error(f"Error accessing Redis cache for Monthly Raid Grouped stats: {e}")
        file_logger.error(f"Error accessing Redis cache for Monthly Raid Grouped stats: {e}")

    # If no Cache set
    console_logger.debug("Cache miss for Monthly Raid Grouped stats, fetching new data")
    file_logger.debug("Cache miss for Monthly Raid Grouped stats, fetching new data")
    result = get_task_result(query_tasks.query_monthly_raid_grouped_stats_api)
    serialized_result = json.dumps(result)

    # If cached result is None, update the cache
    ttl = seconds_until_next_month()
    try:
        redis_client.set(cache_key, serialized_result, ex=ttl)
        console_logger.info("Cache set with new Monthly Raid Grouped stats")
        file_logger.info("Cache set with new Monthly Raid Grouped stats")
    except Exception as e:
        console_logger.error(f"Error setting Redis cache for Monthly Raid Grouped stats: {e}")
        file_logger.error(f"Error setting Redis cache for Monthly Raid Grouped stats: {e}")

    console_logger.info("Successfully obtained Monthly Raid Grouped stats")
    file_logger.info("Sucessfully obtained Monthly Raid Grouped stats")
    return JSONResponse(content=result)

@fastapi.get("/api/hourly-raid-total-stats")
async def hourly_raid_total_stats(request: Request, secret: str= Depends(validate_secret), _ip = Depends(validate_ip), _header = Depends(validate_secret_header)):
    console_logger.debug("Request received for Hourly Raid Total stats")
    file_logger.debug("Request received for Hourly Raid Total stats")

    cache_key = "hourly-raid-total-stats"

    try:
        cached_result = redis_client.get(cache_key)
        if cached_result:
            # If cached result exists, decode it and return
            console_logger.info("Cache hit for Hourly Raid Total stats")
            file_logger.info("Cache hit for Hourly Raid Total stats")
            return JSONResponse(content=json.loads(cached_result.decode("utf-8")))
    except Exception as e:
        console_logger.error(f"Error accessing Redis cache for Hourly Raid Total stats: {e}")
        file_logger.error(f"Error accessing Redis cache for Hourly Raid Total stats: {e}")

    # If no Cache set
    console_logger.debug("Cache miss for Hourly Raid Total stats, fetching new data")
    file_logger.debug("Cache miss for Hourly Raid Total stats, fetching new data")
    result = get_task_result(query_tasks.query_hourly_raid_total_stats_api)
    serialized_result = json.dumps(result)

    # If cached result is None, update the cache
    ttl = seconds_until_next_hour()
    try:
        redis_client.set(cache_key, serialized_result, ex=ttl)
        console_logger.info("Cache set with new Hourly Raid Total stats")
        file_logger.info("Cache set with new Hourly Raid Total stats")
    except Exception as e:
        console_logger.error(f"Error setting Redis cache for Hourly Raid Total stats: {e}")
        file_logger.error(f"Error setting Redis cache for Hourly Raid Total stats: {e}")

    console_logger.info("Successfully obtained Hourly Raid Total stats")
    file_logger.info("Sucessfully obtained Hourly Raid Total stats")
    return JSONResponse(content=result)

@fastapi.get("/api/daily-raid-total-stats")
async def daily_raid_total_stats(request: Request, secret: str= Depends(validate_secret), _ip = Depends(validate_ip), _header = Depends(validate_secret_header)):
    console_logger.debug("Request received for Daily Raid Total stats")
    file_logger.debug("Request received for Daily Raid Total stats")

    cache_key = "daily-raid-total-stats"

    try:
        cached_result = redis_client.get(cache_key)
        if cached_result:
            # If cached result exists, decode it and return
            console_logger.info("Cache hit for Daily Raid Total stats")
            file_logger.info("Cache hit for Daily Raid Total stats")
            return JSONResponse(content=json.loads(cached_result.decode("utf-8")))
    except Exception as e:
        console_logger.error(f"Error accessing Redis cache for Daily Raid Total stats: {e}")
        file_logger.error(f"Error accessing Redis cache for Daily Raid Total stats: {e}")

    # If no Cache set
    console_logger.debug("Cache miss for Daily Raid Total stats, fetching new data")
    file_logger.debug("Cache miss for Daily Raid Total stats, fetching new data")
    result = get_task_result(query_tasks.query_daily_raid_total_stats_api)
    serialized_result = json.dumps(result)

    # If cached result is None, update the cache
    ttl = seconds_until_midnight()
    try:
        redis_client.set(cache_key, serialized_result, ex=ttl)
        console_logger.info("Cache set with new Daily Raid Total stats")
        file_logger.info("Cache set with new Daily Raid Total stats")
    except Exception as e:
        console_logger.error(f"Error setting Redis cache for Daily Raid Total stats: {e}")
        file_logger.error(f"Error setting Redis cache for Daily Raid Total stats: {e}")

    console_logger.info("Successfully obtained Daily Raid Total stats")
    file_logger.info("Sucessfully obtained Daily Raid Total stats")
    return JSONResponse(content=result)

@fastapi.get("/api/total-raid-total-stats")
async def total_raid_total_stats(request: Request, secret: str= Depends(validate_secret), _ip = Depends(validate_ip), _header = Depends(validate_secret_header)):
    console_logger.debug("Request received for Total Raid Total stats")
    file_logger.debug("Request received for Total Raid Total stats")

    cache_key = "total-raid-total-stats"

    try:
        cached_result = redis_client.get(cache_key)
        if cached_result:
            # If cached result exists, decode it and return
            console_logger.info("Cache hit for Total Raid Total stats")
            file_logger.info("Cache hit for Total Raid Total stats")
            return JSONResponse(content=json.loads(cached_result.decode("utf-8")))
    except Exception as e:
        console_logger.error(f"Error accessing Redis cache for Total Raid Total stats: {e}")
        file_logger.error(f"Error accessing Redis cache for Total Raid Total stats: {e}")

    # If no Cache set
    console_logger.debug("Cache miss for Total Raid Total stats, fetching new data")
    file_logger.debug("Cache miss for Total Raid Total stats, fetching new data")
    result = get_task_result(query_tasks.query_total_raid_total_stats_api)
    serialized_result = json.dumps(result)

    # If cached result is None, update the cache
    ttl = seconds_until_midnight()
    try:
        redis_client.set(cache_key, serialized_result, ex=ttl)
        console_logger.info("Cache set with new Total Raid Total stats")
        file_logger.info("Cache set with new Total Raid Total stats")
    except Exception as e:
        console_logger.error(f"Error setting Redis cache for Total Raid Total stats: {e}")
        file_logger.error(f"Error setting Redis cache for Total Raid Total stats: {e}")

    console_logger.info("Successfully obtained Total Raid Total stats")
    file_logger.info("Sucessfully obtained Total Raid Total stats")
    return JSONResponse(content=result)

# Invasion Section
@fastapi.get("/api/daily-invasion-grouped-stats")
async def daily_invasion_grouped_stats(request: Request, secret: str= Depends(validate_secret), _ip = Depends(validate_ip), _header = Depends(validate_secret_header)):
    console_logger.debug("Request received for Daily Invasion Grouped stats")
    file_logger.debug("Request received for Daily Invasion Grouped stats")

    cache_key = "daily-invasion-grouped-stats"

    try:
        cached_result = redis_client.get(cache_key)
        if cached_result:
            # If cached result exists, decode it and return
            console_logger.info("Cache hit for Daily Invasion Grouped stats")
            file_logger.info("Cache hit for Daily Invasion Grouped stats")
            return JSONResponse(content=json.loads(cached_result.decode("utf-8")))
    except Exception as e:
        console_logger.error(f"Error accessing Redis cache for Daily Invasion Grouped stats: {e}")
        file_logger.error(f"Error accessing Redis cache for Daily Invasion Grouped stats: {e}")

    # If no Cache set
    console_logger.debug("Cache miss for Daily Invasion Grouped stats, fetching new data")
    file_logger.debug("Cache miss for Daily Invasion Grouped stats, fetching new data")
    result = get_task_result(query_tasks.query_daily_invasion_grouped_stats_api)
    serialized_result = json.dumps(result)

    # If cached result is None, update the cache
    ttl = seconds_until_midnight()
    try:
        redis_client.set(cache_key, serialized_result, ex=ttl)
        console_logger.info("Cache set with new Daily Invasion Grouped stats")
        file_logger.info("Cache set with new Daily Invasion Grouped stats")
    except Exception as e:
        console_logger.error(f"Error setting Redis cache for Daily Invasion Grouped stats: {e}")
        file_logger.error(f"Error setting Redis cache for Daily Invasion Grouped stats: {e}")

    console_logger.info("Successfully obtained Daily Invasion Grouped stats")
    file_logger.info("Sucessfully obtained Daily Invasion Grouped stats")
    return JSONResponse(content=result)

@fastapi.get("/api/weekly-invasion-grouped-stats")
async def weekly_invasion_grouped_stats(request: Request, secret: str= Depends(validate_secret), _ip = Depends(validate_ip), _header = Depends(validate_secret_header)):
    console_logger.debug("Request received for Weekly Invasion Grouped stats")
    file_logger.debug("Request received for Weekly Invasion Grouped stats")

    cache_key = "weekly-invasion-grouped-stats"

    try:
        cached_result = redis_client.get(cache_key)
        if cached_result:
            # If cached result exists, decode it and return
            console_logger.info("Cache hit for Weekly Invasion Grouped stats")
            file_logger.info("Cache hit for Weekly Invasion Grouped stats")
            return JSONResponse(content=json.loads(cached_result.decode("utf-8")))
    except Exception as e:
        console_logger.error(f"Error accessing Redis cache for Weekly Invasion Grouped stats: {e}")
        file_logger.error(f"Error accessing Redis cache for Weekly Invasion Grouped stats: {e}")

    # If no Cache set
    console_logger.debug("Cache miss for Weekly Invasion Grouped stats, fetching new data")
    file_logger.debug("Cache miss for Weekly Invasion Grouped stats, fetching new data")
    result = get_task_result(query_tasks.query_weekly_invasion_grouped_stats_api)
    serialized_result = json.dumps(result)

    # If cached result is None, update the cache
    ttl = seconds_until_next_week()
    try:
        redis_client.set(cache_key, serialized_result, ex=ttl)
        console_logger.info("Cache set with new Weekly Invasion Grouped stats")
        file_logger.info("Cache set with new Weekly Invasion Grouped stats")
    except Exception as e:
        console_logger.error(f"Error setting Redis cache for Weekly Invasion Grouped stats: {e}")
        file_logger.error(f"Error setting Redis cache for Weekly Invasion Grouped stats: {e}")

    console_logger.info("Successfully obtained Weekly Invasion Grouped stats")
    file_logger.info("Sucessfully obtained Weekly Invasion Grouped stats")
    return JSONResponse(content=result)

@fastapi.get("/api/monthly-invasion-grouped-stats")
async def monthly_invasion_grouped_stats(request: Request, secret: str= Depends(validate_secret), _ip = Depends(validate_ip), _header = Depends(validate_secret_header)):
    console_logger.debug("Request received for Monthly Invasion Grouped stats")
    file_logger.debug("Request received for Monthly Invasion Grouped stats")

    cache_key = "monthly-invasion-grouped-stats"

    try:
        cached_result = redis_client.get(cache_key)
        if cached_result:
            # If cached result exists, decode it and return
            console_logger.info("Cache hit for Monthly Invasion Grouped stats")
            file_logger.info("Cache hit for Monthly Invasion Grouped stats")
            return JSONResponse(content=json.loads(cached_result.decode("utf-8")))
    except Exception as e:
        console_logger.error(f"Error accessing Redis cache for Monthly Invasion Grouped stats: {e}")
        file_logger.error(f"Error accessing Redis cache for Monthly Invasion Grouped stats: {e}")

    # If no Cache set
    console_logger.debug("Cache miss for  stats, fetching new data")
    file_logger.debug("Cache miss for stats, fetching new data")
    result = get_task_result(query_tasks.query_monthly_invasion_grouped_stats_api)
    serialized_result = json.dumps(result)

    # If cached result is None, update the cache
    ttl = seconds_until_next_month()
    try:
        redis_client.set(cache_key, serialized_result, ex=ttl)
        console_logger.info("Cache set with new Monthly Invasion Grouped stats")
        file_logger.info("Cache set with new Monthly Invasion Grouped stats")
    except Exception as e:
        console_logger.error(f"Error setting Redis cache for Monthly Invasion Grouped stats: {e}")
        file_logger.error(f"Error setting Redis cache for Monthly Invasion Grouped stats: {e}")

    console_logger.info("Successfully obtained Monthly Invasion Grouped stats")
    file_logger.info("Sucessfully obtained Monthly Invasion Grouped stats")
    return JSONResponse(content=result)

@fastapi.get("/api/hourly-invasion-total-stats")
async def hourly_invasion_total_stats(request: Request, secret: str= Depends(validate_secret), _ip = Depends(validate_ip), _header = Depends(validate_secret_header)):
    console_logger.debug("Request received for Hourly Invasion Total stats")
    file_logger.debug("Request received for Hourly Invasion Total stats")

    cache_key = "hourly-invasion-total-stats"

    try:
        cached_result = redis_client.get(cache_key)
        if cached_result:
            # If cached result exists, decode it and return
            console_logger.info("Cache hit for Hourly Invasion Total stats")
            file_logger.info("Cache hit for Hourly Invasion Total stats")
            return JSONResponse(content=json.loads(cached_result.decode("utf-8")))
    except Exception as e:
        console_logger.error(f"Error accessing Redis cache for Hourly Invasion Total stats: {e}")
        file_logger.error(f"Error accessing Redis cache for Hourly Invasion Total stats: {e}")

    # If no Cache set
    console_logger.debug("Cache miss for Hourly Invasion Total stats, fetching new data")
    file_logger.debug("Cache miss for Hourly Invasion Total stats, fetching new data")
    result = get_task_result(query_tasks.query_hourly_invasions_total_stats_api)
    serialized_result = json.dumps(result)

    # If cached result is None, update the cache
    ttl = seconds_until_next_hour()
    try:
        redis_client.set(cache_key, serialized_result, ex=ttl)
        console_logger.info("Cache set with new Hourly Invasion Total stats")
        file_logger.info("Cache set with new Hourly Invasion Total stats")
    except Exception as e:
        console_logger.error(f"Error setting Redis cache for Hourly Invasion Total stats: {e}")
        file_logger.error(f"Error setting Redis cache for Hourly Invasion Total stats: {e}")

    console_logger.info("Successfully obtained Hourly Invasion Total stats")
    file_logger.info("Sucessfully obtained Hourly Invasion Total stats")
    return JSONResponse(content=result)

@fastapi.get("/api/daily-invasion-total-stats")
async def daily_invasion_total_stats(request: Request, secret: str= Depends(validate_secret), _ip = Depends(validate_ip), _header = Depends(validate_secret_header)):
    console_logger.debug("Request received for Daily Invasion Total stats")
    file_logger.debug("Request received for Daily Invasion Total stats")

    cache_key = "daily-invasion-total-stats"

    try:
        cached_result = redis_client.get(cache_key)
        if cached_result:
            # If cached result exists, decode it and return
            console_logger.info("Cache hit for Daily Invasion Total stats")
            file_logger.info("Cache hit for Daily Invasion Total stats")
            return JSONResponse(content=json.loads(cached_result.decode("utf-8")))
    except Exception as e:
        console_logger.error(f"Error accessing Redis cache for Daily Invasion Total stats: {e}")
        file_logger.error(f"Error accessing Redis cache for Daily Invasion Total stats: {e}")

    # If no Cache set
    console_logger.debug("Cache miss for Daily Invasion Total stats, fetching new data")
    file_logger.debug("Cache miss for Daily Invasion Total stats, fetching new data")
    result = get_task_result(query_tasks.query_daily_invasions_total_stats_api)
    serialized_result = json.dumps(result)

    # If cached result is None, update the cache
    ttl = seconds_until_midnight()
    try:
        redis_client.set(cache_key, serialized_result, ex=ttl)
        console_logger.info("Cache set with new Daily Invasion Total stats")
        file_logger.info("Cache set with new Daily Invasion Total stats")
    except Exception as e:
        console_logger.error(f"Error setting Redis cache for Daily Invasion Total stats: {e}")
        file_logger.error(f"Error setting Redis cache for Daily Invasion Total stats: {e}")

    console_logger.info("Successfully obtained Daily Invasion Total stats")
    file_logger.info("Sucessfully obtained Daily Invasion Total stats")
    return JSONResponse(content=result)

@fastapi.get("/api/total-invasion-total-stats")
async def total_invasion_total_stats(request: Request, secret: str= Depends(validate_secret), _ip = Depends(validate_ip), _header = Depends(validate_secret_header)):
    console_logger.debug("Request received for Total Invasion Total stats")
    file_logger.debug("Request received for Total Invasion Total stats")

    cache_key = "total-invasion-total-stats"

    try:
        cached_result = redis_client.get(cache_key)
        if cached_result:
            # If cached result exists, decode it and return
            console_logger.info("Cache hit for  stats")
            file_logger.info("Cache hit for  stats")
            return JSONResponse(content=json.loads(cached_result.decode("utf-8")))
    except Exception as e:
        console_logger.error(f"Error accessing Redis cache for Total Invasion Total stats: {e}")
        file_logger.error(f"Error accessing Redis cache for Total Invasion Total stats: {e}")

    # If no Cache set
    console_logger.debug("Cache miss for Total Invasion Total stats, fetching new data")
    file_logger.debug("Cache miss for Total Invasion Total stats, fetching new data")
    result = get_task_result(query_tasks.query_total_invasions_total_stats_api)
    serialized_result = json.dumps(result)

    # If cached result is None, update the cache
    ttl = seconds_until_midnight()
    try:
        redis_client.set(cache_key, serialized_result, ex=ttl)
        console_logger.info("Cache set with new Total Invasion Total stats")
        file_logger.info("Cache set with new Total Invasion Total stats")
    except Exception as e:
        console_logger.error(f"Error setting Redis cache for Total Invasion Total stats: {e}")
        file_logger.error(f"Error setting Redis cache for Total Invasion Total stats: {e}")

    console_logger.info("Successfully obtained Total Invasion Total stats")
    file_logger.info("Sucessfully obtained Total Invasion Total stats")
    return JSONResponse(content=result)

# Pokemon TTH Section
@fastapi.get("/api/hourly-pokemon-tth-stats")
async def hourly_pokemon_tth_stats(request: Request, secret: str= Depends(validate_secret), _ip = Depends(validate_ip), _header = Depends(validate_secret_header)):
    console_logger.debug("Request received for Hourly Pokemon TTH stats")
    file_logger.debug("Request received for Hourly Pokemon TTH stats")

    cache_key = "hourly-pokemon-tth-stats"

    try:
        cached_result = redis_client.get(cache_key)
        if cached_result:
            # If cached result exists, decode it and return
            console_logger.info("Cache hit for Hourly Pokemon TTH stats")
            file_logger.info("Cache hit for Hourly Pokemon TTH stats")
            return JSONResponse(content=json.loads(cached_result.decode("utf-8")))
    except Exception as e:
        console_logger.error(f"Error accessing Redis cache for Hourly Pokemon TTH stats: {e}")
        file_logger.error(f"Error accessing Redis cache for Hourly Pokemon TTH stats: {e}")

    # If no Cache set
    console_logger.debug("Cache miss for Hourly Pokemon TTH stats, fetching new data")
    file_logger.debug("Cache miss for Hourly Pokemon TTH stats, fetching new data")
    result = get_task_result(query_tasks.query_hourly_pokemon_tth_stats_api)
    serialized_result = json.dumps(result)

    # If cached result is None, update the cache
    ttl = seconds_until_next_hour()
    try:
        redis_client.set(cache_key, serialized_result, ex=ttl)
        console_logger.info("Cache set with new Hourly Pokemon TTH stats")
        file_logger.info("Cache set with new Hourly Pokemon TTH stats")
    except Exception as e:
        console_logger.error(f"Error setting Redis cache for Hourly Pokemon TTH stats: {e}")
        file_logger.error(f"Error setting Redis cache for Hourly Pokemon TTH stats: {e}")

    console_logger.info("Successfully obtained Hourly Pokemon TTH stats")
    file_logger.info("Sucessfully obtained Hourly Pokemon TTH stats")
    return JSONResponse(content=result)

@fastapi.get("/api/daily-pokemon-tth-stats")
async def daily_pokemon_tth_stats(request: Request, secret: str= Depends(validate_secret), _ip = Depends(validate_ip), _header = Depends(validate_secret_header)):
    console_logger.debug("Request received for Daily Pokemon TTH stats")
    file_logger.debug("Request received for Daily Pokemon TTH stats")

    cache_key = "daily-pokemon-tth-stats"

    try:
        cached_result = redis_client.get(cache_key)
        if cached_result:
            # If cached result exists, decode it and return
            console_logger.info("Cache hit for Daily Pokemon TTH stats")
            file_logger.info("Cache hit for Daily Pokemon TTH stats")
            return JSONResponse(content=json.loads(cached_result.decode("utf-8")))
    except Exception as e:
        console_logger.error(f"Error accessing Redis cache for Daily Pokemon TTH stats: {e}")
        file_logger.error(f"Error accessing Redis cache for Daily Pokemon TTH stats: {e}")

    # If no Cache set
    console_logger.debug("Cache miss for Daily Pokemon TTH stats, fetching new data")
    file_logger.debug("Cache miss for Daily Pokemon TTH stats, fetching new data")
    result = get_task_result(query_tasks.query_daily_pokemon_tth_stats_api)
    serialized_result = json.dumps(result)

    # If cached result is None, update the cache
    ttl = seconds_until_midnight()
    try:
        redis_client.set(cache_key, serialized_result, ex=ttl)
        console_logger.info("Cache set with new Daily Pokemon TTH stats")
        file_logger.info("Cache set with new Daily Pokemon TTH stats")
    except Exception as e:
        console_logger.error(f"Error setting Redis cache for Daily Pokemon TTH stats: {e}")
        file_logger.error(f"Error setting Redis cache for Daily Pokemon TTH stats: {e}")

    console_logger.info("Successfully obtained Daily Pokemon TTH stats")
    file_logger.info("Sucessfully obtained Daily Pokemon TTH stats")
    return JSONResponse(content=result)

# API format for VictoriaMetrics/Prometheus with Redis Cache
@fastapi.get("/metrics/daily-area-pokemon")
async def daily_area_pokemon_metrics(request: Request, secret: str = Depends(validate_secret), _ip = Depends(validate_ip), _header = Depends(validate_secret_header)):
    cache_key = "metrics:daily-area-pokemon"

    try:
        # Attempt to retrieve cached metrics
        cached_metrics = redis_client.get(cache_key)
        if cached_metrics:
            console_logger.info("Cache hit for daily grouped area Pokemon metrics")
            file_logger.info("Cache hit for daily grouped area Pokemon metrics")
            return Response(content=cached_metrics.decode("utf-8"), media_type="text/plain")
    except Exception as e:
        console_logger.error(f"Error accessing Redis cache for daily grouped area Pokemon metrics: {e}")
        file_logger.error(f"Error accessing Redis cache for daily grouped area Pokemon metrics: {e}")

    try:
        # If cache miss, fetch data and format for VictoriaMetrics
        daily_area_stats = get_task_result(query_tasks.query_daily_pokemon_grouped_stats)
        formatted_daily_area_stats = format_results_to_victoria(daily_area_stats, 'psyduck_daily_area_stats')
        console_logger.info("Cache miss. Fetched data for daily grouped area Pokemon metrics")
        file_logger.info("Cache miss. Fetched data for daily grouped area Pokemon metrics")

        # Combine formatted metrics for area stats
        daily_area_pokemon_prometheus_metrics = '\n'.join([formatted_daily_area_stats])

        # Cache the newly fetched and formatted metrics with a dynamic expiration
        ttl = seconds_until_midnight()
        redis_client.set(cache_key, daily_area_pokemon_prometheus_metrics, ex=ttl)

        console_logger.info("Successfully set cache for daily area Pokemon metrics")
        return Response(content=daily_area_pokemon_prometheus_metrics, media_type="text/plain")
    except Exception as e:
        console_logger.error(f"Error processing metrics: {e}")
        file_logger.error(f"Error processing metrics: {e}")
        return Response(content=f"Error generating metrics: {e}", media_type="text/plain", status_code=500)

@fastapi.get("/metrics/weekly-area-pokemon")
async def weekly_area_pokemon_metrics(request: Request, secret: str = Depends(validate_secret), _ip = Depends(validate_ip), _header = Depends(validate_secret_header)):
    cache_key = "metrics:weekly-area-pokemon"

    try:
        # Attempt to retrieve cached metrics
        cached_metrics = redis_client.get(cache_key)
        if cached_metrics:
            console_logger.info("Cache hit for weekly grouped area Pokemon metrics")
            file_logger.info("Cache hit for weekly grouped area Pokemon metrics")
            return Response(content=cached_metrics.decode("utf-8"), media_type="text/plain")
    except Exception as e:
        console_logger.error(f"Error accessing Redis cache for weekly grouped area Pokemon metrics: {e}")
        file_logger.error(f"Error accessing Redis cache for weekly grouped area Pokemon metrics: {e}")

    try:
        # Fetch data for metrics
        weekly_area_stats = get_task_result(query_tasks.query_weekly_pokemon_grouped_stats)
        console_logger.info(f"Cache Miss. Fetched weekly grouped pokemon API task sucessfuly")
        file_logger.info(f"Cache Miss. Fetched weekly grouped pokemon API tasks sucessfuly")

        # Format the result
        formatted_weekly_area_stats = format_results_to_victoria(weekly_area_stats, 'psyduck_weekly_area_stats')
        console_logger.debug(f"Formatted weekly area stats for VictoriaMetrics")
        file_logger.debug(f"Formatted weekly area stats for VictoriaMetrics")

        # Combine formatted metrics for grouped stats
        weekly_area_pokemon_prometheus_metrics = '\n'.join([
            formatted_weekly_area_stats
        ])

        ttl = seconds_until_next_week()
        redis_client.set(cache_key, weekly_area_pokemon_prometheus_metrics, ex=ttl)
        # Return as plain text
        console_logger.info(f"Successfully set cache for grouped-weekly API for VictoriaMetrics")
        return Response(content=weekly_area_pokemon_prometheus_metrics, media_type="text/plain")
    except Exception as e:
        console_logger.error(f"Error generating grouped metrics: {e}")
        file_logger.error(f"Error generating grouped metrics: {e}")
        return Response(content=f"Error generating grouped metrics: {e}", media_type="text/plain", status_code=500)

@fastapi.get("/metrics/monthly-area-pokemon")
async def monthly_area_pokemon_metrics(request: Request, secret: str = Depends(validate_secret), _ip = Depends(validate_ip), _header = Depends(validate_secret_header)):
    cache_key = "metrics:monthly-area-pokemon"

    try:
        # Attempt to retrieve cached metrics
        cached_metrics = redis_client.get(cache_key)
        if cached_metrics:
            console_logger.info("Cache hit for monthly grouped area Pokemon metrics")
            file_logger.info("Cache hit for monthly grouped area Pokemon metrics")
            return Response(content=cached_metrics.decode("utf-8"), media_type="text/plain")
    except Exception as e:
        console_logger.error(f"Error accessing Redis cache for monthly grouped area Pokemon metrics: {e}")
        file_logger.error(f"Error accessing Redis cache for monthly grouped area Pokemon metrics: {e}")

    try:
        # Fetch data for metrics
        monthly_area_stats = get_task_result(query_tasks.query_monthly_pokemon_grouped_stats)
        console_logger.info(f"Cache Miss. Fetched monthly grouped pokemon API task sucessfuly")
        file_logger.info(f"Cache Miss. Fetched monthly grouped pokemon API tasks sucessfuly")

        # Format the result
        formatted_monthly_area_stats = format_results_to_victoria(monthly_area_stats, 'psyduck_monthly_area_stats')
        console_logger.debug(f"Formatted monthly area stats for VictoriaMetrics")
        file_logger.debug(f"Formatted monthly area stats for VictoriaMetrics")

        # Combine formatted metrics for grouped stats
        monthly_area_pokemon_prometheus_metrics = '\n'.join([
            formatted_monthly_area_stats
        ])

        ttl = seconds_until_next_month()
        redis_client.set(cache_key, monthly_area_pokemon_prometheus_metrics, ex=ttl)
        # Return as plain text
        console_logger.info(f"Successfully set cache for grouped-monthly API for VictoriaMetrics")
        return Response(content=monthly_area_pokemon_prometheus_metrics, media_type="text/plain")
    except Exception as e:
        console_logger.error(f"Error generating grouped metrics: {e}")
        file_logger.error(f"Error generating grouped metrics: {e}")
        return Response(content=f"Error generating grouped metrics: {e}", media_type="text/plain", status_code=500)

@fastapi.get("/metrics/total-hourly-pokemon")
async def total_hourly_pokemon_metrics(request: Request, secret: str = Depends(validate_secret), _ip = Depends(validate_ip), _header = Depends(validate_secret_header)):
    cache_key = "metrics:total-hourly-pokemon"

    try:
        # Attempt to retrieve cached metrics
        cached_metrics = redis_client.get(cache_key)
        if cached_metrics:
            console_logger.info("Cache hit for hourly total area Pokemon metrics")
            file_logger.info("Cache hit for hourly total area Pokemon metrics")
            return Response(content=cached_metrics.decode("utf-8"), media_type="text/plain")
    except Exception as e:
        console_logger.error(f"Error accessing Redis cache for hourly totald area Pokemon metrics: {e}")
        file_logger.error(f"Error accessing Redis cache for hourly total area Pokemon metrics: {e}")

    try:
        # Fetch data for metrics
        hourly_total_stats = get_task_result(query_tasks.query_hourly_pokemon_total_stats)
        console_logger.info(f"Cache Miss. Fetched hourly total pokemon API task sucessfuly")
        file_logger.info(f"Cache Miss. Fetched hourly total pokemon API tasks sucessfuly")

        # Format each result set
        formatted_hourly_total_stats = format_results_to_victoria(hourly_total_stats, 'psyduck_hourly_total_stats')
        console_logger.debug(f"Formatted hourly total stats for VictoriaMetrics")
        file_logger.debug(f"Formatted hourly total stats for VictoriaMetrics")

        # Combine all formatted metrics
        total_hourly_pokemon_prometheus_metrics = '\n'.join([
            formatted_hourly_total_stats
        ])

        ttl = seconds_until_next_hour()
        redis_client.set(cache_key, total_hourly_pokemon_prometheus_metrics, ex=ttl)
        # Return as plain text
        console_logger.info(f"Successfully set cache for hourly total pokemon API for VictoriaMetrics")
        return Response(content=total_hourly_pokemon_prometheus_metrics, media_type="text/plain")
    except Exception as e:
        console_logger.error(f"Error generating metrics: {e}")
        file_logger.error(f"Error generating metrics: {e}")
        return Response(content=f"Error generating metrics: {e}", media_type="text/plain", status_code=500)

@fastapi.get("/metrics/total-daily-pokemon")
async def total_daily_pokemon_metrics(request: Request, secret: str = Depends(validate_secret), _ip = Depends(validate_ip), _header = Depends(validate_secret_header)):
    cache_key = "metris:total-daily-pokemon"

    try:
        # Attempt to retrieve cached metrics
        cached_metrics = redis_client.get(cache_key)
        if cached_metrics:
            console_logger.info("Cache hit for daily total Pokemon metrics")
            file_logger.info("Cache hit for daily total Pokemon metrics")
            return Response(content=cached_metrics.decode("utf-8"), media_type="text/plain")
    except Exception as e:
        console_logger.error(f"Error accessing Redis cache for total Pokemon metrics: {e}")
        file_logger.error(f"Error accessing Redis cache for total Pokemon metrics: {e}")


    try:
        # Fetch data for metrics
        daily_total_stats = get_task_result(query_tasks.query_daily_pokemon_total_stats)
        console_logger.info(f"Cache Miss. Fetched daily total pokemon API task sucessfuly")
        file_logger.info(f"Cache Miss. Fetched daily total pokemon API tasks sucessfuly")

        # Format each result set
        formatted_daily_total_stats = format_results_to_victoria(daily_total_stats, 'psyduck_daily_total_stats')
        console_logger.debug(f"Formatted daily total stats for VictoriaMetrics")
        file_logger.debug(f"Formatted daily total stats for VictoriaMetrics")

        # Combine all formatted metrics
        total_daily_pokemon_prometheus_metrics = '\n'.join([
            formatted_daily_total_stats
        ])

        ttl = seconds_until_midnight()
        redis_client.set(cache_key, total_daily_pokemon_prometheus_metrics, ex=ttl)
        # Return as plain text
        console_logger.info(f"Successfully set cache for daily total pokemon API for VictoriaMetrics")
        return Response(content=total_daily_pokemon_prometheus_metrics, media_type="text/plain")
    except Exception as e:
        console_logger.error(f"Error generating metrics: {e}")
        file_logger.error(f"Error generating metrics: {e}")
        return Response(content=f"Error generating metrics: {e}", media_type="text/plain", status_code=500)

@fastapi.get("/metrics/total-pokemon")
async def total_pokemon_metrics(request: Request, secret: str = Depends(validate_secret), _ip = Depends(validate_ip), _header = Depends(validate_secret_header)):
    cache_key = "metrics:total-pokemon"

    try:
        # Attempt to retrieve cached metrics
        cached_metrics = redis_client.get(cache_key)
        if cached_metrics:
            console_logger.info("Cache hit for total Pokemon metrics")
            file_logger.info("Cache hit for total Pokemon metrics")
            return Response(content=cached_metrics.decode("utf-8"), media_type="text/plain")
    except Exception as e:
        console_logger.error(f"Error accessing Redis cache for total Pokemon metrics: {e}")
        file_logger.error(f"Error accessing Redis cache for total Pokemon metrics: {e}")


    try:
        # Fetch data for metrics
        total_stats = get_task_result(query_tasks.query_pokemon_total_stats)
        console_logger.info(f"Cache Miss. Fetched total pokemon API task sucessfuly")
        file_logger.info(f"Cache Miss. Fetched total pokemon API tasks sucessfuly")

        # Format each result set
        formatted_total_stats = format_results_to_victoria(total_stats, 'psyduck_total_stats')
        console_logger.debug(f"Formatted total stats for VictoriaMetrics")
        file_logger.debug(f"Formatted total stats for VictoriaMetrics")

        # Combine all formatted metrics
        total_pokemon_prometheus_metrics = '\n'.join([
            formatted_total_stats
        ])

        ttl = seconds_until_midnight()
        redis_client.set(cache_key, total_pokemon_prometheus_metrics, ex=ttl)
        # Return as plain text
        console_logger.info(f"Successfully set cache for total pokemon API for VictoriaMetrics")
        return Response(content=total_pokemon_prometheus_metrics, media_type="text/plain")
    except Exception as e:
        console_logger.error(f"Error generating metrics: {e}")
        file_logger.error(f"Error generating metrics: {e}")
        return Response(content=f"Error generating metrics: {e}", media_type="text/plain", status_code=500)

@fastapi.get("/metrics/surge-daily-stats")
async def surge_daily_metrics(request: Request, secret: str = Depends(validate_secret), _ip = Depends(validate_ip), _header = Depends(validate_secret_header)):
    cache_key = "metrics:surge-daily-stats"

    try:
        # Attempt to retrieve cached metrics
        cached_metrics = redis_client.get(cache_key)
        if cached_metrics:
            console_logger.info("Cache hit for surge daily Pokemon metrics")
            file_logger.info("Cache hit for surge daily Pokemon metrics")
            return Response(content=cached_metrics.decode("utf-8"), media_type="text/plain")
    except Exception as e:
        console_logger.error(f"Error accessing Redis cache for surge daily Pokemon metrics: {e}")
        file_logger.error(f"Error accessing Redis cache for surge daily Pokemon metrics: {e}")

    try:
        # Fetch data for metrics
        surge_daily_stats = get_task_result(query_tasks.query_daily_surge_api_pokemon_stats)
        console_logger.info(f"Cache Miss. Fetched surge daily API task sucessfuly")
        file_logger.info(f"Cache Miss. Fetched surge daily API tasks sucessfuly")

        # Format each result set
        formatted_surge_daily_stats = format_results_to_victoria_by_hour(surge_daily_stats, 'psyduck_surge_daily')
        console_logger.debug(f"Formatted surge daily stats for VictoriaMetrics")
        file_logger.debug(f"Formatted surge daily stats for VictoriaMetrics")

        # Combine all formatted metrics
        surge_daily_prometheus_metrics = '\n'.join([
            formatted_surge_daily_stats
        ])

        ttl = seconds_until_midnight()
        redis_client.set(cache_key, surge_daily_prometheus_metrics, ex=ttl)
        # Return as plain text
        console_logger.info(f"Successfully set cache for surge daily Pokemon API for VictoriaMetrics")
        return Response(content=surge_daily_prometheus_metrics, media_type="text/plain")
    except Exception as e:
        console_logger.error(f"Error generating metrics: {e}")
        file_logger.error(f"Error generating metrics: {e}")
        return Response(content=f"Error generating metrics: {e}", media_type="text/plain", status_code=500)

@fastapi.get("/metrics/surge-weekly-stats")
async def surge_weekly_metrics(request: Request, secret: str = Depends(validate_secret), _ip = Depends(validate_ip), _header = Depends(validate_secret_header)):
    cache_key = "metrics:surge-weekly-stats"

    try:
        # Attempt to retrieve cached metrics
        cached_metrics = redis_client.get(cache_key)
        if cached_metrics:
            console_logger.info("Cache hit for weekly Surge Pokemon metrics")
            file_logger.info("Cache hit for weekly Surge Pokemon metrics")
            return Response(content=cached_metrics.decode("utf-8"), media_type="text/plain")
    except Exception as e:
        console_logger.error(f"Error accessing Redis cache for weekly Surge Pokemon metrics: {e}")
        file_logger.error(f"Error accessing Redis cache for weekly Surge Pokemon metrics: {e}")

    try:
        # Fetch data for metrics
        surge_weekly_stats = get_task_result(query_tasks.query_weekly_surge_api_pokemon_stats)
        console_logger.info(f"Cache Miss. Fetched surge weekly API task sucessfuly")
        file_logger.info(f"Cache Miss. Fetched surge weekly API tasks sucessfuly")

        # Format each result set
        formatted_surge_weekly_stats = format_results_to_victoria_by_hour(surge_weekly_stats, 'psyduck_surge_weekly')
        console_logger.debug(f"Formatted surge weekly stats for VictoriaMetrics")
        file_logger.debug(f"Formatted surge weekly stats for VictoriaMetrics")

        # Combine all formatted metrics
        surge_weekly_prometheus_metrics = '\n'.join([
            formatted_surge_weekly_stats
        ])

        ttl = seconds_until_next_week()
        redis_client.set(cache_key, surge_weekly_prometheus_metrics, ex=ttl)
        # Return as plain text
        console_logger.info(f"Successfully set cache for Surge Weekly Pokemon for VictoriaMetrics")
        return Response(content=surge_weekly_prometheus_metrics, media_type="text/plain")
    except Exception as e:
        console_logger.error(f"Error generating metrics: {e}")
        file_logger.error(f"Error generating metrics: {e}")
        return Response(content=f"Error generating metrics: {e}", media_type="text/plain", status_code=500)

@fastapi.get("/metrics/surge-monthly-stats")
async def surge_monthly_metrics(request: Request, secret: str = Depends(validate_secret), _ip = Depends(validate_ip), _header = Depends(validate_secret_header)):
    cache_key = "metrics:surge-monthly-stats"

    try:
        # Attempt to retrieve cached metrics
        cached_metrics = redis_client.get(cache_key)
        if cached_metrics:
            console_logger.info("Cache hit for monthly Surge Pokemon metrics")
            file_logger.info("Cache hit for monthly Surge Pokemon metrics")
            return Response(content=cached_metrics.decode("utf-8"), media_type="text/plain")
    except Exception as e:
        console_logger.error(f"Error accessing Redis cache for monthly Surge Pokemon metrics: {e}")
        file_logger.error(f"Error accessing Redis cache for monthly Surge Pokemon metrics: {e}")

    try:
        # Fetch data for metrics
        surge_monthly_stats = get_task_result(query_tasks.query_monthly_surge_api_pokemon_stats)
        console_logger.info(f"Cache Miss. Fetched surge monthly API task sucessfuly")
        file_logger.info(f"Cache Miss. Fetched surge monthly API tasks sucessfuly")

        # Format each result set
        formatted_surge_monthly_stats = format_results_to_victoria_by_hour(surge_monthly_stats, 'psyduck_surge_monthly')
        console_logger.debug(f"Formatted surge monthly stats for VictoriaMetrics")
        file_logger.debug(f"Formatted surge monthly stats for VictoriaMetrics")

        # Combine all formatted metrics
        surge_monthly_prometheus_metrics = '\n'.join([
            formatted_surge_monthly_stats
        ])

        ttl = seconds_until_next_month()
        redis_client.set(cache_key, surge_monthly_prometheus_metrics, ex=ttl)
        # Return as plain text
        console_logger.info(f"Successfully set cache for monthly Surge Pokemon API for VictoriaMetrics")
        return Response(content=surge_monthly_prometheus_metrics, media_type="text/plain")
    except Exception as e:
        console_logger.error(f"Error generating metrics: {e}")
        file_logger.error(f"Error generating metrics: {e}")
        return Response(content=f"Error generating metrics: {e}", media_type="text/plain", status_code=500)

# Quest Section
@fastapi.get("/metrics/daily-quest-grouped-stats")
async def daily_quest_grouped_stats_metrics(request: Request, secret: str = Depends(validate_secret), _ip = Depends(validate_ip), _header = Depends(validate_secret_header)):
    cache_key = "metrics:daily-quest-grouped-stats"

    try:
        # Attempt to retrieve cached metrics
        cached_metrics = redis_client.get(cache_key)
        if cached_metrics:
            console_logger.info("Cache hit for Daily Quest Grouped metrics")
            file_logger.info("Cache hit for Daily Quest Grouped metrics")
            return Response(content=cached_metrics.decode("utf-8"), media_type="text/plain")
    except Exception as e:
        console_logger.error(f"Error accessing Redis cache for Daily Quest Grouped metrics: {e}")
        file_logger.error(f"Error accessing Redis cache for Daily Quest Grouped metrics: {e}")

    try:
        # Fetch data for metrics
        daily_quest_grouped_stats = get_task_result(query_tasks.query_daily_quest_grouped_stats_api)
        console_logger.info(f"Cache Miss. Fetched Daily Quest Grouped API task sucessfuly")
        file_logger.info(f"Cache Miss. Fetched Daily Quest Grouped API tasks sucessfuly")

        # Format each result set
        formatted_daily_quest_grouped_stats = format_results_to_victoria(daily_quest_grouped_stats, 'psyduck_daily_quest_grouped_stats')
        console_logger.debug(f"Formatted Daily Quest Grouped stats for VictoriaMetrics")
        file_logger.debug(f"Formatted Daily Quest Grouped stats for VictoriaMetrics")

        # Combine all formatted metrics
        daily_quest_grouped_stats_prometheus_metrics = '\n'.join([
            formatted_daily_quest_grouped_stats
        ])

        ttl = seconds_until_fourpm()
        redis_client.set(cache_key, daily_quest_grouped_stats_prometheus_metrics, ex=ttl)
        # Return as plain text
        console_logger.info(f"Successfully set cache for Daily Quest Grouped API for VictoriaMetrics")
        return Response(content=daily_quest_grouped_stats_prometheus_metrics, media_type="text/plain")
    except Exception as e:
        console_logger.error(f"Error generating metrics: {e}")
        file_logger.error(f"Error generating metrics: {e}")
        return Response(content=f"Error generating metrics: {e}", media_type="text/plain", status_code=500)

@fastapi.get("/metrics/weekly-quest-grouped-stats")
async def weekly_quest_grouped_stats_metrics(request: Request, secret: str = Depends(validate_secret), _ip = Depends(validate_ip), _header = Depends(validate_secret_header)):
    cache_key = "metrics:weekly-quest-grouped-stats"

    try:
        # Attempt to retrieve cached metrics
        cached_metrics = redis_client.get(cache_key)
        if cached_metrics:
            console_logger.info("Cache hit for Weekly Quest Grouped metrics")
            file_logger.info("Cache hit for Weekly Quest Grouped metrics")
            return Response(content=cached_metrics.decode("utf-8"), media_type="text/plain")
    except Exception as e:
        console_logger.error(f"Error accessing Redis cache for Weekly Quest Grouped metrics: {e}")
        file_logger.error(f"Error accessing Redis cache for Weekly Quest Grouped metrics: {e}")

    try:
        # Fetch data for metrics
        weekly_quest_grouped_stats = get_task_result(query_tasks.query_weekly_quest_grouped_stats_api)
        console_logger.info(f"Cache Miss. Fetched Weekly Quest Grouped API task sucessfuly")
        file_logger.info(f"Cache Miss. Fetched Weekly Quest Grouped API tasks sucessfuly")

        # Format each result set
        formatted_weekly_quest_grouped_stats = format_results_to_victoria(weekly_quest_grouped_stats, 'psyduck_weekly_quest_grouped_stats')
        console_logger.debug(f"Formatted Weekly Quest Grouped stats for VictoriaMetrics")
        file_logger.debug(f"Formatted Weekly Quest Grouped stats for VictoriaMetrics")

        # Combine all formatted metrics
        weekly_quest_grouped_stats_prometheus_metrics = '\n'.join([
            formatted_weekly_quest_grouped_stats
        ])

        ttl = seconds_until_next_week_fourpm()
        redis_client.set(cache_key, weekly_quest_grouped_stats_prometheus_metrics, ex=ttl)
        # Return as plain text
        console_logger.info(f"Successfully set cache for Weekly Quest Grouped API for VictoriaMetrics")
        return Response(content=weekly_quest_grouped_stats_prometheus_metrics, media_type="text/plain")
    except Exception as e:
        console_logger.error(f"Error generating metrics: {e}")
        file_logger.error(f"Error generating metrics: {e}")
        return Response(content=f"Error generating metrics: {e}", media_type="text/plain", status_code=500)

@fastapi.get("/metrics/monthly-quest-grouped-stats")
async def monthly_quest_grouped_stats_metrics(request: Request, secret: str = Depends(validate_secret), _ip = Depends(validate_ip), _header = Depends(validate_secret_header)):
    cache_key = "metrics:monthly-quest-grouped-stats"

    try:
        # Attempt to retrieve cached metrics
        cached_metrics = redis_client.get(cache_key)
        if cached_metrics:
            console_logger.info("Cache hit for Monthly Quest Grouped metrics")
            file_logger.info("Cache hit for Monthly Quest Grouped metrics")
            return Response(content=cached_metrics.decode("utf-8"), media_type="text/plain")
    except Exception as e:
        console_logger.error(f"Error accessing Redis cache for Monthly Quest Grouped metrics: {e}")
        file_logger.error(f"Error accessing Redis cache for Monthly Quest Grouped metrics: {e}")

    try:
        # Fetch data for metrics
        monthly_quest_grouped_stats = get_task_result(query_tasks.query_monthly_quest_grouped_stats_api)
        console_logger.info(f"Cache Miss. Fetched Monthly Quest Grouped API task sucessfuly")
        file_logger.info(f"Cache Miss. Fetched Monthly Quest Grouped API tasks sucessfuly")

        # Format each result set
        formatted_monthly_quest_grouped_stats = format_results_to_victoria(monthly_quest_grouped_stats, 'psyduck_monthly_quest_grouped_stats')
        console_logger.debug(f"Formatted Monthly Quest Grouped stats for VictoriaMetrics")
        file_logger.debug(f"Formatted Monthly Quest Grouped stats for VictoriaMetrics")

        # Combine all formatted metrics
        monthly_quest_grouped_stats_prometheus_metrics = '\n'.join([
            formatted_monthly_quest_grouped_stats
        ])

        ttl = seconds_until_next_month_fourpm()
        redis_client.set(cache_key, monthly_quest_grouped_stats_prometheus_metrics, ex=ttl)
        # Return as plain text
        console_logger.info(f"Successfully set cache for Monthly Quest Grouped API for VictoriaMetrics")
        return Response(content=monthly_quest_grouped_stats_prometheus_metrics, media_type="text/plain")
    except Exception as e:
        console_logger.error(f"Error generating metrics: {e}")
        file_logger.error(f"Error generating metrics: {e}")
        return Response(content=f"Error generating metrics: {e}", media_type="text/plain", status_code=500)

@fastapi.get("/metrics/daily-quest-total-stats")
async def daily_quest_total_stats_metrics(request: Request, secret: str = Depends(validate_secret), _ip = Depends(validate_ip), _header = Depends(validate_secret_header)):
    cache_key = "metrics:daily-quest-total-stats"

    try:
        # Attempt to retrieve cached metrics
        cached_metrics = redis_client.get(cache_key)
        if cached_metrics:
            console_logger.info("Cache hit for Daily Quest Total metrics")
            file_logger.info("Cache hit for Daily Quest Total metrics")
            return Response(content=cached_metrics.decode("utf-8"), media_type="text/plain")
    except Exception as e:
        console_logger.error(f"Error accessing Redis cache for Daily Quest Total metrics: {e}")
        file_logger.error(f"Error accessing Redis cache for Daily Quest Total metrics: {e}")

    try:
        # Fetch data for metrics
        daily_quest_total_stats = get_task_result(query_tasks.query_daily_quest_total_stats_api)
        console_logger.info(f"Cache Miss. Fetched Daily Quest Total API task sucessfuly")
        file_logger.info(f"Cache Miss. Fetched Daily Quest Total API tasks sucessfuly")

        # Format each result set
        formatted_daily_quest_total_stats = format_results_to_victoria(daily_quest_total_stats, 'psyduck_daily_quest_total_stats')
        console_logger.debug(f"Formatted Daily Quest Total stats for VictoriaMetrics")
        file_logger.debug(f"Formatted Daily Quest Total stats for VictoriaMetrics")

        # Combine all formatted metrics
        daily_quest_total_stats_prometheus_metrics = '\n'.join([
            formatted_daily_quest_total_stats
        ])

        ttl = seconds_until_fourpm()
        redis_client.set(cache_key, daily_quest_total_stats_prometheus_metrics, ex=ttl)
        # Return as plain text
        console_logger.info(f"Successfully set cache for Daily Quest Total API for VictoriaMetrics")
        return Response(content=daily_quest_total_stats_prometheus_metrics, media_type="text/plain")
    except Exception as e:
        console_logger.error(f"Error generating metrics: {e}")
        file_logger.error(f"Error generating metrics: {e}")
        return Response(content=f"Error generating metrics: {e}", media_type="text/plain", status_code=500)

@fastapi.get("/metrics/total-quest-total-stats")
async def total_quest_total_stats_metrics(request: Request, secret: str = Depends(validate_secret), _ip = Depends(validate_ip), _header = Depends(validate_secret_header)):
    cache_key = "metrics:total-quest-total-stats"

    try:
        # Attempt to retrieve cached metrics
        cached_metrics = redis_client.get(cache_key)
        if cached_metrics:
            console_logger.info("Cache hit for Total Quest Total metrics")
            file_logger.info("Cache hit for Total Quest Total metrics")
            return Response(content=cached_metrics.decode("utf-8"), media_type="text/plain")
    except Exception as e:
        console_logger.error(f"Error accessing Redis cache for Total Quest Total metrics: {e}")
        file_logger.error(f"Error accessing Redis cache for Total Quest Total metrics: {e}")

    try:
        # Fetch data for metrics
        total_quest_total_stats = get_task_result(query_tasks.query_total_quest_total_stats_api)
        console_logger.info(f"Cache Miss. Fetched Total Quest Total API task sucessfuly")
        file_logger.info(f"Cache Miss. Fetched Total Quest Total API tasks sucessfuly")

        # Format each result set
        formatted_total_quest_total_stats = format_results_to_victoria(total_quest_total_stats, 'psyduck_total_quest_total_stats')
        console_logger.debug(f"Formatted Total Quest Total stats for VictoriaMetrics")
        file_logger.debug(f"Formatted Total Quest Total stats for VictoriaMetrics")

        # Combine all formatted metrics
        total_quest_total_stats_prometheus_metrics = '\n'.join([
            formatted_total_quest_total_stats
        ])

        ttl = seconds_until_fourpm()
        redis_client.set(cache_key, total_quest_total_stats_prometheus_metrics, ex=ttl)
        # Return as plain text
        console_logger.info(f"Successfully set cache for Total Quest Total API for VictoriaMetrics")
        return Response(content=total_quest_total_stats_prometheus_metrics, media_type="text/plain")
    except Exception as e:
        console_logger.error(f"Error generating metrics: {e}")
        file_logger.error(f"Error generating metrics: {e}")
        return Response(content=f"Error generating metrics: {e}", media_type="text/plain", status_code=500)

# Raid Section
@fastapi.get("/metrics/daily-raid-grouped-stats")
async def daily_raid_grouped_stats_metrics(request: Request, secret: str = Depends(validate_secret), _ip = Depends(validate_ip), _header = Depends(validate_secret_header)):
    cache_key = "metrics:daily-raid-grouped-stats"

    try:
        # Attempt to retrieve cached metrics
        cached_metrics = redis_client.get(cache_key)
        if cached_metrics:
            console_logger.info("Cache hit for Daily Raid Grouped metrics")
            file_logger.info("Cache hit for Daily Raid Grouped metrics")
            return Response(content=cached_metrics.decode("utf-8"), media_type="text/plain")
    except Exception as e:
        console_logger.error(f"Error accessing Redis cache for Daily Raid Grouped metrics: {e}")
        file_logger.error(f"Error accessing Redis cache for Daily Raid Grouped metrics: {e}")

    try:
        # Fetch data for metrics
        daily_raid_grouped_stats = get_task_result(query_tasks.query_daily_raid_grouped_stats_api)
        console_logger.info(f"Cache Miss. Fetched Daily Raid Grouped API task sucessfuly")
        file_logger.info(f"Cache Miss. Fetched Daily Raid Grouped API tasks sucessfuly")

        # Format each result set
        formatted_daily_raid_grouped_stats = format_results_to_victoria(daily_raid_grouped_stats, 'psyduck_daily_raid_grouped_stats')
        console_logger.debug(f"Formatted Daily Raid Grouped stats for VictoriaMetrics")
        file_logger.debug(f"Formatted Daily Raid Grouped stats for VictoriaMetrics")

        # Combine all formatted metrics
        daily_raid_grouped_stats_prometheus_metrics = '\n'.join([
            formatted_daily_raid_grouped_stats
        ])

        ttl = seconds_until_midnight()
        redis_client.set(cache_key, daily_raid_grouped_stats_prometheus_metrics, ex=ttl)
        # Return as plain text
        console_logger.info(f"Successfully set cache for Daily Raid Grouped API for VictoriaMetrics")
        return Response(content=daily_raid_grouped_stats_prometheus_metrics, media_type="text/plain")
    except Exception as e:
        console_logger.error(f"Error generating metrics: {e}")
        file_logger.error(f"Error generating metrics: {e}")
        return Response(content=f"Error generating metrics: {e}", media_type="text/plain", status_code=500)

@fastapi.get("/metrics/weekly-raid-grouped-stats")
async def weekly_raid_grouped_stats_metrics(request: Request, secret: str = Depends(validate_secret), _ip = Depends(validate_ip), _header = Depends(validate_secret_header)):
    cache_key = "metrics:weekly-raid-grouped-stats"

    try:
        # Attempt to retrieve cached metrics
        cached_metrics = redis_client.get(cache_key)
        if cached_metrics:
            console_logger.info("Cache hit for Weekly Raid Grouped metrics")
            file_logger.info("Cache hit for Weekly Raid Grouped metrics")
            return Response(content=cached_metrics.decode("utf-8"), media_type="text/plain")
    except Exception as e:
        console_logger.error(f"Error accessing Redis cache for Weekly Raid Grouped metrics: {e}")
        file_logger.error(f"Error accessing Redis cache for Weekly Raid Grouped metrics: {e}")

    try:
        # Fetch data for metrics
        weekly_raid_grouped_stats = get_task_result(query_tasks.query_weekly_raid_grouped_stats_api)
        console_logger.info(f"Cache Miss. Fetched Weekly Raid Grouped API task sucessfuly")
        file_logger.info(f"Cache Miss. Fetched Weekly Raid Grouped API tasks sucessfuly")

        # Format each result set
        formatted_weekly_raid_grouped_stats = format_results_to_victoria(weekly_raid_grouped_stats, 'psyduck_weekly_raid_grouped_stats')
        console_logger.debug(f"Formatted Weekly Raid Grouped stats for VictoriaMetrics")
        file_logger.debug(f"Formatted Weekly Raid Grouped stats for VictoriaMetrics")

        # Combine all formatted metrics
        weekly_raid_grouped_stats_prometheus_metrics = '\n'.join([
            formatted_weekly_raid_grouped_stats
        ])

        ttl = seconds_until_next_week()
        redis_client.set(cache_key, weekly_raid_grouped_stats_prometheus_metrics, ex=ttl)
        # Return as plain text
        console_logger.info(f"Successfully set cache for Weekly Raid Grouped API for VictoriaMetrics")
        return Response(content=weekly_raid_grouped_stats_prometheus_metrics, media_type="text/plain")
    except Exception as e:
        console_logger.error(f"Error generating metrics: {e}")
        file_logger.error(f"Error generating metrics: {e}")
        return Response(content=f"Error generating metrics: {e}", media_type="text/plain", status_code=500)

@fastapi.get("/metrics/monthly-raid-grouped-stats")
async def monthly_raid_grouped_stats_metrics(request: Request, secret: str = Depends(validate_secret), _ip = Depends(validate_ip), _header = Depends(validate_secret_header)):
    cache_key = "metrics:monthly-raid-grouped-stats"

    try:
        # Attempt to retrieve cached metrics
        cached_metrics = redis_client.get(cache_key)
        if cached_metrics:
            console_logger.info("Cache hit for Monthly Raid Grouped metrics")
            file_logger.info("Cache hit for Monthly Raid Grouped metrics")
            return Response(content=cached_metrics.decode("utf-8"), media_type="text/plain")
    except Exception as e:
        console_logger.error(f"Error accessing Redis cache for Monthly Raid Grouped metrics: {e}")
        file_logger.error(f"Error accessing Redis cache for Monthly Raid Grouped metrics: {e}")

    try:
        # Fetch data for metrics
        monthly_raid_grouped_stats = get_task_result(query_tasks.query_monthly_raid_grouped_stats_api)
        console_logger.info(f"Cache Miss. Fetched Monthly Raid Grouped API task sucessfuly")
        file_logger.info(f"Cache Miss. Fetched Monthly Raid Grouped API tasks sucessfuly")

        # Format each result set
        formatted_monthly_raid_grouped_stats = format_results_to_victoria(monthly_raid_grouped_stats, 'psyduck_monthly_raid_grouped_stats')
        console_logger.debug(f"Formatted Monthly Raid Grouped stats for VictoriaMetrics")
        file_logger.debug(f"Formatted Monthly Raid Grouped stats for VictoriaMetrics")

        # Combine all formatted metrics
        monthly_raid_grouped_stats_prometheus_metrics = '\n'.join([
            formatted_monthly_raid_grouped_stats
        ])

        ttl = seconds_until_next_month()
        redis_client.set(cache_key, monthly_raid_grouped_stats_prometheus_metrics, ex=ttl)
        # Return as plain text
        console_logger.info(f"Successfully set cache for Monthly Raid Grouped API for VictoriaMetrics")
        return Response(content=monthly_raid_grouped_stats_prometheus_metrics, media_type="text/plain")
    except Exception as e:
        console_logger.error(f"Error generating metrics: {e}")
        file_logger.error(f"Error generating metrics: {e}")
        return Response(content=f"Error generating metrics: {e}", media_type="text/plain", status_code=500)

@fastapi.get("/metrics/hourly-raid-total-stats")
async def hourly_raid_total_stats_metrics(request: Request, secret: str = Depends(validate_secret), _ip = Depends(validate_ip), _header = Depends(validate_secret_header)):
    cache_key = "metrics:hourly-raid-total-stats"

    try:
        # Attempt to retrieve cached metrics
        cached_metrics = redis_client.get(cache_key)
        if cached_metrics:
            console_logger.info("Cache hit for Hourly Raid Total metrics")
            file_logger.info("Cache hit for Hourly Raid Total metrics")
            return Response(content=cached_metrics.decode("utf-8"), media_type="text/plain")
    except Exception as e:
        console_logger.error(f"Error accessing Redis cache for Hourly Raid Total metrics: {e}")
        file_logger.error(f"Error accessing Redis cache for Hourly Raid Total metrics: {e}")

    try:
        # Fetch data for metrics
        hourly_raid_total_stats = get_task_result(query_tasks.query_hourly_raid_total_stats_api)
        console_logger.info(f"Cache Miss. Fetched Hourly Raid Total API task sucessfuly")
        file_logger.info(f"Cache Miss. Fetched Hourly Raid Total API tasks sucessfuly")

        # Format each result set
        formatted_hourly_raid_total_stats = format_results_to_victoria(hourly_raid_total_stats, 'psyduck_hourly_raid_total_stats')
        console_logger.debug(f"Formatted Hourly Raid Total stats for VictoriaMetrics")
        file_logger.debug(f"Formatted Hourly Raid Total stats for VictoriaMetrics")

        # Combine all formatted metrics
        hourly_raid_total_stats_prometheus_metrics = '\n'.join([
            formatted_hourly_raid_total_stats
        ])

        ttl = seconds_until_next_hour()
        redis_client.set(cache_key, hourly_raid_total_stats_prometheus_metrics, ex=ttl)
        # Return as plain text
        console_logger.info(f"Successfully set cache for Hourly Raid Total API for VictoriaMetrics")
        return Response(content=hourly_raid_total_stats_prometheus_metrics, media_type="text/plain")
    except Exception as e:
        console_logger.error(f"Error generating metrics: {e}")
        file_logger.error(f"Error generating metrics: {e}")
        return Response(content=f"Error generating metrics: {e}", media_type="text/plain", status_code=500)

@fastapi.get("/metrics/daily-raid-total-stats")
async def daily_raid_total_stats_metrics(request: Request, secret: str = Depends(validate_secret), _ip = Depends(validate_ip), _header = Depends(validate_secret_header)):
    cache_key = "metrics:daily-raid-total-stats"

    try:
        # Attempt to retrieve cached metrics
        cached_metrics = redis_client.get(cache_key)
        if cached_metrics:
            console_logger.info("Cache hit for Daily Raid Total metrics")
            file_logger.info("Cache hit for Daily Raid Total metrics")
            return Response(content=cached_metrics.decode("utf-8"), media_type="text/plain")
    except Exception as e:
        console_logger.error(f"Error accessing Redis cache for Daily Raid Total metrics: {e}")
        file_logger.error(f"Error accessing Redis cache for Daily Raid Total metrics: {e}")

    try:
        # Fetch data for metrics
        daily_raid_total_stats = get_task_result(query_tasks.query_daily_raid_total_stats_api)
        console_logger.info(f"Cache Miss. Fetched Daily Raid Total API task sucessfuly")
        file_logger.info(f"Cache Miss. Fetched Daily Raid Total API tasks sucessfuly")

        # Format each result set
        formatted_daily_raid_total_stats = format_results_to_victoria(daily_raid_total_stats, 'psyduck_daily_raid_total_stats')
        console_logger.debug(f"Formatted Daily Raid Total stats for VictoriaMetrics")
        file_logger.debug(f"Formatted Daily Raid Total stats for VictoriaMetrics")

        # Combine all formatted metrics
        daily_raid_total_stats_prometheus_metrics = '\n'.join([
            formatted_daily_raid_total_stats
        ])

        ttl = seconds_until_midnight()
        redis_client.set(cache_key, daily_raid_total_stats_prometheus_metrics, ex=ttl)
        # Return as plain text
        console_logger.info(f"Successfully set cache for Daily Raid Total API for VictoriaMetrics")
        return Response(content=daily_raid_total_stats_prometheus_metrics, media_type="text/plain")
    except Exception as e:
        console_logger.error(f"Error generating metrics: {e}")
        file_logger.error(f"Error generating metrics: {e}")
        return Response(content=f"Error generating metrics: {e}", media_type="text/plain", status_code=500)

@fastapi.get("/metrics/total-raid-total-stats")
async def total_raid_total_stats_metrics(request: Request, secret: str = Depends(validate_secret), _ip = Depends(validate_ip), _header = Depends(validate_secret_header)):
    cache_key = "metrics:total-raid-total-stats"

    try:
        # Attempt to retrieve cached metrics
        cached_metrics = redis_client.get(cache_key)
        if cached_metrics:
            console_logger.info("Cache hit for Total Raid Total metrics")
            file_logger.info("Cache hit for Total Raid Total metrics")
            return Response(content=cached_metrics.decode("utf-8"), media_type="text/plain")
    except Exception as e:
        console_logger.error(f"Error accessing Redis cache for Total Raid Total metrics: {e}")
        file_logger.error(f"Error accessing Redis cache for Total Raid Total metrics: {e}")

    try:
        # Fetch data for metrics
        total_raid_total_stats = get_task_result(query_tasks.query_total_raid_total_stats_api)
        console_logger.info(f"Cache Miss. Fetched Total Raid Total API task sucessfuly")
        file_logger.info(f"Cache Miss. Fetched Total Raid Total API tasks sucessfuly")

        # Format each result set
        formatted_total_raid_total_stats = format_results_to_victoria(total_raid_total_stats, 'psyduck_total_raid_total_stats')
        console_logger.debug(f"Formatted Total Raid Total stats for VictoriaMetrics")
        file_logger.debug(f"Formatted Total Raid Total stats for VictoriaMetrics")

        # Combine all formatted metrics
        total_raid_total_stats_prometheus_metrics = '\n'.join([
            formatted_total_raid_total_stats
        ])

        ttl = seconds_until_midnight()
        redis_client.set(cache_key, total_raid_total_stats_prometheus_metrics, ex=ttl)
        # Return as plain text
        console_logger.info(f"Successfully set cache for Total Raid Total API for VictoriaMetrics")
        return Response(content=total_raid_total_stats_prometheus_metrics, media_type="text/plain")
    except Exception as e:
        console_logger.error(f"Error generating metrics: {e}")
        file_logger.error(f"Error generating metrics: {e}")
        return Response(content=f"Error generating metrics: {e}", media_type="text/plain", status_code=500)

# Invasion Section
@fastapi.get("/metrics/daily-invasion-grouped-stats")
async def daily_invasion_grouped_stats_metrics(request: Request, secret: str = Depends(validate_secret), _ip = Depends(validate_ip), _header = Depends(validate_secret_header)):
    cache_key = "metrics:daily-invasion-grouped-stats"

    try:
        # Attempt to retrieve cached metrics
        cached_metrics = redis_client.get(cache_key)
        if cached_metrics:
            console_logger.info("Cache hit for Daily Invasion Grouped metrics")
            file_logger.info("Cache hit for Daily Invasion Grouped metrics")
            return Response(content=cached_metrics.decode("utf-8"), media_type="text/plain")
    except Exception as e:
        console_logger.error(f"Error accessing Redis cache for Daily Invasion Grouped metrics: {e}")
        file_logger.error(f"Error accessing Redis cache for Daily Invasion Grouped metrics: {e}")

    try:
        # Fetch data for metrics
        daily_invasion_grouped_stats = get_task_result(query_tasks.query_daily_invasion_grouped_stats_api)
        console_logger.info(f"Cache Miss. Fetched Daily Invasion Grouped API task sucessfuly")
        file_logger.info(f"Cache Miss. Fetched Daily Invasion Grouped API tasks sucessfuly")

        # Format each result set
        formatted_daily_invasion_grouped_stats = format_results_to_victoria(daily_invasion_grouped_stats, 'psyduck_daily_invasion_grouped_stats')
        console_logger.debug(f"Formatted Daily Invasion Grouped stats for VictoriaMetrics")
        file_logger.debug(f"Formatted Daily Invasion Grouped stats for VictoriaMetrics")

        # Combine all formatted metrics
        daily_invasion_grouped_stats_prometheus_metrics = '\n'.join([
            formatted_daily_invasion_grouped_stats
        ])

        ttl = seconds_until_midnight()
        redis_client.set(cache_key, daily_invasion_grouped_stats_prometheus_metrics, ex=ttl)
        # Return as plain text
        console_logger.info(f"Successfully set cache for Daily Invasion Grouped API for VictoriaMetrics")
        return Response(content=daily_invasion_grouped_stats_prometheus_metrics, media_type="text/plain")
    except Exception as e:
        console_logger.error(f"Error generating metrics: {e}")
        file_logger.error(f"Error generating metrics: {e}")
        return Response(content=f"Error generating metrics: {e}", media_type="text/plain", status_code=500)

@fastapi.get("/metrics/weekly-invasion-grouped-stats")
async def weekly_invasion_grouped_stats_metrics(request: Request, secret: str = Depends(validate_secret), _ip = Depends(validate_ip), _header = Depends(validate_secret_header)):
    cache_key = "metrics:weekly-invasion-grouped-stats"

    try:
        # Attempt to retrieve cached metrics
        cached_metrics = redis_client.get(cache_key)
        if cached_metrics:
            console_logger.info("Cache hit for Weekly Invasion Grouped metrics")
            file_logger.info("Cache hit for Weekly Invasion Grouped metrics")
            return Response(content=cached_metrics.decode("utf-8"), media_type="text/plain")
    except Exception as e:
        console_logger.error(f"Error accessing Redis cache for Weekly Invasion Grouped metrics: {e}")
        file_logger.error(f"Error accessing Redis cache for Weekly Invasion Grouped metrics: {e}")

    try:
        # Fetch data for metrics
        weekly_invasion_grouped_stats = get_task_result(query_tasks.query_weekly_invasion_grouped_stats_api)
        console_logger.info(f"Cache Miss. Fetched Weekly Invasion Grouped API task sucessfuly")
        file_logger.info(f"Cache Miss. Fetched Weekly Invasion Grouped API tasks sucessfuly")

        # Format each result set
        formatted_weekly_invasion_grouped_stats = format_results_to_victoria(weekly_invasion_grouped_stats, 'psyduck_weekly_invasion_grouped_stats')
        console_logger.debug(f"Formatted Weekly Invasion Grouped stats for VictoriaMetrics")
        file_logger.debug(f"Formatted Weekly Invasion Grouped stats for VictoriaMetrics")

        # Combine all formatted metrics
        weekly_invasion_grouped_stats_prometheus_metrics = '\n'.join([
            formatted_weekly_invasion_grouped_stats
        ])

        ttl = seconds_until_next_week()
        redis_client.set(cache_key, weekly_invasion_grouped_stats_prometheus_metrics, ex=ttl)
        # Return as plain text
        console_logger.info(f"Successfully set cache for Weekly Invasion Grouped API for VictoriaMetrics")
        return Response(content=weekly_invasion_grouped_stats_prometheus_metrics, media_type="text/plain")
    except Exception as e:
        console_logger.error(f"Error generating metrics: {e}")
        file_logger.error(f"Error generating metrics: {e}")
        return Response(content=f"Error generating metrics: {e}", media_type="text/plain", status_code=500)

@fastapi.get("/metrics/monthly-invasion-grouped-stats")
async def monthly_invasion_grouped_stats_metrics(request: Request, secret: str = Depends(validate_secret), _ip = Depends(validate_ip), _header = Depends(validate_secret_header)):
    cache_key = "metrics:monthly-invasion-grouped-stats"

    try:
        # Attempt to retrieve cached metrics
        cached_metrics = redis_client.get(cache_key)
        if cached_metrics:
            console_logger.info("Cache hit for Monthly Invasion Grouped metrics")
            file_logger.info("Cache hit for Monthly Invasion Grouped metrics")
            return Response(content=cached_metrics.decode("utf-8"), media_type="text/plain")
    except Exception as e:
        console_logger.error(f"Error accessing Redis cache for Monthly Invasion Grouped metrics: {e}")
        file_logger.error(f"Error accessing Redis cache for Monthly Invasion Grouped metrics: {e}")

    try:
        # Fetch data for metrics
        monthly_invasion_grouped_stats = get_task_result(query_tasks.query_monthly_invasion_grouped_stats_api)
        console_logger.info(f"Cache Miss. Fetched Monthly Invasion Grouped API task sucessfuly")
        file_logger.info(f"Cache Miss. Fetched Monthly Invasion Grouped API tasks sucessfuly")

        # Format each result set
        formatted_monthly_invasion_grouped_stats = format_results_to_victoria(monthly_invasion_grouped_stats, 'psyduck_monthly_invasion_grouped_stats')
        console_logger.debug(f"Formatted Monthly Invasion Grouped stats for VictoriaMetrics")
        file_logger.debug(f"Formatted Monthly Invasion Grouped stats for VictoriaMetrics")

        # Combine all formatted metrics
        monthly_invasion_grouped_stats_prometheus_metrics = '\n'.join([
            formatted_monthly_invasion_grouped_stats
        ])

        ttl = seconds_until_next_month()
        redis_client.set(cache_key, monthly_invasion_grouped_stats_prometheus_metrics, ex=ttl)
        # Return as plain text
        console_logger.info(f"Successfully set cache for Monthly Invasion Grouped API for VictoriaMetrics")
        return Response(content=monthly_invasion_grouped_stats_prometheus_metrics, media_type="text/plain")
    except Exception as e:
        console_logger.error(f"Error generating metrics: {e}")
        file_logger.error(f"Error generating metrics: {e}")
        return Response(content=f"Error generating metrics: {e}", media_type="text/plain", status_code=500)

@fastapi.get("/metrics/hourly-invasion-total-stats")
async def hourly_invasions_total_stats_metrics(request: Request, secret: str = Depends(validate_secret), _ip = Depends(validate_ip), _header = Depends(validate_secret_header)):
    cache_key = "metrics:hourly-invasion-total-stats"

    try:
        # Attempt to retrieve cached metrics
        cached_metrics = redis_client.get(cache_key)
        if cached_metrics:
            console_logger.info("Cache hit for Hourly Invasion Total metrics")
            file_logger.info("Cache hit for Hourly Invasion Total metrics")
            return Response(content=cached_metrics.decode("utf-8"), media_type="text/plain")
    except Exception as e:
        console_logger.error(f"Error accessing Redis cache for Hourly Invasion Total metrics: {e}")
        file_logger.error(f"Error accessing Redis cache for Hourly Invasion Total metrics: {e}")

    try:
        # Fetch data for metrics
        hourly_invasions_total_stats = get_task_result(query_tasks.query_hourly_invasions_total_stats_api)
        console_logger.info(f"Cache Miss. Fetched Hourly Invasion Total API task sucessfuly")
        file_logger.info(f"Cache Miss. Fetched Hourly Invasion Total API tasks sucessfuly")

        # Format each result set
        formatted_hourly_invasions_total_stats = format_results_to_victoria(hourly_invasions_total_stats, 'psyduck_hourly_invasions_total_stats')
        console_logger.debug(f"Formatted Hourly Invasion Total stats for VictoriaMetrics")
        file_logger.debug(f"Formatted Hourly Invasion Total stats for VictoriaMetrics")

        # Combine all formatted metrics
        hourly_invasions_total_stats_prometheus_metrics = '\n'.join([
            formatted_hourly_invasions_total_stats
        ])

        ttl = seconds_until_next_hour()
        redis_client.set(cache_key, hourly_invasions_total_stats_prometheus_metrics, ex=ttl)
        # Return as plain text
        console_logger.info(f"Successfully set cache for Hourly Invasion Total API for VictoriaMetrics")
        return Response(content=hourly_invasions_total_stats_prometheus_metrics, media_type="text/plain")
    except Exception as e:
        console_logger.error(f"Error generating metrics: {e}")
        file_logger.error(f"Error generating metrics: {e}")
        return Response(content=f"Error generating metrics: {e}", media_type="text/plain", status_code=500)

@fastapi.get("/metrics/daily-invasion-total-stats")
async def daily_invasions_total_stats_metrics(request: Request, secret: str = Depends(validate_secret), _ip = Depends(validate_ip), _header = Depends(validate_secret_header)):
    cache_key = "metrics:daily-invasion-total-stats"

    try:
        # Attempt to retrieve cached metrics
        cached_metrics = redis_client.get(cache_key)
        if cached_metrics:
            console_logger.info("Cache hit for Daily Invasion Total metrics")
            file_logger.info("Cache hit for Daily Invasion Total metrics")
            return Response(content=cached_metrics.decode("utf-8"), media_type="text/plain")
    except Exception as e:
        console_logger.error(f"Error accessing Redis cache for Daily Invasion Total metrics: {e}")
        file_logger.error(f"Error accessing Redis cache for Daily Invasion Total metrics: {e}")

    try:
        # Fetch data for metrics
        daily_invasions_total_stats = get_task_result(query_tasks.query_daily_invasions_total_stats_api)
        console_logger.info(f"Cache Miss. Fetched Daily Invasion Total API task sucessfuly")
        file_logger.info(f"Cache Miss. Fetched Daily Invasion Total API tasks sucessfuly")

        # Format each result set
        formatted_daily_invasions_total_stats = format_results_to_victoria(daily_invasions_total_stats, 'psyduck_daily_invasions_total_stats')
        console_logger.debug(f"Formatted Daily Invasion Total stats for VictoriaMetrics")
        file_logger.debug(f"Formatted Daily Invasion Total stats for VictoriaMetrics")

        # Combine all formatted metrics
        daily_invasions_total_stats_prometheus_metrics = '\n'.join([
            formatted_daily_invasions_total_stats
        ])

        ttl = seconds_until_midnight()
        redis_client.set(cache_key, daily_invasions_total_stats_prometheus_metrics, ex=ttl)
        # Return as plain text
        console_logger.info(f"Successfully set cache for Daily Invasion Total API for VictoriaMetrics")
        return Response(content=daily_invasions_total_stats_prometheus_metrics, media_type="text/plain")
    except Exception as e:
        console_logger.error(f"Error generating metrics: {e}")
        file_logger.error(f"Error generating metrics: {e}")
        return Response(content=f"Error generating metrics: {e}", media_type="text/plain", status_code=500)

@fastapi.get("/metrics/total-invasion-total-stats")
async def total_invasions_total_stats_metrics(request: Request, secret: str = Depends(validate_secret), _ip = Depends(validate_ip), _header = Depends(validate_secret_header)):
    cache_key = "metrics:total-invasion-total-stats"

    try:
        # Attempt to retrieve cached metrics
        cached_metrics = redis_client.get(cache_key)
        if cached_metrics:
            console_logger.info("Cache hit for Total Invasion Total metrics")
            file_logger.info("Cache hit for Total Invasion Total metrics")
            return Response(content=cached_metrics.decode("utf-8"), media_type="text/plain")
    except Exception as e:
        console_logger.error(f"Error accessing Redis cache for Total Invasion Total metrics: {e}")
        file_logger.error(f"Error accessing Redis cache for Total Invasion Total metrics: {e}")

    try:
        # Fetch data for metrics
        total_invasions_total_stats = get_task_result(query_tasks.query_total_invasions_total_stats_api)
        console_logger.info(f"Cache Miss. Fetched Total Invasion Total API task sucessfuly")
        file_logger.info(f"Cache Miss. Fetched Total Invasion Total API tasks sucessfuly")

        # Format each result set
        formatted_total_invasions_total_stats = format_results_to_victoria(total_invasions_total_stats, 'psyduck_total_invasions_total_stats')
        console_logger.debug(f"Formatted Total Invasion Total stats for VictoriaMetrics")
        file_logger.debug(f"Formatted Total Invasion Total stats for VictoriaMetrics")

        # Combine all formatted metrics
        total_invasions_total_stats_prometheus_metrics = '\n'.join([
            formatted_total_invasions_total_stats
        ])

        ttl = seconds_until_midnight()
        redis_client.set(cache_key, total_invasions_total_stats_prometheus_metrics, ex=ttl)
        # Return as plain text
        console_logger.info(f"Successfully set cache for Total Invasion Total API for VictoriaMetrics")
        return Response(content=total_invasions_total_stats_prometheus_metrics, media_type="text/plain")
    except Exception as e:
        console_logger.error(f"Error generating metrics: {e}")
        file_logger.error(f"Error generating metrics: {e}")
        return Response(content=f"Error generating metrics: {e}", media_type="text/plain", status_code=500)

# Pokemon TTH Section
@fastapi.get("/metrics/hourly-pokemon-tth-stats")
async def hourly_pokemon_tth_stats_metrics(request: Request, secret: str = Depends(validate_secret), _ip = Depends(validate_ip), _header = Depends(validate_secret_header)):
    cache_key = "metrics:hourly-pokemon-tth-stats"

    try:
        # Attempt to retrieve cached metrics
        cached_metrics = redis_client.get(cache_key)
        if cached_metrics:
            console_logger.info("Cache hit for Hourly Pokemon TTH metrics")
            file_logger.info("Cache hit for Hourly Pokemon TTH metrics")
            return Response(content=cached_metrics.decode("utf-8"), media_type="text/plain")
    except Exception as e:
        console_logger.error(f"Error accessing Redis cache for Hourly Pokemon TTH metrics: {e}")
        file_logger.error(f"Error accessing Redis cache for Hourly Pokemon TTH metrics: {e}")

    try:
        # Fetch data for metrics
        hourly_pokemon_tth_stats = get_task_result(query_tasks.query_hourly_pokemon_tth_stats_api)
        console_logger.info(f"Cache Miss. Fetched Hourly Pokemon TTH API task sucessfuly")
        file_logger.info(f"Cache Miss. Fetched Hourly Pokemon TTH API tasks sucessfuly")

        # Format each result set
        formatted_hourly_pokemon_tth_stats = format_results_to_victoria(hourly_pokemon_tth_stats, 'psyduck_hourly_pokemon_tth_stats')
        console_logger.debug(f"Formatted Hourly Pokemon TTH stats for VictoriaMetrics")
        file_logger.debug(f"Formatted Hourly Pokemon TTH stats for VictoriaMetrics")

        # Combine all formatted metrics
        hourly_pokemon_tth_stats_prometheus_metrics = '\n'.join([
            formatted_hourly_pokemon_tth_stats
        ])

        ttl = seconds_until_next_hour()
        redis_client.set(cache_key, hourly_pokemon_tth_stats_prometheus_metrics, ex=ttl)
        # Return as plain text
        console_logger.info(f"Successfully set cache for Hourly Pokemon TTH API for VictoriaMetrics")
        return Response(content=hourly_pokemon_tth_stats_prometheus_metrics, media_type="text/plain")
    except Exception as e:
        console_logger.error(f"Error generating metrics: {e}")
        file_logger.error(f"Error generating metrics: {e}")
        return Response(content=f"Error generating metrics: {e}", media_type="text/plain", status_code=500)

@fastapi.get("/metrics/daily-pokemon-tth-stats")
async def daily_pokemon_tth_stats_metrics(request: Request, secret: str = Depends(validate_secret), _ip = Depends(validate_ip), _header = Depends(validate_secret_header)):
    cache_key = "metrics:daily-pokemon-tth-stats"

    try:
        # Attempt to retrieve cached metrics
        cached_metrics = redis_client.get(cache_key)
        if cached_metrics:
            console_logger.info("Cache hit for Daily Pokemon TTH metrics")
            file_logger.info("Cache hit for Daily Pokemon TTH metrics")
            return Response(content=cached_metrics.decode("utf-8"), media_type="text/plain")
    except Exception as e:
        console_logger.error(f"Error accessing Redis cache for Daily Pokemon TTH metrics: {e}")
        file_logger.error(f"Error accessing Redis cache for Daily Pokemon TTH metrics: {e}")

    try:
        # Fetch data for metrics
        daily_pokemon_tth_stats = get_task_result(query_tasks.query_daily_pokemon_tth_stats_api)
        console_logger.info(f"Cache Miss. Fetched Daily Pokemon TTH API task sucessfuly")
        file_logger.info(f"Cache Miss. Fetched Daily Pokemon TTH API tasks sucessfuly")

        # Format each result set
        formatted_daily_pokemon_tth_stats = format_results_to_victoria_by_hour(daily_pokemon_tth_stats, 'psyduck_daily_pokemon_tth_stats')
        console_logger.debug(f"Formatted Daily Pokemon TTH stats for VictoriaMetrics")
        file_logger.debug(f"Formatted Daily Pokemon TTH stats for VictoriaMetrics")

        # Combine all formatted metrics
        daily_pokemon_tth_stats_prometheus_metrics = '\n'.join([
            formatted_daily_pokemon_tth_stats
        ])

        ttl = seconds_until_midnight()
        redis_client.set(cache_key, daily_pokemon_tth_stats_prometheus_metrics, ex=ttl)
        # Return as plain text
        return Response(content=daily_pokemon_tth_stats_prometheus_metrics, media_type="text/plain")
        console_logger.info(f"Successfully set cache for Daily Pokemon TTH API for VictoriaMetrics")
    except Exception as e:
        console_logger.error(f"Error generating metrics: {e}")
        file_logger.error(f"Error generating metrics: {e}")
        return Response(content=f"Error generating metrics: {e}", media_type="text/plain", status_code=500)

# Organises for VictoriaMetrics
def format_results_to_victoria(data, metric_prefix):
    prometheus_metrics = []
    for area_name, stats_list in data.items():
        for row in stats_list:
            area_name_formatted = area_name.replace('-', '_').replace(' ', '_').lower()
            area_label = "area=\"" + area_name_formatted +"\""

            day_label = ""
            if 'day' in row:
                day_formatted = str(row['day']).replace('-', '_')
                day_label = "day=\"" + day_formatted +"\""

            # Create a Victoria metric line for each column (now key) in the row
            for key, value in row.items():
                if key in ['area_name', 'day']:
                    continue
                if value is None  or (isinstance(value, str) and not value.isdigit()):
                    continue

                metric_name = f'{metric_prefix}_{key}'
                labels = f"{area_label}{', ' + day_label if day_label else ''}"
                prometheus_metric_line = f'{metric_name}{{{labels}}} {value}'
                prometheus_metrics.append(prometheus_metric_line)

    formatted_metrics = '\n'.join(prometheus_metrics)
    return formatted_metrics

# Organises Hour for VictoriaMetrics
def format_results_to_victoria_by_hour(data, metric_prefix):
    prometheus_metrics = []
    for hour, stats_list in data.items():
        for row in stats_list:
            # Extract the hour and use it as a label
            hour_formatted = str(hour).zfill(2)
            hour_label = "hour=\"" + hour_formatted +"\""

            # Extract area_name if present and format it as a label
            area_label = f'area_name="{row.get("area_name", "")}"' if 'area_name' in row else ""
            labels = [hour_label]

            if area_label:
                labels.append(area_label)

            labels_str = ",".join(labels)

            # Create a Victoria metric line for each column (now key) in the row
            for key, value in row.items():
                # Skip the area_name and  hour key since it's already used as a label
                if key in ['hour', 'area_name'] or value is None or (isinstance(value, str) and not value.isdigit()):
                    continue
                metric_name = f'{metric_prefix}_{key}'
                prometheus_metric_line = f'{metric_name}{{{labels_str}}} {value}'
                prometheus_metrics.append(prometheus_metric_line)

    formatted_metrics = '\n'.join(prometheus_metrics)
    return formatted_metrics
