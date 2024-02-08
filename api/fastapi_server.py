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
from processor.tasks import query_daily_api_pokemon_stats, query_weekly_api_pokemon_stats, query_monthly_api_pokemon_stats, query_hourly_total_api_pokemon_stats, query_daily_total_api_pokemon_stats, query_total_api_pokemon_stats, query_daily_surge_api_pokemon_stats, query_weekly_surge_api_pokemon_stats, query_monthly_surge_api_pokemon_stats
from utils.time_utils import seconds_until_next_hour, seconds_until_midnight, seconds_until_next_week, seconds_until_next_month
import os
import json
from datetime import date, datetime

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
    "/metrics/daily-area-pokemon",
    "/metrics/weekly-area-pokemon",
    "/metrics/monthly-area-pokemon",
    "/metrics/total-hourly-pokemon",
    "/metrics/total-daily-pokemon",
    "/metrics/total-pokemon",
    "/metrics/surge-daily-stats",
    "/metrics/surge-weekly-stats",
    "/metrics/surge-monthly-stats"
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
    except Exception as e:
        console_logger.error(f"Error accessing Redis cache for daily Pokemon stats: {e}")
        file_logger.error(f"Error accessing Redis cache for daily Pokemon stats: {e}")
        cached_result = None

    result = get_task_result(query_daily_api_pokemon_stats)
    serialized_result = json.dumps(result)

    # If cached result is None or different from new result, update the cache
    try:
        if not cached_result or cached_result != serialized_result:
            ttl = seconds_until_midnight()
            redis_client.set(cache_key, serialized_result, ex=ttl)
            console_logger.info("Cache updated with new daily Pokemon stats")
            file_logger.info("Cache updated with new daily Pokemon stats")
        else:
            console_logger.debug("Cached data is up-to-date. No update needed.")
            file_logger.debug("Cached data is up-to-date. No update needed.")
    except Exception as e:
        console_logger.error(f"Error updating Redis cache for daily Pokemon stats: {e}")
        file_logger.error(f"Error updating Redis cache for daily Pokemon stats: {e}")

    return JSONResponse(content=result)
    console_logger.info("Successfully obtained daily Pokemon stats")
    file_logger.info("Sucessfully obtained daily Pokemon stats")

@fastapi.get("/api/weekly-area-pokemon-stats")
async def weekly_area_pokemon_stats(request: Request, secret: str = Depends(validate_secret), _ip = Depends(validate_ip), _header = Depends(validate_secret_header)):
    console_logger.debug("Request received for weekly Pokemon stats")
    file_logger.debug("Request received for weekly Pokemon stats")

    cache_key = "weekly-area-pokemon-stats"

    try:
        cached_result = redis_client.get(cache_key)
    except Exception as e:
        console_logger.error(f"Error accessing Redis cache for weekly Pokemon stats: {e}")
        file_logger.error(f"Error accessing Redis cache for weekly Pokemon stats: {e}")
        cached_result = None

    result = get_task_result(query_weekly_api_pokemon_stats)
    serialized_result = json.dumps(result)

    # If cached result is None or different from new result, update the cache
    try:
        if not cached_result or cached_result != serialized_result:
            ttl = seconds_until_next_week()
            redis_client.set(cache_key, serialized_result, ex=ttl)
            console_logger.info("Cache updated with new weekly Pokemon stats")
            file_logger.info("Cache updated with new weekly Pokemon stats")
        else:
            console_logger.debug("Cached data is up-to-date. No update needed.")
            file_logger.debug("Cached data is up-to-date. No update needed.")
    except Exception as e:
        console_logger.error(f"Error updating Redis cache for weekly Pokemon stats: {e}")
        file_logger.error(f"Error updating Redis cache for weekly Pokemon stats: {e}")

    return JSONResponse(content=result)
    console_logger.info("Successfully obtained weekly Pokemon stats")
    file_logger.info("Sucessfully obtained weekly Pokemon stats")

@fastapi.get("/api/monthly-area-pokemon-stats")
async def monthly_area_pokemon_stats(request: Request, secret: str = Depends(validate_secret), _ip = Depends(validate_ip), _header = Depends(validate_secret_header)):
    console_logger.debug("Request received for monthly Pokemon stats")
    file_logger.debug("Request received for monthly Pokemon stats")

    cache_key = "monthly-area-pokemon-stats"

    try:
        cached_result = redis_client.get(cache_key)
    except Exception as e:
        console_logger.error(f"Error accessing Redis cache for monthly Pokemon stats: {e}")
        file_logger.error(f"Error accessing Redis cache for monthly Pokemon stats: {e}")
        cached_result = None

    result = get_task_result(query_monthly_api_pokemon_stats)
    serialized_result = json.dumps(result)

    # If cached result is None or different from new result, update the cache
    try:
        if not cached_result or cached_result != serialized_result:
            ttl = seconds_until_next_month()
            redis_client.set(cache_key, serialized_result, ex=ttl)
            console_logger.info("Cache updated with new monthly Pokemon stats")
            file_logger.info("Cache updated with new monthly Pokemon stats")
        else:
            console_logger.debug("Cached data is up-to-date. No update needed.")
            file_logger.debug("Cached data is up-to-date. No update needed.")
    except Exception as e:
        console_logger.error(f"Error updating Redis cache for monthly Pokemon stats: {e}")
        file_logger.error(f"Error updating Redis cache for monthly Pokemon stats: {e}")

    return JSONResponse(content=result)
    console_logger.info("Successfully obtained monthly Pokemon stats")
    file_logger.info("Sucessfully obtained monthly Pokemon stats")

# API Totals
@fastapi.get("/api/hourly-total-pokemon-stats")
async def hourly_total_pokemon_stats(request: Request, secret: str = Depends(validate_secret), _ip = Depends(validate_ip), _header = Depends(validate_secret_header)):
    console_logger.debug("Request received for hourly total Pokemon stats")
    file_logger.debug("Request received for hourly total Pokemon stats")

    cache_key = "hourly-total-pokemon-stats"

    try:
        cached_result = redis_client.get(cache_key)
    except Exception as e:
        console_logger.error(f"Error accessing Redis cache for hourly total Pokemon stats: {e}")
        file_logger.error(f"Error accessing Redis cache for hourly total Pokemon stats: {e}")
        cached_result = None

    result = get_task_result(query_hourly_total_api_pokemon_stats)
    serialized_result = json.dumps(result)

    # If cached result is None or different from new result, update the cache
    try:
        if not cached_result or cached_result != serialized_result:
            ttl = seconds_until_next_hour()
            redis_client.set(cache_key, serialized_result, ex=ttl)
            console_logger.info("Cache updated with new hourly total Pokemon stats")
            file_logger.info("Cache updated with new hourly total Pokemon stats")
        else:
            console_logger.debug("Cached data is up-to-date. No update needed.")
            file_logger.debug("Cached data is up-to-date. No update needed.")
    except Exception as e:
        console_logger.error(f"Error updating Redis cache for hourly total Pokemon stats: {e}")
        file_logger.error(f"Error updating Redis cache for hourly total Pokemon stats: {e}")

    return JSONResponse(content=result)
    console_logger.info("Successfully obtained hourly total Pokemon stats")
    file_logger.info("Sucessfully obtained hourly total Pokemon stats")


@fastapi.get("/api/daily-total-pokemon-stats")
async def daily_total_pokemon_stats(request: Request, secret: str= Depends(validate_secret), _ip = Depends(validate_ip), _header = Depends(validate_secret_header)):
    console_logger.debug("Request received for daily total Pokemon stats")
    file_logger.debug("Request received for daily total Pokemon stats")

    cache_key = "daily-total-pokemon-stats"

    try:
        cached_result = redis_client.get(cache_key)
    except Exception as e:
        console_logger.error(f"Error accessing Redis cache for daily total Pokemon stats: {e}")
        file_logger.error(f"Error accessing Redis cache for daily total Pokemon stats: {e}")
        cached_result = None

    result = get_task_result(query_daily_total_api_pokemon_stats)
    serialized_result = json.dumps(result)

    # If cached result is None or different from new result, update the cache
    try:
        if not cached_result or cached_result != serialized_result:
            ttl = seconds_until_midnight()
            redis_client.set(cache_key, serialized_result, ex=ttl)
            console_logger.info("Cache updated with new daily total Pokemon stats")
            file_logger.info("Cache updated with new daily total Pokemon stats")
        else:
            console_logger.debug("Cached data is up-to-date. No update needed.")
            file_logger.debug("Cached data is up-to-date. No update needed.")
    except Exception as e:
        console_logger.error(f"Error updating Redis cache for daily total Pokemon stats: {e}")
        file_logger.error(f"Error updating Redis cache for daily total Pokemon stats: {e}")

    return JSONResponse(content=result)
    console_logger.info("Successfully obtained daily total Pokemon stats")
    file_logger.info("Sucessfully obtained daily total Pokemon stats")

@fastapi.get("/api/total-pokemon-stats")
async def total_pokemon_stats(request: Request, secret: str= Depends(validate_secret), _ip = Depends(validate_ip), _header = Depends(validate_secret_header)):
    console_logger.debug("Request received for total Pokemon stats")
    file_logger.debug("Request received for total Pokemon stats")

    cache_key = "total-pokemon-stats"

    try:
        cached_result = redis_client.get(cache_key)
    except Exception as e:
        console_logger.error(f"Error accessing Redis cache for total Pokemon stats: {e}")
        file_logger.error(f"Error accessing Redis cache for total Pokemon stats: {e}")
        cached_result = None

    result = get_task_result(query_total_api_pokemon_stats)
    serialized_result = json.dumps(result)

    # If cached result is None or different from new result, update the cache
    try:
        if not cached_result or cached_result != serialized_result:
            ttl = seconds_until_midnight()
            redis_client.set(cache_key, serialized_result, ex=ttl)
            console_logger.info("Cache updated with new total Pokemon stats")
            file_logger.info("Cache updated with new total Pokemon stats")
        else:
            console_logger.debug("Cached data is up-to-date. No update needed.")
            file_logger.debug("Cached data is up-to-date. No update needed.")
    except Exception as e:
        console_logger.error(f"Error updating Redis cache for total Pokemon stats: {e}")
        file_logger.error(f"Error updating Redis cache for total Pokemon stats: {e}")

    return JSONResponse(content=result)
    console_logger.info("Successfully obtained total Pokemon stats")
    file_logger.info("Sucessfully obtained total Pokemon stats")

# API Surge's
@fastapi.get("/api/surge-daily-stats")
async def surge_daily_pokemon_stats(request: Request, secret: str= Depends(validate_secret), _ip = Depends(validate_ip), _header = Depends(validate_secret_header)):
    console_logger.info("Sucessfully obtained Surge Daily Pokemon Stats")
    file_logger.info("Sucessfully obtained Surge Daily Pokemon Stats")

    console_logger.debug("Request received for Surge Daily Pokemon stats")
    file_logger.debug("Request received for Surge Daily Pokemon stats")

    cache_key = "surge-daily-pokemon-stats"

    try:
        cached_result = redis_client.get(cache_key)
    except Exception as e:
        console_logger.error(f"Error accessing Redis cache for Surge Daily Pokemon stats: {e}")
        file_logger.error(f"Error accessing Redis cache for Surge Daily Pokemon stats: {e}")
        cached_result = None

    result = get_task_result(query_daily_surge_api_pokemon_stats)
    serialized_result = json.dumps(result)

    # If cached result is None or different from new result, update the cache
    try:
        if not cached_result or cached_result != serialized_result:
            ttl = seconds_until_midnight()
            redis_client.set(cache_key, serialized_result, ex=ttl)
            console_logger.info("Cache updated with new Surge Daily Pokemon stats")
            file_logger.info("Cache updated with new Surge Daily Pokemon stats")
        else:
            console_logger.debug("Cached data is up-to-date. No update needed.")
            file_logger.debug("Cached data is up-to-date. No update needed.")
    except Exception as e:
        console_logger.error(f"Error updating Redis cache for Surge Daily Pokemon stats: {e}")
        file_logger.error(f"Error updating Redis cache for Surge Daily Pokemon stats: {e}")

    return JSONResponse(content=result)
    console_logger.info("Successfully obtained Surge Daily Pokemon stats")
    file_logger.info("Sucessfully obtained Surge Daily Pokemon stats")

@fastapi.get("/api/surge-weekly-stats")
async def surge_weekly_pokemon_stats(request: Request, secret: str= Depends(validate_secret), _ip = Depends(validate_ip), _header = Depends(validate_secret_header)):
    console_logger.debug("Request received for Surge Weekly Pokemon stats")
    file_logger.debug("Request received for Surge Weekly Pokemon stats")

    cache_key = "surge-weekly-pokemon-stats"

    try:
        cached_result = redis_client.get(cache_key)
    except Exception as e:
        console_logger.error(f"Error accessing Redis cache for Surge Weekly Pokemon stats: {e}")
        file_logger.error(f"Error accessing Redis cache for Surge Weekly Pokemon stats: {e}")
        cached_result = None

    result = get_task_result(query_weekly_surge_api_pokemon_stats)
    serialized_result = json.dumps(result)

    # If cached result is None or different from new result, update the cache
    try:
        if not cached_result or cached_result != serialized_result:
            ttl = seconds_until_next_week()
            redis_client.set(cache_key, serialized_result, ex=ttl)
            console_logger.info("Cache updated with new Surge Weekly Pokemon stats")
            file_logger.info("Cache updated with new Surge Weekly Pokemon stats")
        else:
            console_logger.debug("Cached data is up-to-date. No update needed.")
            file_logger.debug("Cached data is up-to-date. No update needed.")
    except Exception as e:
        console_logger.error(f"Error updating Redis cache for Surge Weekly Pokemon stats: {e}")
        file_logger.error(f"Error updating Redis cache for Surge Weekly Pokemon stats: {e}")

    return JSONResponse(content=result)
    console_logger.info("Successfully obtained Surge Weekly Pokemon stats")
    file_logger.info("Sucessfully obtained Surge Weekly Pokemon stats")

@fastapi.get("/api/surge-monthly-stats")
async def surge_monthly_pokemon_stats(request: Request, secret: str= Depends(validate_secret), _ip = Depends(validate_ip), _header = Depends(validate_secret_header)):
    console_logger.debug("Request received for Surge Monthly Pokemon stats")
    file_logger.debug("Request received for Surge Monthly Pokemon stats")

    cache_key = "surge-monthly-pokemon-stats"

    try:
        cached_result = redis_client.get(cache_key)
    except Exception as e:
        console_logger.error(f"Error accessing Redis cache for Surge Monthly Pokemon stats: {e}")
        file_logger.error(f"Error accessing Redis cache for Surge Monthly Pokemon stats: {e}")
        cached_result = None

    result = get_task_result(query_monthly_surge_api_pokemon_stats)
    serialized_result = json.dumps(result)

    # If cached result is None or different from new result, update the cache
    try:
        if not cached_result or cached_result != serialized_result:
            ttl = seconds_until_next_month()
            redis_client.set(cache_key, serialized_result, ex=ttl)
            console_logger.info("Cache updated with new Surge Monthly Pokemon stats")
            file_logger.info("Cache updated with new Surge Monthly Pokemon stats")
        else:
            console_logger.debug("Cached data is up-to-date. No update needed.")
            file_logger.debug("Cached data is up-to-date. No update needed.")
    except Exception as e:
        console_logger.error(f"Error updating Redis cache for Surge Monthly Pokemon stats: {e}")
        file_logger.error(f"Error updating Redis cache for Surge Monthly Pokemon stats: {e}")

    return JSONResponse(content=result)
    console_logger.info("Successfully obtained Surge Monthly Pokemon stats")
    file_logger.info("Sucessfully obtained Surge Monthly Pokemon stats")

# API format for VictoriaMetrics/Prometheus

@fastapi.get("/metrics/daily-area-pokemon")
@cache(expire=app_config.api_metrics_daily_area_pokemon_cache)
async def daily_area_pokemon_metrics(request: Request, secret: str = Depends(validate_secret), _ip = Depends(validate_ip), _header = Depends(validate_secret_header)):
    try:
        # Fetch data for metrics
        daily_area_stats = get_task_result(query_daily_api_pokemon_stats)
        console_logger.info(f"Fetched daily grouped pokemon API task sucessfuly")
        file_logger.info(f"Fetched daily grouped pokemon API tasks sucessfuly")

        # Format each result set
        formatted_daily_area_stats = format_results_to_victoria(daily_area_stats, 'psyduck_daily_area_stats')
        console_logger.debug(f"Formatted daily area stats for VictoriaMetrics.")
        file_logger.debug(f"Formatted daily area stats for VictoriaMetrics.")

        # Combine formatted metrics for area stats
        daily_area_pokemon_prometheus_metrics = '\n'.join([
            formatted_daily_area_stats
        ])

        # Return as plain text
        return Response(content=daily_area_pokemon_prometheus_metrics, media_type="text/plain")
        console_logger.info(f"Successfully retrieved grouped APIs for VictoriaMetrics")
    except Exception as e:
        console_logger.error(f"Error generating grouped metrics: {e}")
        file_logger.error(f"Error generating grouped metrics: {e}")
        return Response(content=f"Error generating grouped metrics: {e}", media_type="text/plain", status_code=500)

@fastapi.get("/metrics/weekly-area-pokemon")
@cache(expire=app_config.api_metrics_weekly_area_pokemon_cache)
async def weekly_area_pokemon_metrics(request: Request, secret: str = Depends(validate_secret), _ip = Depends(validate_ip), _header = Depends(validate_secret_header)):
    try:
        # Fetch data for metrics
        weekly_area_stats = get_task_result(query_weekly_api_pokemon_stats)
        console_logger.info(f"Fetched weekly grouped pokemon API task sucessfuly")
        file_logger.info(f"Fetched weekly grouped pokemon API tasks sucessfuly")

        # Format the result
        formatted_weekly_area_stats = format_results_to_victoria(weekly_area_stats, 'psyduck_weekly_area_stats')
        console_logger.debug(f"Formatted weekly area stats for VictoriaMetrics")
        file_logger.debug(f"Formatted weekly area stats for VictoriaMetrics")

        # Combine formatted metrics for grouped stats
        weekly_area_pokemon_prometheus_metrics = '\n'.join([
            formatted_weekly_area_stats
        ])

        # Return as plain text
        return Response(content=weekly_area_pokemon_prometheus_metrics, media_type="text/plain")
        console_logger.info(f"Successfully retrieved grouped-weekly API for VictoriaMetrics")
    except Exception as e:
        console_logger.error(f"Error generating grouped metrics: {e}")
        file_logger.error(f"Error generating grouped metrics: {e}")
        return Response(content=f"Error generating grouped metrics: {e}", media_type="text/plain", status_code=500)

@fastapi.get("/metrics/monthly-area-pokemon")
@cache(expire=app_config.api_metrics_monthly_area_pokemon_cache)
async def monthly_area_pokemon_metrics(request: Request, secret: str = Depends(validate_secret), _ip = Depends(validate_ip), _header = Depends(validate_secret_header)):
    try:
        # Fetch data for metrics
        monthly_area_stats = get_task_result(query_monthly_api_pokemon_stats)
        console_logger.info(f"Fetched monthly grouped pokemon API task sucessfuly")
        file_logger.info(f"Fetched monthly grouped pokemon API tasks sucessfuly")

        # Format the result
        formatted_monthly_area_stats = format_results_to_victoria(monthly_area_stats, 'psyduck_monthly_area_stats')
        console_logger.debug(f"Formatted monthly area stats for VictoriaMetrics")
        file_logger.debug(f"Formatted monthly area stats for VictoriaMetrics")

        # Combine formatted metrics for grouped stats
        monthly_area_pokemon_prometheus_metrics = '\n'.join([
            formatted_monthly_area_stats
        ])

        # Return as plain text
        return Response(content=monthly_area_pokemon_prometheus_metrics, media_type="text/plain")
        console_logger.info(f"Successfully retrieved grouped-monthly API for VictoriaMetrics")
    except Exception as e:
        console_logger.error(f"Error generating grouped metrics: {e}")
        file_logger.error(f"Error generating grouped metrics: {e}")
        return Response(content=f"Error generating grouped metrics: {e}", media_type="text/plain", status_code=500)

@fastapi.get("/metrics/total-hourly-pokemon")
@cache(expire=app_config.api_metrics_hourly_total_pokemon_cache)
async def total_hourly_pokemon_metrics(request: Request, secret: str = Depends(validate_secret), _ip = Depends(validate_ip), _header = Depends(validate_secret_header)):
    try:
        # Fetch data for metrics
        hourly_total_stats = get_task_result(query_hourly_total_api_pokemon_stats)
        console_logger.info(f"Fetched hourly total pokemon API task sucessfuly")
        file_logger.info(f"Fetched hourly total pokemon API tasks sucessfuly")

        # Format each result set
        formatted_hourly_total_stats = format_results_to_victoria(hourly_total_stats, 'psyduck_hourly_total_stats')
        console_logger.debug(f"Formatted hourly total stats for VictoriaMetrics")
        file_logger.debug(f"Formatted hourly total stats for VictoriaMetrics")

        # Combine all formatted metrics
        total_hourly_pokemon_prometheus_metrics = '\n'.join([
            formatted_hourly_total_stats
        ])

        # Return as plain text
        return Response(content=total_hourly_pokemon_prometheus_metrics, media_type="text/plain")
        console_logger.info(f"Successfully retrieved all total and surge APIs for VictoriaMetrics")
    except Exception as e:
        console_logger.error(f"Error generating metrics: {e}")
        file_logger.error(f"Error generating metrics: {e}")
        return Response(content=f"Error generating metrics: {e}", media_type="text/plain", status_code=500)

@fastapi.get("/metrics/total-daily-pokemon")
@cache(expire=app_config.api_metrics_daily_total_pokemon_cache)
async def total_daily_pokemon_metrics(request: Request, secret: str = Depends(validate_secret), _ip = Depends(validate_ip), _header = Depends(validate_secret_header)):
    try:
        # Fetch data for metrics
        daily_total_stats = get_task_result(query_daily_total_api_pokemon_stats)
        console_logger.info(f"Fetched daily total pokemon API task sucessfuly")
        file_logger.info(f"Fetched daily total pokemon API tasks sucessfuly")

        # Format each result set
        formatted_daily_total_stats = format_results_to_victoria(daily_total_stats, 'psyduck_daily_total_stats')
        console_logger.debug(f"Formatted daily total stats for VictoriaMetrics")
        file_logger.debug(f"Formatted daily total stats for VictoriaMetrics")

        # Combine all formatted metrics
        total_daily_pokemon_prometheus_metrics = '\n'.join([
            formatted_daily_total_stats
        ])

        # Return as plain text
        return Response(content=total_daily_pokemon_prometheus_metrics, media_type="text/plain")
        console_logger.info(f"Successfully retrieved all total and surge APIs for VictoriaMetrics")
    except Exception as e:
        console_logger.error(f"Error generating metrics: {e}")
        file_logger.error(f"Error generating metrics: {e}")
        return Response(content=f"Error generating metrics: {e}", media_type="text/plain", status_code=500)

@fastapi.get("/metrics/total-pokemon")
@cache(expire=app_config.api_metrics_total_pokemon_cache)
async def total_pokemon_metrics(request: Request, secret: str = Depends(validate_secret), _ip = Depends(validate_ip), _header = Depends(validate_secret_header)):
    try:
        # Fetch data for metrics
        total_stats = get_task_result(query_total_api_pokemon_stats)
        console_logger.info(f"Fetched total pokemon API task sucessfuly")
        file_logger.info(f"Fetched total pokemon API tasks sucessfuly")

        # Format each result set
        formatted_total_stats = format_results_to_victoria(total_stats, 'psyduck_total_stats')
        console_logger.debug(f"Formatted total stats for VictoriaMetrics")
        file_logger.debug(f"Formatted total stats for VictoriaMetrics")

        # Combine all formatted metrics
        total_pokemon_prometheus_metrics = '\n'.join([
            formatted_total_stats
        ])

        # Return as plain text
        return Response(content=total_pokemon_prometheus_metrics, media_type="text/plain")
        console_logger.info(f"Successfully retrieved all total and surge APIs for VictoriaMetrics")
    except Exception as e:
        console_logger.error(f"Error generating metrics: {e}")
        file_logger.error(f"Error generating metrics: {e}")
        return Response(content=f"Error generating metrics: {e}", media_type="text/plain", status_code=500)

@fastapi.get("/metrics/surge-daily-stats")
@cache(expire=app_config.api_metrics_surge_daily_pokemon_cache)
async def surge_daily_metrics(request: Request, secret: str = Depends(validate_secret), _ip = Depends(validate_ip), _header = Depends(validate_secret_header)):
    try:
        # Fetch data for metrics
        surge_daily_stats = get_task_result(query_daily_surge_api_pokemon_stats)
        console_logger.info(f"Fetched surge daily API task sucessfuly")
        file_logger.info(f"Fetched surge daily API tasks sucessfuly")

        # Format each result set
        formatted_surge_daily_stats = format_results_to_victoria_by_hour(surge_daily_stats, 'psyduck_surge_daily')
        console_logger.debug(f"Formatted surge daily stats for VictoriaMetrics")
        file_logger.debug(f"Formatted surge daily stats for VictoriaMetrics")

        # Combine all formatted metrics
        surge_daily_prometheus_metrics = '\n'.join([
            formatted_surge_daily_stats
        ])

        # Return as plain text
        return Response(content=surge_daily_prometheus_metrics, media_type="text/plain")
        console_logger.info(f"Successfully retrieved all total and surge APIs for VictoriaMetrics")
    except Exception as e:
        console_logger.error(f"Error generating metrics: {e}")
        file_logger.error(f"Error generating metrics: {e}")
        return Response(content=f"Error generating metrics: {e}", media_type="text/plain", status_code=500)

@fastapi.get("/metrics/surge-weekly-stats")
@cache(expire=app_config.api_metrics_surge_weekly_pokemon_cache)
async def surge_weekly_metrics(request: Request, secret: str = Depends(validate_secret), _ip = Depends(validate_ip), _header = Depends(validate_secret_header)):
    try:
        # Fetch data for metrics
        surge_weekly_stats = get_task_result(query_weekly_surge_api_pokemon_stats)
        console_logger.info(f"Fetched surge weekly API task sucessfuly")
        file_logger.info(f"Fetched surge weekly API tasks sucessfuly")

        # Format each result set
        formatted_surge_weekly_stats = format_results_to_victoria_by_hour(surge_weekly_stats, 'psyduck_surge_weekly')
        console_logger.debug(f"Formatted surge weekly stats for VictoriaMetrics")
        file_logger.debug(f"Formatted surge weekly stats for VictoriaMetrics")

        # Combine all formatted metrics
        surge_weekly_prometheus_metrics = '\n'.join([
            formatted_surge_weekly_stats
        ])

        # Return as plain text
        return Response(content=surge_weekly_prometheus_metrics, media_type="text/plain")
        console_logger.info(f"Successfully retrieved all total and surge APIs for VictoriaMetrics")
    except Exception as e:
        console_logger.error(f"Error generating metrics: {e}")
        file_logger.error(f"Error generating metrics: {e}")
        return Response(content=f"Error generating metrics: {e}", media_type="text/plain", status_code=500)

@fastapi.get("/metrics/surge-monthly-stats")
@cache(expire=app_config.api_metrics_surge_monthly_pokemon_cache)
async def surge_monthly_metrics(request: Request, secret: str = Depends(validate_secret), _ip = Depends(validate_ip), _header = Depends(validate_secret_header)):
    try:
        # Fetch data for metrics
        surge_monthly_stats = get_task_result(query_monthly_surge_api_pokemon_stats)
        console_logger.info(f"Fetched surge monthly API task sucessfuly")
        file_logger.info(f"Fetched surge monthly API tasks sucessfuly")

        # Format each result set
        formatted_surge_monthly_stats = format_results_to_victoria_by_hour(surge_monthly_stats, 'psyduck_surge_monthly')
        console_logger.debug(f"Formatted surge monthly stats for VictoriaMetrics")
        file_logger.debug(f"Formatted surge monthly stats for VictoriaMetrics")

        # Combine all formatted metrics
        surge_monthly_prometheus_metrics = '\n'.join([
            formatted_surge_monthly_stats
        ])

        # Return as plain text
        return Response(content=surge_monthly_prometheus_metrics, media_type="text/plain")
        console_logger.info(f"Successfully retrieved all total and surge APIs for VictoriaMetrics")
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

            # Create a Victoria metric line for each column (now key) in the row
            for key, value in row.items():
                # Skip the hour key since it's already used as a label
                if value is None  or (isinstance(value, str) and not value.isdigit()):
                    continue
                metric_name = f'{metric_prefix}_{key}'
                prometheus_metric_line = f'{metric_name}{{{hour_label}}} {value}'
                prometheus_metrics.append(prometheus_metric_line)

    formatted_metrics = '\n'.join(prometheus_metrics)
    return formatted_metrics