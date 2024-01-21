import logging
from logging.handlers import RotatingFileHandler
from logging import StreamHandler
from fastapi import FastAPI, HTTPException, Depends, Header, Request, Response
from starlette.responses import JSONResponse
from fastapi_cache import FastAPICache
from fastapi_cache.backends.redis import RedisBackend
from fastapi_cache.decorator import cache
from redis import asyncio as aioredis
from celery.result import AsyncResult
from celery import Celery
from config.app_config import app_config
from processor.tasks import query_daily_api_pokemon_stats, query_weekly_api_pokemon_stats, query_monthly_api_pokemon_stats, query_hourly_total_api_pokemon_stats, query_daily_total_api_pokemon_stats, query_total_api_pokemon_stats, query_daily_surge_api_pokemon_stats, query_weekly_surge_api_pokemon_stats, query_monthly_surge_api_pokemon_stats
import os
import json

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
    "/metrics"
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
            console_logger.info(f"Access denied for IP: {client_host}")
            file_logger.info(f"Access denied for IP: {client_host}")
            # Return a 403 Forbidden response
            return JSONResponse(status_code=403, content={"detail": "Access denied"})

        console_logger.info(f"Access from IP: {client_host} allowed.")
        file_logger.info(f"Access from IP: {client_host} allowed.")
    return await call_next(request)

fastapi = FastAPI()

fastapi.middleware('http')(check_ip_middleware)
fastapi.middleware('http')(check_path_middleware)

@fastapi.on_event("startup")
async def startup():
    console_logger.info("Starting up the application")
    file_logger.info("Starting up the application")

    # Initiliaze Redis
    redis = aioredis.from_url(app_config.redis_url, encoding="utf8", decode_responses=True)
    FastAPICache.init(RedisBackend(redis), prefix="fastapi-cache")
    console_logger.info("FastAPI cache initialized with Redis backend")
    file_logger.info("FastAPI cache initialized with Redis backend")
    
    # Clear the cache on startup
    await FastAPICache.clear(namespace="fastapi-cache")
    console_logger.info("Previous cache cleared on startup")
    file_logger.info("Previous cache cleared on startup")


async def validate_secret_header(secret: str = Header(None, alias=app_config.api_header_name)):
    if app_config.api_secret_header_key:
        if secret != app_config.api_secret_header_key:
    #if not secret or secret != app_config.api_secret_header_key:
            console_logger.warning("Unauthorized access attempt with wrong secret header")
            file_logger.warning("Unauthorized access attempt with wrong secret header")
            raise HTTPException(status_code=403, detail="Unauthorized access")
        console_logger.info("Secret header validated successfully.")
        file_logger.info("Secret header validated successfully.")
    else:
        console_logger.info("No API secret header key set, skipping secret validation.")
        file_logger.info("No API secret header key set, skipping secret validation.")

async def validate_secret(secret: str = None):
    if app_config.api_secret_key:
        if secret != app_config.api_secret_key:
            console_logger.warning("Unauthorized access attempt with wrong secret")
            file_logger.warning("Unauthorized access attempt with wrong secret")
            raise HTTPException(status_code=403, detail="Unauthorized access")
        console_logger.info("Secret validated successfully.")
        file_logger.info("Secret validated successfully.")
    else:
        console_logger.info("No API secret key set, skipping secret validation.")
        file_logger.info("No API secret key set, skipping secret validation.")        

async def validate_ip(request: Request):
    client_host = request.client.host
    if app_config.api_ip_restriction and client_host not in app_config.api_allowed_ips:
        console_logger.info(f"Access denied for IP: {client_host}")
        file_logger.info(f"Access denied for IP: {client_host}")
        raise HTTPException(status_code=403, detail="Access denied")
    console_logger.info(f"Access from IP: {client_host} allowed.")
    file_logger.info(f"Access from IP: {client_host} allowed.")

def get_task_result(task_function, *args, **kwargs):
    console_logger.info(f"Fetching task result for {task_function.__name__}")
    file_logger.info(f"Fetching task result for {task_function.__name__}")
    result = task_function.delay(*args, **kwargs)
    return result.get(timeout=50)

# API Grouped
@fastapi.get("/api/daily-area-pokemon-stats")
@cache(expire=app_config.api_daily_pokemon_cache)
async def daily_area_pokemon_stats(request: Request, secret: str = Depends(validate_secret), _ip = Depends(validate_ip), _header = Depends(validate_secret_header)):
    console_logger.info("Request received for daily Pokemon stats")
    file_logger.info("Request received for daily Pokemon stats")
    return get_task_result(query_daily_api_pokemon_stats)

@fastapi.get("/api/weekly-area-pokemon-stats")
@cache(expire=app_config.api_weekly_pokemon_cache)
async def weekly_area_pokemon_stats(request: Request, secret: str = Depends(validate_secret), _ip = Depends(validate_ip), _header = Depends(validate_secret_header)):
    console_logger.info("Request received for weekly Pokemon stats")
    file_logger.info("Request received for weekly Pokemon stats")
    return get_task_result(query_weekly_api_pokemon_stats)

@fastapi.get("/api/monthly-area-pokemon-stats")
@cache(expire=app_config.api_monthly_pokemon_cache)
async def monthly_area_pokemon_stats(request: Request, secret: str = Depends(validate_secret), _ip = Depends(validate_ip), _header = Depends(validate_secret_header)):
    console_logger.info("Request received for monthly Pokemon stats")
    file_logger.info("Request received for monthly Pokemon stats")
    return get_task_result(query_monthly_api_pokemon_stats)

# API Totals
@fastapi.get("/api/hourly-total-pokemon-stats")
@cache(expire=app_config.api_hourly_total_pokemon_cache)
async def hourly_total_pokemon_stats(request: Request, secret: str = Depends(validate_secret), _ip = Depends(validate_ip), _header = Depends(validate_secret_header)):
        console_logger.info("Request received for hourly total Pokemon stats")
        file_logger.info("Request received for hourly total Pokemon stats")
        return get_task_result(query_hourly_total_api_pokemon_stats)

@fastapi.get("/api/daily-total-pokemon-stats")
@cache(expire=app_config.api_daily_total_pokemon_cache)
async def daily_total_pokemon_stats(request: Request, secret: str= Depends(validate_secret), _ip = Depends(validate_ip), _header = Depends(validate_secret_header)):
    console_logger.info("Request received for daily total Pokemon stats")
    file_logger.info("Request received for daily total Pokemon stats")
    return get_task_result(query_daily_total_api_pokemon_stats)

@fastapi.get("/api/total-pokemon-stats")
@cache(expire=app_config.api_total_pokemon_cache)
async def total_pokemon_stats(request: Request, secret: str= Depends(validate_secret), _ip = Depends(validate_ip), _header = Depends(validate_secret_header)):
    console_logger.info("Request received for total Pokemon stats")
    file_logger.info("Request received for total Pokemon stats")
    return get_task_result(query_total_api_pokemon_stats)

# API Surge's
@fastapi.get("/api/surge-daily-stats")
@cache(expire=app_config.api_surge_daily_cache)
async def surge_daily_pokemon_stats(request: Request, secret: str= Depends(validate_secret), _ip = Depends(validate_ip), _header = Depends(validate_secret_header)):
    console_logger.info("Request received for Surge Pokemon Daily Stats")
    file_logger.info("Request received for Surge Pokemon Daily Stats")
    return get_task_result(query_daily_surge_api_pokemon_stats)

@fastapi.get("/api/surge-weekly-stats")
@cache(expire=app_config.api_surge_weekly_cache)
async def surge_weekly_pokemon_stats(request: Request, secret: str= Depends(validate_secret), _ip = Depends(validate_ip), _header = Depends(validate_secret_header)):
    console_logger.info("Request received for Surge Pokemon Weekly Stats")
    file_logger.info("Request received for Surge Pokemon Weekly Stats")
    return get_task_result(query_weekly_surge_api_pokemon_stats)

@fastapi.get("/api/surge-monthly-stats")
@cache(expire=app_config.api_surge_monthly_cache)
async def surge_monthly_pokemon_stats(request: Request, secret: str= Depends(validate_secret), _ip = Depends(validate_ip), _header = Depends(validate_secret_header)):
    console_logger.info("Request received for Surge Pokemon Monthly Stats")
    file_logger.info("Request received for Surge Pokemon Monthly Stats")
    return get_task_result(query_monthly_surge_api_pokemon_stats)

@fastapi.get("/metrics")
async def metrics(request: Request, secret: str = Depends(validate_secret), _ip = Depends(validate_ip), _header = Depends(validate_secret_header)):
    try:
        # Fetching data from each API task
        daily_area_stats = get_task_result(query_daily_api_pokemon_stats)
        weekly_stats = get_task_result(query_weekly_api_pokemon_stats)
        weekly_area_stats = get_task_result(query_weekly_api_pokemon_stats)
        monthly_area_stats = get_task_result(query_monthly_api_pokemon_stats)
        hourly_total_stats = get_task_result(query_hourly_total_api_pokemon_stats)
        daily_total_stats = get_task_result(query_daily_total_api_pokemon_stats)
        total_stats = get_task_result(query_total_api_pokemon_stats)
        surge_daily_stats = get_task_result(query_daily_surge_api_pokemon_stats)
        surge_weekly_stats = get_task_result(query_weekly_surge_api_pokemon_stats)
        surge_monthly_stats = get_task_result(query_monthly_surge_api_pokemon_stats)
        console_logger.info(f"Fetched all API tasks sucessfuly")
        file_logger.info(f"Fetched all API tasks sucessfuly")

        # Format each result set
        formatted_daily_area_stats = format_results_to_victoria(daily_area_stats)
        console_logger.info(f"Formatted daily area stats for VictoriaMetrics.")
        file_logger.info(f"Formatted daily area stats for VictoriaMetrics.")

        formatted_weekly_stats = format_results_to_victoria(weekly_stats)
        console_logger.info(f"Formatted weekly stats for VictoriaMetrics")
        file_logger.info(f"Formatted weekly stats for VictoriaMetrics")

        formatted_weekly_area_stats = format_results_to_victoria(weekly_area_stats)
        console_logger.info(f"Formatted weekly area stats for VictoriaMetrics")
        file_logger.info(f"Formatted weekly area stats for VictoriaMetrics")

        formatted_monthly_area_stats = format_results_to_victoria(monthly_area_stats)
        console_logger.info(f"Formatted monthly area stats for VictoriaMetrics")
        file_logger.info(f"Formatted monthly area stats for VictoriaMetrics")

        formatted_hourly_total_stats = format_results_to_victoria(hourly_total_stats)
        console_logger.info(f"Formatted hourly total stats for VictoriaMetrics")
        file_logger.info(f"Formatted hourly total stats for VictoriaMetrics")

        formatted_daily_total_stats = format_results_to_victoria(daily_total_stats)
        console_logger.info(f"Formatted daily total stats for VictoriaMetrics")
        file_logger.info(f"Formatted daily total stats for VictoriaMetrics")

        formatted_total_stats = format_results_to_victoria(total_stats)
        console_logger.info(f"Formatted total stats for VictoriaMetrics: {formatted_total_stats}")
        file_logger.info(f"Formatted total stats for VictoriaMetrics")

        formatted_surge_daily_stats = format_results_to_victoria_by_hour(surge_daily_stats, 'surge_daily')
        console_logger.info(f"Formatted surge daily stats for VictoriaMetrics: {formatted_surge_daily_stats}")
        file_logger.info(f"Formatted surge daily stats for VictoriaMetrics: {formatted_surge_daily_stats}")

        formatted_surge_weekly_stats = format_results_to_victoria_by_hour(surge_weekly_stats, 'surge_weekly')
        console_logger.info(f"Formatted surge weekly stats for VictoriaMetrics")
        file_logger.info(f"Formatted surge weekly stats for VictoriaMetrics")

        formatted_surge_monthly_stats = format_results_to_victoria_by_hour(surge_monthly_stats, 'surge_monthly')
        console_logger.info(f"Formatted monthly weekly stats for VictoriaMetrics")
        file_logger.info(f"Formatted monthly weekly stats for VictoriaMetrics")

        # Combine all formatted metrics
        prometheus_metrics = '\n'.join([
            formatted_daily_area_stats, 
            formatted_weekly_stats, 
            formatted_weekly_area_stats, 
            formatted_monthly_area_stats,
            formatted_hourly_total_stats, 
            formatted_daily_total_stats, 
            formatted_total_stats,
            formatted_surge_daily_stats, 
            formatted_surge_weekly_stats, 
            formatted_surge_monthly_stats
        ])

        # Return as plain text
        return Response(content=prometheus_metrics, media_type="text/plain")
    except Exception as e:
        console_logger.info(f"Error generating metrics: {e}")
        file_logger.info(f"Error generating metrics: {e}")
        return Response(content=f"Error generating metrics: {e}", media_type="text/plain", status_code=500)


# Organises for VictoriaMetrics
def format_results_to_victoria(data):
    prometheus_metrics = []
    for area_name, stats_list in data.items():
        for row in stats_list:
            area_name_formatted = area_name.replace('-', '_').replace(' ', '_').lower()
            area_label = "area=\"" + area_name_formatted +"\""

            # Create a Victoria metric line for each column (now key) in the row
            for key, value in row.items():
                if key == 'area_name' or value is None  or (isinstance(value, str) and not value.isdigit()):
                    continue
                metric_name = f'pokemon_stats_{key}'
                prometheus_metric_line = f'{metric_name}{{{area_label}}} {value}'
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
            hour_label = f"hour=\"{hour_formatted}\""

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