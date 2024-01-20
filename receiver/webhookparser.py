import logging
from logging.handlers import RotatingFileHandler
from logging import StreamHandler
from fastapi import FastAPI, Request, HTTPException
from fastapi.responses import RedirectResponse
import json
from shapely.geometry import Point, Polygon
import requests
import os
import datetime
import asyncio
from processor.celery_app import celery
from config.app_config import app_config
from processor.tasks import insert_data_task, generate_unique_id
from threading import Lock, Thread
import time
from cachetools import TTLCache
import httpx
import backoff

# Setup the FastAPI app
webhook_processor = FastAPI()

# Caching geofences
geofence_cache = TTLCache(maxsize=app_config.max_size_geofence, ttl=app_config.cache_geofences)

# Data processing queue
is_processing_queue = False
data_queue = []
data_queue_lock = asyncio.Lock()


# Configuration values
console_log_level_str = app_config.webhook_console_log_level.upper()
log_level_str = app_config.webhook_log_level.upper()
log_file = app_config.webhook_log_file
max_bytes = app_config.webhook_log_max_bytes
backup_count = app_config.webhook_max_log_files

# Console Logger
console_logger = logging.getLogger("webhook_console_logger")
if console_log_level_str == "OFF":
    console_logger.disabled = True
else:
    console_log_level = getattr(logging, console_log_level_str, logging.INFO)
    console_logger.setLevel(console_log_level)
    # Handler
    console_handler = logging.StreamHandler()
    console_handler.setLevel(console_log_level)
    # Formatter
    console_formatter = logging.Formatter('[%(asctime)s] %(levelname)s in %(module)s: %(message)s')
    console_handler.setFormatter(console_formatter)
    console_logger.addHandler(console_handler)

# File logger
file_logger = logging.getLogger("webhook_file_logger")
if log_level_str == "OFF":
    log_level = logging.NOTSET
else:
    log_level = getattr(logging, log_level_str, logging.INFO)

file_logger.setLevel(log_level)
if not os.path.exists(os.path.dirname(log_file)):
    os.makedirs(os.path.dirname(log_file))

#handler
file_handler = RotatingFileHandler(log_file, maxBytes=max_bytes, backupCount=backup_count)
file_handler.setLevel(log_level)
# Formatter
file_formatter = logging.Formatter('[%(asctime)s] %(levelname)s in %(module)s: %(message)s')
file_handler.setFormatter(file_formatter)
file_logger.addHandler(file_handler)

refresh_task = None

@webhook_processor.on_event("startup")
async def startup_event():
    global geofence_cache, refresh_task
    try:
        geofences = await fetch_geofences()
        geofence_cache['geofences'] = geofences
        console_logger.info(f"Sucessfully obtained {len(geofences)} geofences.")
        file_logger.info(f"Sucessfully obtained {len(geofences)} geofences.")
        # Cancel existing refresh tasks before creating a new one
        if refresh_task:
            refresh_task.cancel()
        refresh_task = asyncio.create_task(refresh_geofences())
    except httpx.HTTPError as e:
        console_logger.error(f"Failed to fetch geofences: {e}")
        file_Logger.error(f"Failed to fetch geofences: {e}")


@backoff.on_exception(backoff.expo, httpx.HTTPError, max_tries=app_config.max_tries_geofences, jitter=None, factor=app_config.retry_delay_mult_geofences)
async def fetch_geofences():
    async with httpx.AsyncClient() as client:
        headers = {"Authorization": f"Bearer {app_config.bearer_token}"}
        response = await client.get(app_config.geofence_api_url, headers=headers)
        if response.status_code == 200:
            return response.json().get("data", {}).get("features", [])
        else:
            console_logger.error(f"Failed to fetch geofences. Status Code: {response.status_code}")
            file_logger.error(f"Failed to fetch geofences. Status Code: {response.status_code}")
            raise httpx.HTTPError(f"Failed to fetch geofences. Status Code: {response.status_code}")

async def refresh_geofences():
    while True:
        try:
            geofences = await fetch_geofences()
            geofence_cache['geofences'] = geofences
            console_logger.info(f"Successfully refreshed {len(geofences)} geofences.")
            file_logger.info(f"Successfully refreshed {len(geofences)} geofences.")
        except httpx.HTTPError as e:
            console_logger.error(f"Failed to refresh geofences: {e}")
            file_logger.error(f"Failed to refresh geofences: {e}")
        await asyncio.sleep(app_config.refresh_geofences)


def is_inside_geofence(lat, lon, geofences):
    point = Point(lon, lat)
    for geofence in geofences:
        polygon = geofence["geometry"]["coordinates"][0]
        if point.within(Polygon(polygon)):
            return True, geofence.get("properties", {}).get("name", "Unknown")
    return False, None

def calculate_despawn_time(disappear_time, first_seen):
    if disappear_time is None or first_seen is None:
        return None
    time_diff = disappear_time - first_seen
    total_seconds = time_diff // 1
    return total_seconds

async def validate_remote_addr(request: Request):
    if not app_config.allow_webhook_host:
        return
    if request.client.host != app_config.allow_webhook_host:
        raise HTTPException(status_code=403, detail="Access denied")

@webhook_processor.post("/")
def root_post_redirect():
    return RedirectResponse(url="/webhook", status_code=307)

@webhook_processor.post("/webhook")
async def receive_data(request: Request):
    console_logger.debug(f"Received request on path: {request.url.path}")
    file_logger.debug(f"Received request on path: {request.url.path}")
    global data_queue, is_processing_queue, geofence_cache
    await validate_remote_addr(request)
    console_logger.info(f"Queue size before processing: {len(data_queue)}")
    file_logger.info(f"Queue size before processing: {len(data_queue)}")
    async with data_queue_lock: 
        data = await request.json()
 
    geofences = geofence_cache.get('geofences', [])
    if not geofences:
        console_logger.info("No geofences matched.")
        file_logger.info("No geofences matched.")
        return {"status": "info", "message": "No geofences available"}

    def filter_criteria(message):
        required_fields = [
            'pokemon_id', 'form', 'latitude', 'longitude',
            'individual_attack', 'individual_defense', 'individual_stamina'
        ]
        return all(message.get(field) is not None for field in required_fields)

    def extract_pvp_ranks(pvp_data):
        ranks = {f'pvp_{category}_rank': None for category in ['great', 'little', 'ultra']}
        if pvp_data:
            for category in ['great', 'little', 'ultra']:
                category_data = pvp_data.get(category, [])
                ranks[f'pvp_{category}_rank'] = 1 if any(entry.get('rank') == 1 for entry in category_data) else None
        return ranks

    if isinstance(data, list):
        for item in data:
            if item.get('type') == 'pokemon':
                message = item.get('message', {})
                if filter_criteria(message):
                    lat, lon = message.get('latitude'), message.get('longitude')
                    inside, geofence_name = is_inside_geofence(lat, lon, geofences)
                    if inside:
                        ind_attack = message['individual_attack']
                        ind_defense = message['individual_defense']
                        ind_stamina = message['individual_stamina']

                        if ind_attack == ind_defense == ind_stamina == 15:
                            iv_value = 100
                        elif ind_attack == ind_defense == ind_stamina == 0:
                            iv_value = 0
                        else:
                            iv_value = None

                        despawn_time = calculate_despawn_time(
                            message.get('disappear_time'),
                            message.get('first_seen')
                        )
                        filtered_data = {
                            'pokemon_id': message['pokemon_id'],
                            'form': message['form'],
                            'latitude': lat,
                            'longitude': lon,
                            'iv': iv_value,
                            **extract_pvp_ranks(message.get('pvp', {})),
                            'shiny':message['shiny'],
                            'area_name': geofence_name,
                            'despawn_time': despawn_time
                        }

                        item_unique_id = generate_unique_id(filtered_data)
                        data_queue.append((filtered_data, item_unique_id))

                        if len(data_queue) >= app_config.max_queue_size and not is_processing_queue :
                            is_processing_queue = True
                            process_full_queue()
                else:
                    console_logger.debug("Data did not meet filter criteria")
                    file_logger.debug("Data did not meet filter criteria")
            else:
                console_logger.debug(f"Unsupported data type found in payload: {item.get('type')}")
                file_logger.debug(f"Unsupported data type found in payload: {item.get('type')}")
    else:
        console_logger.error("Received data is not in list format")
        file_logger.error("Received data is not in list format")

    console_logger.info(f"Queue size AFTER processing: {len(data_queue)}")
    file_logger.info(f"Queue size AFTER processing: {len(data_queue)}")
    return {"status": "success"}


def process_full_queue():
    global data_queue, is_processing_queue
    console_logger.info(f"Starting full queue processing. Current queue size: {len(data_queue)}")
    file_logger.info(f"Starting full queue processing. Current queue size: {len(data_queue)}")

    retry_count = 0
    while retry_count <= app_config.max_retries:
        try:
            current_batch_data = [item[0] for item in data_queue[:app_config.max_queue_size]]
            current_batch_ids = [item[1] for item in data_queue[:app_config.max_queue_size]]
            batch_unique_id = generate_unique_id(current_batch_ids)

            insert_data_task.delay(current_batch_data, batch_unique_id)
            console_logger.info(f"Processed full queue with unique_id: {batch_unique_id}")
            file_logger.info(f"Processed full queue with unique_id: {batch_unique_id}")
            data_queue = data_queue[app_config.max_queue_size:]
            break
        except Exception as e:
            retry_count += 1
            console_logger.error(f"Error processing queue on attempt {retry_count}: {e}")
            file_logger.error(f"Error processing queue on attempt {retry_count}: {e}")
            time.sleep(app_config.retry_delay)
    
    if retry_count > app_config.max_retries:
        console_logger.error("Maximum retry attempts reached. Unable to process queue")
        file_logger.error("Maximum retry attempts reached. Unable to process queue")
    is_processing_queue = False
    console_logger.info(f"Finished queue processing. Updated queue size: {len(data_queue)}")
    file_logger.info(f"Finished queue processing. Updated queue size: {len(data_queue)}")

def process_remaining_queue_on_shutdown():
    global data_queue
    console_logger.info("Processing remaining items in queue before shutdown.")
    file_logger.info("Processing remaining items in queue before shutdown.")
    while data_queue:
        try:
            current_batch_data = [item[0] for item in data_queue[:app_config.max_queue_size]]
            current_batch_ids = [item[1] for item in data_queue[:app_config.max_queue_size]]
            batch_unique_id = generate_unique_id(current_batch_ids)

            insert_data_task.delay(current_batch_data, batch_unique_id)
            console_logger.info(f"Processed batch with unique_id: {batch_unique_id}")
            file_logger.info(f"Processed batch with unique_id: {batch_unique_id}")

            # Remove processed items from the queue
            data_queue = data_queue[app_config.max_queue_size:]

        except Exception as e:
            console_logger.error(f"Error processing batch during shutdown: {e}")
            file_logger.error(f"Error processing batch during shutdown: {e}")

@webhook_processor.on_event("shutdown")
async def shutdown_event():
    global refresh_task
    console_logger.info("Application shutdown initiated.")
    file_logger.info("Application shutdown initiated.")
    # Cancel refresh task during shutdown
    if refresh_task:
        refresh_task.cancel()
    # Ensure all remaining items in the queue are processed
    async with data_queue_lock:
        await process_remaining_queue_on_shutdown()