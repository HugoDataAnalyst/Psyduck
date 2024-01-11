import logging
from logging.handlers import RotatingFileHandler
from logging import StreamHandler
from apscheduler.schedulers.background import BackgroundScheduler
from fastapi import FastAPI, Request, HTTPException
from fastapi.responses import RedirectResponse
import json
from shapely.geometry import Point, Polygon
import requests
import os
import datetime
from processor.celery_app import celery
from config.app_config import app_config
from processor.tasks import insert_data_task, generate_unique_id
from threading import Lock, Thread
import time

# Setup the FastAPI app
webhook_processor = FastAPI()

# Scheduler

scheduler = BackgroundScheduler()

# Data processing queue
data_queue_lock = Lock()
is_processing_queue = False
data_queue = []

# Configure logger
logger = logging.getLogger("webhook_logger")
log_level = getattr(logging, app_config.webhook_log_level.upper(), None)
log_file = app_config.webhook_log_file
max_bytes = app_config.webhook_log_max_bytes
backup_count = app_config.webhook_max_log_files

if not os.path.exists(os.path.dirname(log_file)):
    os.makedirs(os.path.dirname(log_file))

file_handler = RotatingFileHandler(log_file, maxBytes=max_bytes, backupCount=backup_count)
file_handler.setLevel(log_level)

console_handler = StreamHandler()
console_handler.setLevel(log_level)

formatter = logging.Formatter('[%(asctime)s] %(levelname)s in %(module)s: %(message)s')
file_handler.setFormatter(formatter)
console_handler.setFormatter(formatter)

logger.addHandler(file_handler)
logger.addHandler(console_handler)
logger.setLevel(log_level)

def fetch_geofences():
    headers = {"Authorization": f"Bearer {app_config.bearer_token}"}
    response = requests.get(app_config.geofence_api_url, headers=headers)
    if response.status_code == 200:
        return response.json().get("data", {}).get("features", [])
    else:
        logger.error(f"Failed to fetch geofences. Status Code: {response.status_code}")
        return []

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
    if request.client.host != app_config.allow_webhook_host:
        raise HTTPException(status_code=403, detail="Access denied")

@webhook_processor.post("/")
def root_post_redirect():
    return RedirectResponse(url="/webhook", status_code=307)

@webhook_processor.post("/webhook")
async def receive_data(request: Request):
    logger.debug(f"Received request on path: {request.url.path}")
    global data_queue, is_processing_queue
    await validate_remote_addr(request)

    with data_queue_lock: 
        data = await request.json()
        geofences = fetch_geofences()
 
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
                    logger.debug("Data did not meet filter criteria")
            else:
                logger.debug(f"Unsupported data type found in payload: {item.get('type')}")
    else:
        logger.error("Received data is not in list format")

    # Log current queue size for monitoring
    logger.info(f"Current queue size: {len(data_queue)}")

    return {"status": "success"}

def process_full_queue():
    global data_queue, is_processing_queue

    retry_count = 0
    while retry_count <= app_config.max_retries:
        try:
            current_batch_data = [item[0] for item in data_queue[:app_config.max_queue_size]]
            current_batch_ids = [item[1] for item in data_queue[:app_config.max_queue_size]]
            batch_unique_id = generate_unique_id(current_batch_ids)

            insert_data_task.delay(current_batch_data, batch_unique_id)
            logger.info(f"Processed full queue with unique_id: {batch_unique_id}")
            data_queue = data_queue[app_config.max_queue_size:]
            break
        except Exception as e:
            retry_count += 1
            logger.error(f"Error processing queue on attempt {retry_count}: {e}")
            time.sleep(app_config.retry_delay)
    
    if retry_count > app_config.max_retries:
        logger.error("Maximum retry attempts reached. Unable to process queue")
    is_processing_queue = False

def manage_large_queues():
    global data_queue
    with data_queue_lock:
        if len(data_queue) > app_config.extra_flush_threshold:
            process_full_queue()

scheduler.add_job(manage_large_queues, 'interval', seconds=app_config.flush_interval)

scheduler.start()

def process_remaining_queue_on_shutdown():
    global data_queue
    logger.info("Processing remaining items in queue before shutdown.")
    while data_queue:
        try:
            current_batch_data = [item[0] for item in data_queue[:app_config.max_queue_size]]
            current_batch_ids = [item[1] for item in data_queue[:app_config.max_queue_size]]
            batch_unique_id = generate_unique_id(current_batch_ids)

            insert_data_task.delay(current_batch_data, batch_unique_id)
            logger.info(f"Processed batch with unique_id: {batch_unique_id}")

            # Remove processed items from the queue
            data_queue = data_queue[app_config.max_queue_size:]

        except Exception as e:
            logger.error(f"Error processing batch during shutdown: {e}")

@webhook_processor.on_event("shutdown")
async def shutdown_event():
    logger.info("Application shutdown initiated.")
    with data_queue_lock:
        process_remaining_queue_on_shutdown()
