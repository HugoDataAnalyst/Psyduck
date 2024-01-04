from flask import Flask, request, jsonify, abort, redirect, url_for
import json
from shapely.geometry import Point, Polygon
import requests
import os
import datetime
from celery_app import celery
import mysql.connector
from flask_apscheduler import APScheduler
from time import sleep
import logging
from logging.handlers import RotatingFileHandler
from app_config import app_config
from tasks import insert_data_task, generate_unique_id
from threading import Lock, Thread
import time

class Config:
    SCHEDULER_API_ENABLED = True

scheduler = APScheduler()

webhook_processor = Flask(__name__)

# Data processing queue
data_queue_lock = Lock()
is_processing_queue = False

scheduler.init_app(webhook_processor)
scheduler.start()

webhook_processor.config.from_object(Config())

data_queue = []

def configure_flask_logger(webhook_processor):
    log_level = getattr(logging, app_config.flask_log_level.upper(), None)
    log_file = app_config.flask_log_file
    max_bytes = app_config.flask_log_max_bytes
    backup_count = app_config.flask_max_log_files

    if not os.path.exists(os.path.dirname(log_file)):
        os.makedirs(os.path.dirname(log_file))

    file_handler = RotatingFileHandler(log_file, maxBytes=max_bytes, backupCount=backup_count)
    file_handler.setLevel(log_level)

    formatter = logging.Formatter('[%(asctime)s] %(levelname)s in %(module)s: %(message)s')
    file_handler.setFormatter(formatter)

    webhook_processor.logger.addHandler(file_handler)
    webhook_processor.logger.setLevel(log_level)


configure_flask_logger(webhook_processor)

def fetch_geofences():
    headers = {"Authorization": f"Bearer {app_config.bearer_token}"}
    response = requests.get(app_config.geofence_api_url, headers=headers)
    if response.status_code == 200:
        return response.json().get("data", {}).get("features", [])
    else:
        webhook_processor.logger.error(f"Failed to fetch geofences. Status Code: {response.status_code}")
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

@webhook_processor.before_request
def limit_remote_addr():
    if request.remote_addr != app_config.allow_webhook_host:
        abort(403)  # Forbidden

@webhook_processor.route('/', methods=['POST'])
def root_redirect():
    return redirect(url_for('receive_data'), code=307)

@webhook_processor.route('/webhook', methods=['POST'])
def receive_data():
    global data_queue, is_processing_queue

    with data_queue_lock: 
        data = request.json
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
                    webhook_processor.logger.debug("Data did not meet filter criteria")
            else:
                webhook_processor.logger.debug(f"Unsupported data type found in payload: {item.get('type')}")
    else:
        webhook_processor.logger.error("Received data is not in list format")

    # Log current queue size for monitoring
    webhook_processor.logger.info(f"Current queue size: {len(data_queue)}")

    return jsonify({"status": "success"}), 200

def process_full_queue():
    global data_queue, is_processing_queue

    retry_count = 0
    while retry_count <= app_config.max_retries:
        try:
            current_batch_data = [item[0] for item in data_queue[:app_config.max_queue_size]]
            current_batch_ids = [item[1] for item in data_queue[:app_config.max_queue_size]]
            batch_unique_id = generate_unique_id(current_batch_ids)

            insert_data_task.delay(current_batch_data, batch_unique_id)
            webhook_processor.logger.info(f"Processed full queue with unique_id: {batch_unique_id}")
            data_queue = data_queue[app_config.max_queue_size:]
            break
        except Exception as e:
            retry_count += 1
            webhook_processor.logger.error(f"Error processing queue on attempt {retry_count}: {e}")
            time.sleep(app_config.retry_delay)
    
    if retry_count > app_config.max_retries:
        webhook_processor.logger.error("Maximum retry attempts reached. Unable to process queue")
    is_processing_queue = False

def manage_large_queues():
    global data_queue
    while True:
        time.sleep(app_config.flush_interval)
        with data_queue_lock:
            if len(data_queue) > app_config.extra_flush_threshold:
                process_full_queue()

queue_manager_thread = Thread(target=manage_large_queues, daemon=True)
queue_manager_thread.start()

if __name__ == '__main__':
    webhook_processor.run(debug=False, port=app_config.receiver_port)
