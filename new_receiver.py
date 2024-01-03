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
from tasks import insert_data_task

class Config:
    SCHEDULER_API_ENABLED = True

scheduler = APScheduler()

webhook_processor = Flask(__name__)


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

@scheduler.task('interval', id='check_queue', seconds=app_config.flush_interval, misfire_grace_time=900)
def check_queue_size():
    global data_queue
    retries = 0

    while retries <= app_config.max_retries:
        try:
            if len(data_queue) >= app_config.max_queue_size + app_config.extra_flush_threshold:
                num_items_to_flush = len(data_queue)
                insert_data_task.delay(data_queue[:num_items_to_flush].copy())
                data_queue = data_queue[num_items_to_flush:]
                webhook_processor.logger.info(f"Flushed an extra-large batch of size: {num_items_to_flush}")
                break
        except Exception as error:
            webhook_processor.logger.error(f"Error occurred in check_queue_size: {error}")
            retries += 1
            if retries > app_config.max_retries:
                webhook_processor.logger.error("Maximum retries exceeded. Aborting.")
                break
            else:
                webhook_processor.logger.info(f"Retrying... Attempt {retries}/{app_config.max_retries}")
                sleep(app_config.retry_delay)

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
    minutes, seconds = divmod(time_diff, 60)
    return f"{minutes}mins {seconds}secs"

@webhook_processor.before_request
def limit_remote_addr():
    if request.remote_addr != app_config.allow_webhook_host:
        abort(403)  # Forbidden

@webhook_processor.route('/', methods=['POST'])
def root_redirect():
    return redirect(url_for('receive_data'), code=307)

@webhook_processor.route('/webhook', methods=['POST'])
def receive_data():
    global data_queue
    data = request.json
    geofences = fetch_geofences()

    def filter_criteria(message):
        required_fields = [
            'pokemon_id', 'form', 'latitude', 'longitude',
            'individual_attack', 'individual_defense',
            'individual_stamina'
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
                        data_queue.append(filtered_data)

                        if len(data_queue) >= app_config.max_queue_size:
                            insert_data_task.delay(data_queue[:app_config.max_queue_size].copy())
                            data_queue = data_queue[app_config.max_queue_size:]
                        webhook_processor.logger.debug(f"Data matched and saved for geofence: {geofence_name}")
                    else:
                        webhook_processor.logger.debug("Data did not match any geofence")
                else:
                    webhook_processor.logger.debug("Data did not meet filter criteria")
    else:
        webhook_processor.logger.error("Received data is not in list format")

    # Log current queue size for monitoring
    webhook_processor.logger.info(f"Current queue size: {len(data_queue)}")

    return jsonify({"status": "success"}), 200

if __name__ == '__main__':
    webhook_processor.run(debug=True, port=app_config.receiver_port)
