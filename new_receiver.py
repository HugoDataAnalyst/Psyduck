from flask import Flask, request, jsonify, abort, redirect, url_for
import json
from shapely.geometry import Point, Polygon
import requests
import os
import datetime
from celery import Celery
import mysql.connector
from flask_apscheduler import APScheduler
from time import sleep
from celery.utils.log import get_task_logger
import logging
from logging.handlers import RotatingFileHandler

class Config:
    SCHEDULER_API_ENABLED = True

scheduler = APScheduler()

def make_celery(webhook_processor):
    celery = Celery(
        webhook_processor.import_name, 
        backend = webhook_processor.config['CELERY_RESULT_BACKEND'],
        broker = webhook_processor.config['CELERY_BROKER_URL']
    )
    celery.conf.update(webhook_processor.config)
    return celery

def configure_logger(webhook_processor, celery_logger):
    log_level = getattr(logging, webhook_processor.config['CELERY_LOG_LEVEL'].upper(), None)
    log_file = webhook_processor.config['CELERY_LOG_FILE']

    # Create logs directory if it doesn't exist
    if not os.path.exists(os.path.dirname(log_file)):
        os.makedirs(os.path.dirname(log_file))

    # Set up file handler
    file_handler = RotatingFileHandler(log_file, maxBytes=10485760, backupCount=CELERY_MAX_LOG_FILES)
    file_handler.setLevel(log_level)

    # Set up formatter
    formatter = logging.Formatter(
        '[%(asctime)s] %(levelname)s in %(module)s: %(message)s'
    )
    file_handler.setFormatter(formatter)

    # Add handler to the logger
    celery_logger.addHandler(file_handler)
    celery_logger.setLevel(log_level)

webhook_processor = Flask(__name__)

celery_logger = get_task_logger(__name__)
scheduler.init_app(webhook_processor)
scheduler.start()

# Load configuration
with open('config/config.json') as config_file:
    config = json.load(config_file)

GEOFENCE_API_URL = config['GEOFENCE_API_URL']
BEARER_TOKEN = config['BEARER_TOKEN']
ALLOW_WEBHOOK_HOST = config['ALLOW_WEBHOOK_HOST']
RECEIVER_PORT = config['RECEIVER_PORT']
DATABASE_HOST = config['DATABASE_HOST']
DATABASE_PORT = config['DATABASE_PORT']
DATABASE_NAME = config['DATABASE_NAME']
DATABASE_USER = config['DATABASE_USER']
DATABASE_PASSWORD = config['DATABASE_PASSWORD']
MAX_QUEUE_SIZE = config['MAX_QUEUE_SIZE']
EXTRA_FLUSH_THRESHOLD = config['EXTRA_FLUSH_THRESHOLD']
FLUSH_INTERVAL = config['FLUSH_INTERVAL']
MAX_RETRIES = config['MAX_RETRIES']
RETRY_DELAY = config['RETRY_DELAY']
CELERY_MAX_LOG_FILES = config['MAX_LOG_FILES']

webhook_processor.config.from_object(Config())
webhook_processor.config.update(config)
configure_logger(webhook_processor, celery_logger)

celery = make_celery(webhook_processor)

data_queue = []

db_config = {
    'host': config['DATABASE_HOST'],
    'port': config['DATABASE_PORT'],
    'user': config['DATABASE_USER'],
    'password': config['DATABASE_PASSWORD'],
    'database': config['DATABASE_NAME']
}

def configure_flask_logger(webhook_processor):
    log_level = getattr(logging, webhook_processor.config['FLASK_LOG_LEVEL'].upper(), None)
    log_file = webhook_processor.config['FLASK_LOG_FILE']
    max_bytes = webhook_processor.config.get('FLASK_LOG_MAX_BYTES', 10240)  # Default to 10KB
    backup_count = webhook_processor.config.get('FLASK_MAX_LOG_FILES', 5)  # Default to 5 files

    if not os.path.exists(os.path.dirname(log_file)):
        os.makedirs(os.path.dirname(log_file))

    file_handler = RotatingFileHandler(log_file, maxBytes=max_bytes, backupCount=backup_count)
    file_handler.setLevel(log_level)

    formatter = logging.Formatter('[%(asctime)s] %(levelname)s in %(module)s: %(message)s')
    file_handler.setFormatter(formatter)

    webhook_processor.logger.addHandler(file_handler)
    webhook_processor.logger.setLevel(log_level)


configure_flask_logger(webhook_processor)

@scheduler.task('interval', id='check_queue', seconds=FLUSH_INTERVAL, misfire_grace_time=900)
def check_queue_size():
    global data_queue
    retries = 0

    while retries <= MAX_RETRIES:
        try:
            if len(data_queue) >= MAX_QUEUE_SIZE + EXTRA_FLUSH_THRESHOLD:
                num_items_to_flush = len(data_queue)
                insert_data_task.delay(data_queue[:num_items_to_flush].copy())
                data_queue = data_queue[num_items_to_flush:]
                webhook_processor.logger.info(f"Flushed an extra-large batch of size: {num_items_to_flush}")
                break
        except Exception as error:
            celery_logger.error(f"Error occurred in check_queue_size: {error}")
            retries += 1
            if retries > MAX_RETRIES:
                webhook_processor.logger.error("Maximum retries exceeded. Aborting.")
                break
            else:
                webhook_processor.logger.info(f"Retrying... Attempt {retries}/{MAX_RETRIES}")
                sleep(RETRY_DELAY)

@celery.task(bind=True, max_retries=MAX_RETRIES)
def insert_data_task(self, data_batch):
    conn = None
    try:
        conn = mysql.connector.connect(**db_config)
        cursor = conn.cursor()

        insert_query = '''
        INSERT INTO pokemon_sightings (pokemon_id, form, latitude, longitude, iv,
        pvp_great_rank, pvp_little_rank, pvp_ultra_rank, shiny, area_name, despawn_time)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        '''

        for data in data_batch:
            values = (
                data['pokemon_id'], data['form'], data['latitude'], data['longitude'],
                data['iv'], data.get('pvp_great_rank'), data.get('pvp_little_rank'),
                data.get('pvp_ultra_rank'), data['shiny'], data['area_name'],
                data['despawn_time']
            )
            cursor.execute(insert_query, values)
        conn.commit()
        celery_logger.info(f"Successfully inserted {len(data_batch)} records into the database.")
    except mysql.connector.Error as error:
        celery_logger.error(f"Failed to insert record into MySQL table: {error}")
        try:
            # Retry with exponential backoff
            retry_delay = RETRY_DELAY * (2 ** self.request.retries)
            celery_logger.info(f"Retrying in {retry_delay} seconds...")
            self.retry(countdown=retry_delay)
        except self.MaxRetriesExceededError:
            celery_logger.error("Max retries exceeded. Giving up.")
    finally:
        if conn is not None and conn.is_connected():
            cursor.close()
            conn.close()

def fetch_geofences():
    headers = {"Authorization": f"Bearer {BEARER_TOKEN}"}
    response = requests.get(GEOFENCE_API_URL, headers=headers)
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
    if request.remote_addr != ALLOW_WEBHOOK_HOST:
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

                        if len(data_queue) >= MAX_QUEUE_SIZE:
                            insert_data_task.delay(data_queue[:MAX_QUEUE_SIZE].copy())

                            data_queue = data_queue[MAX_QUEUE_SIZE:]
                        webhook_processor.logger.info(f"Data matched and saved for geofence: {geofence_name}")
                    else:
                        webhook_processor.logger.info("Data did not match any geofence")
                else:
                    webhook_processor.logger.info("Data did not meet filter criteria")
    else:
        webhook_processor.logger.error("Received data is not in list format")

    return jsonify({"status": "success"}), 200

if __name__ == '__main__':
    webhook_processor.run(debug=True, port=RECEIVER_PORT)
