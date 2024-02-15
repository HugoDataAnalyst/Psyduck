from .celery_app import celery
import logging
from logging.handlers import RotatingFileHandler
from celery.utils.log import get_task_logger
from config.app_config import app_config
import pymysql
from pymysql.err import OperationalError, ProgrammingError
import os
import hashlib
import json
import redis
from datetime import datetime, date

# Retrieve configuration values
console_log_level_str = app_config.celery_console_log_level.upper()
log_level_str = app_config.celery_log_level.upper()
log_file = app_config.celery_log_file
max_bytes = app_config.celery_log_max_bytes
backup_count = app_config.celery_max_log_files

# Celery logger
celery_logger = get_task_logger(__name__)

# Console logger setup
if console_log_level_str == "OFF":
    celery_logger.disabled = True
else:
    console_log_level = getattr(logging, console_log_level_str, logging.INFO)
    console_handler = logging.StreamHandler()
    console_handler.setLevel(console_log_level)
    console_formatter = logging.Formatter('[%(asctime)s] %(levelname)s in %(module)s: %(message)s')
    console_handler.setFormatter(console_formatter)
    celery_logger.addHandler(console_handler)

# File logger setup
if log_level_str != "OFF":
    log_level = getattr(logging, log_level_str, logging.INFO)
    celery_logger.setLevel(log_level)
    if not os.path.exists(os.path.dirname(log_file)):
        os.makedirs(os.path.dirname(log_file))
    file_handler = RotatingFileHandler(log_file, maxBytes=max_bytes, backupCount=backup_count)
    file_handler.setLevel(log_level)
    file_formatter = logging.Formatter('[%(asctime)s] %(levelname)s in %(module)s: %(message)s')
    file_handler.setFormatter(file_formatter)
    celery_logger.addHandler(file_handler)

# Database configuration
db_config = {
    'host': app_config.db_host,
    'port': app_config.db_port,
    'user': app_config.db_user,
    'password': app_config.db_password,
    'database': app_config.db_name
}

redis_client = redis.StrictRedis.from_url(app_config.redis_url)

def generate_unique_id(data):
    data_str = json.dumps(data, sort_keys=True)
    return hashlib.md5(data_str.encode()).hexdigest()

# Pokemon Insert task
@celery.task(bind=True, max_retries=app_config.max_retries)
def insert_data_task(self, data_batch, unique_id):
    celery_logger.debug(f"Pokemon Task received with unique_id: {unique_id}")

    if redis_client.get(unique_id):
        celery_logger.debug(f"Duplicate Pokemon task skipped: {unique_id}")
        return "Duplicate Pokemon task skipped"

    redis_client.set(unique_id, 'locked', ex=600)

    conn = None
    try:
        conn = pymysql.connect(**db_config)
        cursor = conn.cursor()
        celery_logger.debug(f"Inserting Pokemon data for unique_id: {unique_id}")

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
        num_records = len(data_batch)
        celery_logger.info(f"Successfully inserted {num_records} Pokemon records into the database for unique_id: {unique_id}")
        return f"Inserted {num_records} Pokemon records"
    except OperationalError as error:
        celery_logger.error(f"Failed to insert Pokemon record into MySQL table: {error}")
        try:
            # Retry with exponential backoff
            retry_delay = app_config.retry_delay * (2 ** self.request.retries)
            celery_logger.debug(f"Retrying Pokemon Insertion in {retry_delay} seconds...")
            self.retry(countdown=retry_delay)
        except self.MaxRetriesExceededError:
            celery_logger.debug("Max Pokemon retries exceeded. Giving up.")
    finally:
        if conn is not None and conn.open:
            cursor.close()
            conn.close()
        redis_client.delete(unique_id)

# Quest Insert Task
@celery.task(bind=True, max_retries=app_config.max_retries)
def insert_quest_data_task(self, data_batch, unique_id)
    celery_logger.debug(f"Quest Task received with unique_id: {unique_id}")

    if redis_client.get(unique_id):
        celery_logger.debug(f"Duplicate Quest task skipped: {unique_id}")
        return "Duplicate Quest task skipped"

    redis_client.set(unique_id, 'locked', ex=600)

    conn = None
    try:
        conn = pymysql.connect(**db_config)
        cursor = conn.cursor()
        celery_logger.debug(f"Inserting Quest data for unique_id: {unique_id}")

        insert_query = '''
        INSERT INTO quest_sightings (pokestop_id, ar_type, normal_type, reward_ar_type, reward_normal_type,
        reward_ar_item_id, reward_ar_item_amount, reward_normal_item_id, reward_normal_item_amount,
        reward_ar_poke_id, reward_ar_poke_form, reward_normal_poke_id, reward_normal_poke_form, area_name)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        '''

        for data in data_batch:
            values = (
                data['pokestop_id'], data.get('ar_type', None), data.get('normal_type', None),
                data.get('reward_ar_type', None), data.get('reward_normal_type', None),
                data.get('reward_ar_item_id', None), data.get('reward_ar_item_amount', None),
                data.get('reward_normal_item_id', None), data.get('reward_normal_item_amount', None),
                data.get('reward_ar_poke_id', None), data.get('reward_ar_poke_form', None),
                data.get('reward_normal_poke_id', None), data.get('reward_normal_poke_form', None),
                data['area_name']
            )
            cursor.execute(insert_query, values)
        conn.commit()
        num_records = len(data_batch)
        celery_logger.info(f"Successfully inserted {num_records} Quest records into the database for unique_id: {unique_id}")
        return f"Inserted {num_records} Quest records"
    except OperationalError as error:
        celery_logger.error(f"Failed to insert Quest record into MySQL table: {error}")
        try:
            # Retry with exponential backoff
            retry_delay = app_config.retry_delay * (2 ** self.request.retries)
            celery_logger.debug(f"Retrying Quest Insertion in {retry_delay} seconds...")
            self.retry(countdown=retry_delay)
        except self.MaxRetriesExceededError:
            celery_logger.debug("Max Quest retries exceeded. Giving up.")
    finally:
        if conn is not None and conn.open:
            cursor.close()
            conn.close()
        redis_client.delete(unique_id)

# Raid Insert Task
@celery.task(bind=True, max_retries=app_config.max_retries)
def insert_raid_data_task(self, data_batch, unique_id)
    celery_logger.debug(f"Raid Task received with unique_id: {unique_id}")

    if redis_client.get(unique_id):
        celery_logger.debug(f"Duplicate Raid task skipped: {unique_id}")
        return "Duplicate Raid task skipped"

    redis_client.set(unique_id, 'locked', ex=600)

    conn = None
    try:
        conn = pymysql.connect(**db_config)
        cursor = conn.cursor()
        celery_logger.debug(f"Inserting Raid data for unique_id: {unique_id}")

        insert_query = '''
        INSERT INTO raid_sightings (gym_id, ex_raid_eligible, is_exclusive, level, pokemon_id, form, costume, area_name)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        '''

        for data in data_batch:
            values = (
                data['gym_id'],
                data['ex_raid_eligible'],
                data['is_exclusive'],
                data['level'],
                data['pokemon_id'],
                data.get('form', ''),
                data.get('costume', ''),
                data['area_name']
            )
            cursor.execute(insert_query, values)
        conn.commit()
        num_records = len(data_batch)
        celery_logger.info(f"Successfully inserted {num_records} Raid records into the database for unique_id: {unique_id}")
        return f"Inserted {num_records} Raid records"
    except OperationalError as error:
        celery_logger.error(f"Failed to insert Raid record into MySQL table: {error}")
        try:
            # Retry with exponential backoff
            retry_delay = app_config.retry_delay * (2 ** self.request.retries)
            celery_logger.debug(f"Retrying Raid Insertion in {retry_delay} seconds...")
            self.retry(countdown=retry_delay)
        except self.MaxRetriesExceededError:
            celery_logger.debug("Max Raid retries exceeded. Giving up.")
    finally:
        if conn is not None and conn.open:
            cursor.close()
            conn.close()
        redis_client.delete(unique_id)

# Invasion Insert Task
@celery.task(bind=True, max_retries=app_config.max_retries)
def insert_invasion_data_task(self, data_batch, unique_id)
    celery_logger.debug(f"Invasion Task received with unique_id: {unique_id}")

    if redis_client.get(unique_id):
        celery_logger.debug(f"Duplicate Invasion task skipped: {unique_id}")
        return "Duplicate Invasion task skipped"

    redis_client.set(unique_id, 'locked', ex=600)

    conn = None
    try:
        conn = pymysql.connect(**db_config)
        cursor = conn.cursor()
        celery_logger.debug(f"Inserting Invasion data for unique_id: {unique_id}")

        insert_query = '''
        INSERT INTO invasion_sightings (pokestop_id, display_type, character, confirmed, area_name)
        VALUES (%s, %s, %s, %s, %s)
        '''

        for data in data_batch:
            values = (
                data['pokestop_id'],
                data['display_type'],
                data['character'],
                data['confirmed'],
                data.get('area_name', '')
            )
            cursor.execute(insert_query, values)
        conn.commit()
        num_records = len(data_batch)
        celery_logger.info(f"Successfully inserted {num_records} Invasion records into the database for unique_id: {unique_id}")
        return f"Inserted {num_records} Invasion records"
    except OperationalError as error:
        celery_logger.error(f"Failed to insert Invasion record into MySQL table: {error}")
        try:
            # Retry with exponential backoff
            retry_delay = app_config.retry_delay * (2 ** self.request.retries)
            celery_logger.debug(f"Retrying Invasion Insertion in {retry_delay} seconds...")
            self.retry(countdown=retry_delay)
        except self.MaxRetriesExceededError:
            celery_logger.debug("Max Invasion retries exceeded. Giving up.")
    finally:
        if conn is not None and conn.open:
            cursor.close()
            conn.close()
        redis_client.delete(unique_id)

# API query task
def execute_query(query, params=None):
    try:
        conn = pymysql.connect(**db_config)
        cursor = conn.cursor(pymysql.cursors.DictCursor)
        cursor.execute(query, params or ())
        return cursor.fetchall()
    except OperationalError as err:
        celery_logger.error(f"Database query error: {err}")
        raise
    finally:
        if conn and conn.open:
            cursor.close()
            conn.close()

# API grouped
@celery.task(bind=True, max_retries=app_config.max_retries)
def query_daily_api_pokemon_stats(self):
    try:
        results = execute_query("SELECT * FROM daily_api_pokemon_stats ORDER BY area_name, pokemon_id")
        return organize_results(results)
    except Exception as e:
        self.retry(exc=e, countdown=app_config.retry_delay)

@celery.task(bind=True, max_retries=app_config.max_retries)
def query_weekly_api_pokemon_stats(self):
    try:
        results = execute_query("SELECT * FROM weekly_api_pokemon_stats ORDER BY area_name, pokemon_id")
        return organize_results(results)
    except Exception as e:
        self.retry(exc=e, countdown=app_config.retry_delay)

@celery.task(bind=True, max_retries=app_config.max_retries)
def query_monthly_api_pokemon_stats(self):
    try:
        results = execute_query("SELECT * FROM monthly_api_pokemon_stats ORDER BY area_name, pokemon_id")
        return organize_results(results)
    except Exception as e:
        self.retry(exc=e, countdown=app_config.retry_delay)

# API Totals
@celery.task(bind=True, max_retries=app_config.max_retries)
def query_hourly_total_api_pokemon_stats(self):
    try:
        results = execute_query("SELECT * FROM hourly_total_api_pokemon_stats ORDER BY area_name")
        return organize_results(results)
    except Exception as e:
        self.retry(exc=e, countdown=app_config.retry_delay)

@celery.task(bind=True, max_retries=app_config.max_retries)
def query_daily_total_api_pokemon_stats(self):
    try:
        results = execute_query("SELECT * FROM daily_total_api_pokemon_stats ORDER BY area_name")
        return organize_results(results)
    except Exception as e:
        self.retry(exc=e, countdown=app_config.retry_delay)

@celery.task(bind=True, max_retries=app_config.max_retries)
def query_total_api_pokemon_stats(self):
    try:
        results = execute_query("SELECT * FROM total_api_pokemon_stats ORDER BY area_name")
        return organize_results(results)
    except Exception as e:
        self.retry(exc=e, countdown=app_config.retry_delay)

# API Surge's
@celery.task(bind=True, max_retries=app_config.max_retries)
def query_daily_surge_api_pokemon_stats(self):
    try:
        results = execute_query("SELECT * FROM daily_surge_pokemon_stats")
        return organize_results_by_hour(results)
    except Exception as e:
        self.retry(exc=e, countdown=app_config.retry_delay)

@celery.task(bind=True, max_retries=app_config.max_retries)
def query_weekly_surge_api_pokemon_stats(self):
    try:
        results = execute_query("SELECT * FROM weekly_surge_pokemon_stats")
        return organize_results_by_hour(results)
    except Exception as e:
        self.retry(exc=e, countdown=app_config.retry_delay)

@celery.task(bind=True, max_retries=app_config.max_retries)
def query_monthly_surge_api_pokemon_stats(self):
    try:
        results = execute_query("SELECT * FROM monthly_surge_pokemon_stats")
        return organize_results_by_hour(results)
    except Exception as e:
        self.retry(exc=e, countdown=app_config.retry_delay)

# Organises Area based APIs
def organize_results(results):
    organized_results = {}
    for row in results:
        # Directly convert the 'day' column to a string in ISO format
        if 'day' in row and isinstance(row['day'], (date, datetime)):
            row['day'] = row['day'].isoformat()

        area = row['area_name']
        if area not in organized_results:
            organized_results[area] = []
        organized_results[area].append(row)
    return organized_results

# Organises Hour based APIs
def organize_results_by_hour(results):
    organized_results_by_hour = {}
    for row in results:
        hour = row['hour']
        if hour not in organized_results_by_hour:
            organized_results_by_hour[hour] = []
        organized_results_by_hour[hour].append(row)
    return organized_results_by_hour
