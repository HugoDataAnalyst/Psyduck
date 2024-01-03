from celery_app import celery
import logging
from celery.utils.log import get_task_logger
from app_config import app_config
import mysql.connector
import os

celery_logger = get_task_logger(__name__)

log_level = getattr(logging, app_config.celery_log_level.upper(), None)
log_file = app_config.celery_log_file


# Create logs directory if it doesn't exist
if not os.path.exists(os.path.dirname(log_file)):
    os.makedirs(os.path.dirname(log_file))

# Set up file handler
file_handler = logging.handlers.RotatingFileHandler(
    log_file, maxBytes=app_config.celery_log_max_bytes, backupCount=app_config.celery_max_log_files
)
file_handler.setLevel(log_level)

# Set up formatter
formatter = logging.Formatter(
    '[%(asctime)s] %(levelname)s in %(module)s: %(message)s'
)
file_handler.setFormatter(formatter)

# Add handler to the logger
celery_logger.addHandler(file_handler)
celery_logger.setLevel(log_level)

# Database configuration
db_config = {
    'host': app_config.db_host,
    'port': app_config.db_port,
    'user': app_config.db_user,
    'password': app_config.db_password,
    'database': app_config.db_name
}

@celery.task(bind=True, max_retries=app_config.max_retries)
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
        return f"Inserted {num_records} records"        
    except mysql.connector.Error as error:
        celery_logger.error(f"Failed to insert record into MySQL table: {error}")
        try:
            # Retry with exponential backoff
            retry_delay = app_config.retry_delay * (2 ** self.request.retries)
            celery_logger.info(f"Retrying in {retry_delay} seconds...")
            self.retry(countdown=retry_delay)
        except self.MaxRetriesExceededError:
            celery_logger.error("Max retries exceeded. Giving up.")
    finally:
        if conn is not None and conn.is_connected():
            cursor.close()
            conn.close()