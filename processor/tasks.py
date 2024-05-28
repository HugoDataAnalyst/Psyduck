import asyncio
from functools import partial
from .celery_app import celery
import logging
from logging.handlers import RotatingFileHandler
from celery.utils.log import get_task_logger
from config.app_config import app_config
from pymysql.err import OperationalError, ProgrammingError
import os
import hashlib
import json
import redis
from datetime import datetime, date
from orm.queries import DatabaseOperations


# Celery logger
celery_logger = get_task_logger(__name__)
# Redis Initializer
redis_client = redis.StrictRedis.from_url(app_config.redis_url)
# Retrieve configuration values
console_log_level_str = app_config.celery_console_log_level.upper()
log_level_str = app_config.celery_log_level.upper()
log_file = app_config.celery_log_file
max_bytes = app_config.celery_log_max_bytes
backup_count = app_config.celery_max_log_files

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

class CeleryTasks(DatabaseOperations):
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

    @staticmethod
    def default(obj):
        if isinstance(obj, (datetime, date)):
            return obj.isoformat()
        raise TypeError("Type not serializable")

    @staticmethod
    def generate_unique_id(data):
        data_str = json.dumps(data, sort_keys=True, default=CeleryTasks.default)
        return hashlib.md5(data_str.encode()).hexdigest()

    # Pokemon Insert task
    @celery.task(bind=True, max_retries=app_config.max_retries)
    def insert_data_task(self, data_batch, unique_id):
        celery_logger.debug(f"Pokemon Task received with unique_id: {unique_id}")

        if redis_client.get(unique_id):
            celery_logger.debug(f"Duplicate Pokemon task skipped: {unique_id}")
            return "Duplicate Pokemon task skipped"

        redis_client.set(unique_id, 'locked', ex=600)

        try:
            # Log the data batch before insertion
            celery_logger.info(f"Pokémon Data batch to insert: {data_batch}")

            loop = asyncio.get_event_loop()
            instance = DatabaseOperations()
            loop.run_until_complete(instance.insert_pokemon_data(data_batch))
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
            redis_client.delete(unique_id)

    # Quest Insert Task
    @celery.task(bind=True, max_retries=app_config.max_retries)
    def insert_quest_data_task(self, data_batch, unique_id):
        celery_logger.debug(f"Quest Task received with unique_id: {unique_id}")

        if redis_client.get(unique_id):
            celery_logger.debug(f"Duplicate Quest task skipped: {unique_id}")
            return "Duplicate Quest task skipped"

        redis_client.set(unique_id, 'locked', ex=600)

        try:
            # Log the data batch before insertion
            celery_logger.info(f"Quest Data batch to insert: {data_batch}")

            loop = asyncio.get_event_loop()
            instance = DatabaseOperations()
            loop.run_until_complete(instance.insert_quest_data(data_batch))
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
            redis_client.delete(unique_id)

    # Raid Insert Task
    @celery.task(bind=True, max_retries=app_config.max_retries)
    def insert_raid_data_task(self, data_batch, unique_id):
        celery_logger.debug(f"Raid Task received with unique_id: {unique_id}")

        if redis_client.get(unique_id):
            celery_logger.debug(f"Duplicate Raid task skipped: {unique_id}")
            return "Duplicate Raid task skipped"

        redis_client.set(unique_id, 'locked', ex=600)

        try:
            # Log the data batch before insertion
            celery_logger.info(f"Raid Data batch to insert: {data_batch}")

            loop = asyncio.get_event_loop()
            instance = DatabaseOperations()
            loop.run_until_complete(instance.insert_raid_data(data_batch))
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
            redis_client.delete(unique_id)

    # Invasion Insert Task
    @celery.task(bind=True, max_retries=app_config.max_retries)
    def insert_invasion_data_task(self, data_batch, unique_id):
        celery_logger.debug(f"Invasion Task received with unique_id: {unique_id}")

        if redis_client.get(unique_id):
            celery_logger.debug(f"Duplicate Invasion task skipped: {unique_id}")
            return "Duplicate Invasion task skipped"

        redis_client.set(unique_id, 'locked', ex=600)

        try:
            # Log the data batch before insertion
            celery_logger.info(f"Invasion Data batch to insert: {data_batch}")

            loop = asyncio.get_event_loop()
            instance = DatabaseOperations()
            loop.run_until_complete(instance.insert_invasion_data(data_batch))
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
            redis_client.delete(unique_id)

    # API Pokemon grouped
    @celery.task(bind=True, max_retries=app_config.max_retries)
    def query_daily_pokemon_grouped_stats(self):
        try:
            loop = asyncio.get_event_loop()
            instance = DatabaseOperations()
            results = loop.run_until_complete(instance.fetch_daily_pokemon_grouped_stats())
            return organize_results(results)
        except Exception as e:
            self.retry(exc=e, countdown=app_config.retry_delay)

    @celery.task(bind=True, max_retries=app_config.max_retries)
    def query_weekly_pokemon_grouped_stats(self):
        try:
            loop = asyncio.get_event_loop()
            instance = DatabaseOperations()
            results = loop.run_until_complete(instance.fetch_weekly_pokemon_grouped_stats())
            return organize_results(results)
        except Exception as e:
            self.retry(exc=e, countdown=app_config.retry_delay)

    @celery.task(bind=True, max_retries=app_config.max_retries)
    def query_monthly_pokemon_grouped_stats(self):
        try:
            loop = asyncio.get_event_loop()
            instance = DatabaseOperations()
            results = loop.run_until_complete(instance.fetch_monthly_pokemon_grouped_stats())
            return organize_results(results)
        except Exception as e:
            self.retry(exc=e, countdown=app_config.retry_delay)

    # API Pokemon Totals
    @celery.task(bind=True, max_retries=app_config.max_retries)
    def query_hourly_pokemon_total_stats(self):
        try:
            loop = asyncio.get_event_loop()
            instance = DatabaseOperations()
            results = loop.run_until_complete(instance.fetch_hourly_pokemon_total_stats())
            return organize_results(results)
        except Exception as e:
            self.retry(exc=e, countdown=app_config.retry_delay)

    @celery.task(bind=True, max_retries=app_config.max_retries)
    def query_daily_pokemon_total_stats(self):
        try:
            loop = asyncio.get_event_loop()
            instance = DatabaseOperations()
            results = loop.run_until_complete(instance.fetch_daily_pokemon_total_stats())
            return organize_results(results)
        except Exception as e:
            self.retry(exc=e, countdown=app_config.retry_delay)

    @celery.task(bind=True, max_retries=app_config.max_retries)
    def query_pokemon_total_stats(self):
        try:
            loop = asyncio.get_event_loop()
            instance = DatabaseOperations()
            results = loop.run_until_complete(instance.fetch_pokemon_total_stats())
            return organize_results(results)
        except Exception as e:
            self.retry(exc=e, countdown=app_config.retry_delay)

    # API Pokemon Surge's
    @celery.task(bind=True, max_retries=app_config.max_retries)
    def query_daily_surge_api_pokemon_stats(self):
        try:
            loop = asyncio.get_event_loop()
            instance = DatabaseOperations()
            results = loop.run_until_complete(instance.fetch_daily_surge_api_pokemon_stats())
            return organize_results_by_hour(results)
        except Exception as e:
            self.retry(exc=e, countdown=app_config.retry_delay)

    @celery.task(bind=True, max_retries=app_config.max_retries)
    def query_weekly_surge_api_pokemon_stats(self):
        try:
            loop = asyncio.get_event_loop()
            instance = DatabaseOperations()
            results = loop.run_until_complete(instance.fetch_weekly_surge_api_pokemon_stats())
            return organize_results_by_hour(results)
        except Exception as e:
            self.retry(exc=e, countdown=app_config.retry_delay)

    @celery.task(bind=True, max_retries=app_config.max_retries)
    def query_monthly_surge_api_pokemon_stats(self):
        try:
            loop = asyncio.get_event_loop()
            instance = DatabaseOperations()
            results = loop.run_until_complete(instance.fetch_monthly_surge_api_pokemon_stats())
            return organize_results_by_hour(results)
        except Exception as e:
            self.retry(exc=e, countdown=app_config.retry_delay)

    # API Quest Grouped
    @celery.task(bind=True, max_retries=app_config.max_retries)
    def query_daily_quest_grouped_stats_api(self):
        try:
            loop = asyncio.get_event_loop()
            instance = DatabaseOperations()
            results = loop.run_until_complete(instance.fetch_daily_quest_grouped_stats())
            return organize_results(results)
        except Exception as e:
            self.retry(exc=e, countdown=app_config.retry_delay)

    @celery.task(bind=True, max_retries=app_config.max_retries)
    def query_weekly_quest_grouped_stats_api(self):
        try:
            loop = asyncio.get_event_loop()
            instance = DatabaseOperations()
            results = loop.run_until_complete(instance.fetch_weekly_quest_grouped_stats())
            return organize_results(results)
        except Exception as e:
            self.retry(exc=e, countdown=app_config.retry_delay)

    @celery.task(bind=True, max_retries=app_config.max_retries)
    def query_monthly_quest_grouped_stats_api(self):
        try:
            loop = asyncio.get_event_loop()
            instance = DatabaseOperations()
            results = loop.run_until_complete(instance.fetch_monthly_quest_grouped_stats())
            return organize_results(results)
        except Exception as e:
            self.retry(exc=e, countdown=app_config.retry_delay)

    # API Quest Totals
    @celery.task(bind=True, max_retries=app_config.max_retries)
    def query_daily_quest_total_stats_api(self):
        try:
            loop = asyncio.get_event_loop()
            instance = DatabaseOperations()
            results = loop.run_until_complete(instance.fetch_daily_quest_total_stats())
            return organize_results(results)
        except Exception as e:
            self.retry(exc=e, countdown=app_config.retry_delay)

    @celery.task(bind=True, max_retries=app_config.max_retries)
    def query_total_quest_total_stats_api(self):
        try:
            loop = asyncio.get_event_loop()
            instance = DatabaseOperations()
            results = loop.run_until_complete(instance.fetch_total_quest_total_stats())
            return organize_results(results)
        except Exception as e:
            self.retry(exc=e, countdown=app_config.retry_delay)

    # API Raids Grouped
    @celery.task(bind=True, max_retries=app_config.max_retries)
    def query_daily_raid_grouped_stats_api(self):
        try:
            loop = asyncio.get_event_loop()
            instance = DatabaseOperations()
            results = loop.run_until_complete(instance.fetch_daily_raid_grouped_stats())
            return organize_results(results)
        except Exception as e:
            self.retry(exc=e, countdown=app_config.retry_delay)

    @celery.task(bind=True, max_retries=app_config.max_retries)
    def query_weekly_raid_grouped_stats_api(self):
        try:
            loop = asyncio.get_event_loop()
            instance = DatabaseOperations()
            results = loop.run_until_complete(instance.fetch_weekly_raid_grouped_stats())
            return organize_results(results)
        except Exception as e:
            self.retry(exc=e, countdown=app_config.retry_delay)

    @celery.task(bind=True, max_retries=app_config.max_retries)
    def query_monthly_raid_grouped_stats_api(self):
        try:
            loop = asyncio.get_event_loop()
            instance = DatabaseOperations()
            results = loop.run_until_complete(instance.fetch_monthly_raid_grouped_stats())
            return organize_results(results)
        except Exception as e:
            self.retry(exc=e, countdown=app_config.retry_delay)

    # API Raids Totals
    @celery.task(bind=True, max_retries=app_config.max_retries)
    def query_hourly_raid_total_stats_api(self):
        try:
            loop = asyncio.get_event_loop()
            instance = DatabaseOperations()
            results = loop.run_until_complete(instance.fetch_hourly_raid_total_stats())
            return organize_results(results)
        except Exception as e:
            self.retry(exc=e, countdown=app_config.retry_delay)

    @celery.task(bind=True, max_retries=app_config.max_retries)
    def query_daily_raid_total_stats_api(self):
        try:
            loop = asyncio.get_event_loop()
            instance = DatabaseOperations()
            results = loop.run_until_complete(instance.fetch_daily_raid_total_stats())
            return organize_results(results)
        except Exception as e:
            self.retry(exc=e, countdown=app_config.retry_delay)

    @celery.task(bind=True, max_retries=app_config.max_retries)
    def query_total_raid_total_stats_api(self):
        try:
            loop = asyncio.get_event_loop()
            instance = DatabaseOperations()
            results = loop.run_until_complete(instance.fetch_total_raid_total_stats())
            return organize_results(results)
        except Exception as e:
            self.retry(exc=e, countdown=app_config.retry_delay)

    # API Invasion Grouped
    @celery.task(bind=True, max_retries=app_config.max_retries)
    def query_daily_invasion_grouped_stats_api(self):
        try:
            loop = asyncio.get_event_loop()
            instance = DatabaseOperations()
            results = loop.run_until_complete(instance.fetch_daily_invasion_grouped_stats())
            return organize_results(results)
        except Exception as e:
            self.retry(exc=e, countdown=app_config.retry_delay)

    @celery.task(bind=True, max_retries=app_config.max_retries)
    def query_weekly_invasion_grouped_stats_api(self):
        try:
            loop = asyncio.get_event_loop()
            instance = DatabaseOperations()
            results = loop.run_until_complete(instance.fetch_weekly_invasion_grouped_stats())
            return organize_results(results)
        except Exception as e:
            self.retry(exc=e, countdown=app_config.retry_delay)

    @celery.task(bind=True, max_retries=app_config.max_retries)
    def query_monthly_invasion_grouped_stats_api(self):
        try:
            loop = asyncio.get_event_loop()
            instance = DatabaseOperations()
            results = loop.run_until_complete(instance.fetch_monthly_invasion_grouped_stats())
            return organize_results(results)
        except Exception as e:
            self.retry(exc=e, countdown=app_config.retry_delay)

    # API Invasion Totals
    @celery.task(bind=True, max_retries=app_config.max_retries)
    def query_hourly_invasions_total_stats_api(self):
        try:
            loop = asyncio.get_event_loop()
            instance = DatabaseOperations()
            results = loop.run_until_complete(instance.fetch_hourly_invasion_total_stats())
            return organize_results(results)
        except Exception as e:
            self.retry(exc=e, countdown=app_config.retry_delay)

    @celery.task(bind=True, max_retries=app_config.max_retries)
    def query_daily_invasions_total_stats_api(self):
        try:
            loop = asyncio.get_event_loop()
            instance = DatabaseOperations()
            results = loop.run_until_complete(instance.fetch_daily_invasion_total_stats())
            return organize_results(results)
        except Exception as e:
            self.retry(exc=e, countdown=app_config.retry_delay)

    @celery.task(bind=True, max_retries=app_config.max_retries)
    def query_total_invasions_total_stats_api(self):
        try:
            loop = asyncio.get_event_loop()
            instance = DatabaseOperations()
            results = loop.run_until_complete(instance.fetch_total_invasion_total_stats())
            return organize_results(results)
        except Exception as e:
            self.retry(exc=e, countdown=app_config.retry_delay)

    # API Pokemon TTH
    @celery.task(bind=True, max_retries=app_config.max_retries)
    def query_hourly_pokemon_tth_stats_api(self):
        try:
            loop = asyncio.get_event_loop()
            instance = DatabaseOperations()
            results = loop.run_until_complete(instance.fetch_hourly_pokemon_tth_stats())
            # Hourly is by Area
            return organize_results(results)
        except Exception as e:
            self.retry(exc=e, countdown=app_config.retry_delay)

    @celery.task(bind=True, max_retries=app_config.max_retries)
    def query_daily_pokemon_tth_stats_api(self):
        try:
            loop = asyncio.get_event_loop()
            instance = DatabaseOperations()
            results = loop.run_until_complete(instance.fetch_daily_pokemon_tth_stats())
            # Daily is by hour
            return organize_results_by_hour(results)
        except Exception as e:
            self.retry(exc=e, countdown=app_config.retry_delay)
