import json
import urllib.parse

class AppConfig:
    def __init__(self, config_path='config/config.json'):
        with open(config_path) as config_file:
            config = json.load(config_file)

        encoded_redis_password = urllib.parse.quote(config['redis']['PASSWORD'])

        self.geofence_api_url = config['koji']['GEOFENCE_API_URL']
        self.bearer_token = config['koji']['BEARER_TOKEN']
        self.max_size_geofence = int(config['koji']['MAX_SIZE_GEOFENCE'])
        self.cache_geofences = int(config['koji']['CACHE_GEOFENCES'])
        self.refresh_geofences = int(config['koji']['REFRESH_GEOFENCES'])
        self.max_tries_geofences = int(config['koji']['MAX_TRIES_GEOFENCES'])
        self.retry_delay_mult_geofences = int(config['koji']['RETRY_DELAY_MULT_GEOFENCES'])
        self.allow_webhook_host = config['receiver']['ALLOW_WEBHOOK_HOST']
        self.receiver_host = config['receiver']['HOST']
        self.receiver_port = int(config['receiver']['PORT'])
        self.receiver_workers = int(config['receiver']['WORKERS'])
        self.max_queue_size = int(config['receiver']['MAX_QUEUE_SIZE'])
        self.extra_flush_threshold = int(config['receiver']['EXTRA_FLUSH_THRESHOLD'])
        self.flush_interval = int(config['receiver']['EXTRA_FLUSH_INTERVAL'])
        self.max_retries = int(config['receiver']['MAX_RETRIES'])
        self.retry_delay = int(config['receiver']['RETRY_DELAY'])
        self.webhook_console_log_level = config['receiver']['CONSOLE_LOG_LEVEL']
        self.webhook_log_level = config['receiver']['LOG_LEVEL']
        self.webhook_log_file = config['receiver']['LOG_FILE']
        self.webhook_log_max_bytes = int(config['receiver']['LOG_MAX_BYTES'])
        self.webhook_max_log_files = int(config['receiver']['MAX_LOG_FILES'])
        self.db_host = config['database']['HOST']
        self.db_port = int(config['database']['PORT'])
        self.db_name = config['database']['NAME']
        self.db_user = config['database']['USER']
        self.db_password = config['database']['PASSWORD']
        self.db_clean = config['database']['CLEAN'].lower() == 'true'
        self.migration_log_level = config['database']['MIGRATION_LOG_LEVEL']
        self.migration_log_file = config['database']['MIGRATION_LOG_FILE']
        self.migration_log_max_bytes = int(config['database']['MIGRATION_LOG_MAX_BYTES'])
        self.migration_max_log_files = int(config['database']['MIGRATION_MAX_LOG_FILES'])
        self.celery_broker_url = f"redis://:{encoded_redis_password}@{config['redis']['HOST']}:{config['redis']['PORT']}/{config['redis']['DB']}"
        self.celery_result_backend = f"redis://:{encoded_redis_password}@{config['redis']['HOST']}:{config['redis']['PORT']}/{config['redis']['DB']}"
        self.celery_console_log_level = config['celery']['CONSOLE_LOG_LEVEL']
        self.celery_log_level = config['celery']['LOG_LEVEL']
        self.celery_log_file = config['celery']['LOG_FILE']
        self.celery_log_max_bytes = int(config['celery']['LOG_MAX_BYTES'])
        self.celery_max_log_files = int(config['celery']['MAX_LOG_FILES'])
        self.celery_workers = int(config['celery']['WORKERS'])
        self.redis_host = config['redis']['HOST']
        self.redis_port = int(config['redis']['PORT'])
        self.redis_db = config['redis']['DB']
        self.redis_url = f"redis://:{encoded_redis_password}@{config['redis']['HOST']}:{config['redis']['PORT']}/{config['redis']['DB']}"
        self.api_log_level = config['api']['LOG_LEVEL']
        self.api_console_log_level = config['api']['CONSOLE_LOG_LEVEL']
        self.api_log_file = config['api']['LOG_FILE']
        self.api_log_max_bytes = int(config['api']['LOG_MAX_BYTES'])
        self.api_max_log_files = int(config['api']['MAX_LOG_FILES'])
        self.api_host = config['api']['HOST']
        self.api_port = int(config['api']['PORT'])
        self.api_workers = int(config['api']['WORKERS'])
        self.api_secret_key = config['api']['SECRET_KEY']
        self.api_secret_header_key = config['api']['SECRET_HEADER_KEY']
        self.api_daily_pokemon_cache = int(config['api']['DAILY_POKEMON_CACHE'])
        self.api_weekly_pokemon_cache = int(config['api']['WEEKLY_POKEMON_CACHE'])
        self.api_monthly_pokemon_cache = int(config['api']['MONTHLY_POKEMON_CACHE'])
        self.api_hourly_total_pokemon_cache = int(config['api']['HOURLY_TOTAL_POKEMON_CACHE'])
        self.api_daily_total_pokemon_cache = int(config['api']['DAILY_TOTAL_POKEMON_CACHE'])
        self.api_total_pokemon_cache = int(config['api']['TOTAL_POKEMON_CACHE'])
        self.api_surge_daily_cache = int(config['api']['SURGE_DAILY_CACHE'])
        self.api_surge_weekly_cache = int(config['api']['SURGE_WEEKLY_CACHE'])
        self.api_surge_monthly_cache = int(config['api']['SURGE_MONTHLY_CACHE'])
        self.api_metrics_daily_area_pokemon_cache = int(config['api']['METRICS_DAILY_AREA_POKEMON_CACHE'])
        self.api_metrics_weekly_area_pokemon_cache = int(config['api']['METRICS_WEEKLY_AREA_POKEMON_CACHE'])
        self.api_metrics_monthly_area_pokemon_cache = int(config['api']['METRICS_MONTHLY_AREA_POKEMON_CACHE'])
        self.api_metrics_hourly_total_pokemon_cache = int(config['api']['METRICS_HOURLY_TOTAL_POKEMON_CACHE'])
        self.api_metrics_daily_total_pokemon_cache = int(config['api']['METRICS_DAILY_TOTAL_POKEMON_CACHE'])
        self.api_metrics_total_pokemon_cache = int(config['api']['METRICS_TOTAL_POKEMON_CACHE'])
        self.api_metrics_surge_daily_pokemon_cache = int(config['api']['METRICS_SURGE_DAILY_POKEMON_CACHE'])
        self.api_metrics_surge_weekly_pokemon_cache = int(config['api']['METRICS_SURGE_WEEKLY_POKEMON_CACHE'])
        self.api_metrics_surge_monthly_pokemon_cache = int(config['api']['METRICS_SURGE_MONTHLY_POKEMON_CACHE'])
        self.api_ip_restriction = config['api']['IP_RESTRICTION'].lower() == 'true'
        self.api_allowed_ips = config['api']['ALLOWED_IPS'].split(", ")
        self.api_header_name = config['api']['HEADER_NAME']
        self.api_path_restriction = config['api']['PATH_RESTRICTION'].lower() == 'true'

app_config = AppConfig()