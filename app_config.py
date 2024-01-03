import json

class AppConfig:
    def __init__(self, config_path='config/config.json'):
        with open(config_path) as config_file:
            config = json.load(config_file)
        
        self.geofence_api_url = config['GEOFENCE_API_URL']
        self.bearer_token = config['BEARER_TOKEN']
        self.allow_webhook_host = config['ALLOW_WEBHOOK_HOST']
        self.receiver_port = int(config['RECEIVER_PORT'])
        self.db_host = config['DATABASE_HOST']
        self.db_port = int(config['DATABASE_PORT'])
        self.db_name = config['DATABASE_NAME']
        self.db_user = config['DATABASE_USER']
        self.db_password = config['DATABASE_PASSWORD']
        self.celery_broker_url = config['CELERY_BROKER_URL']
        self.celery_result_backend = config['CELERY_RESULT_BACKEND']
        self.max_queue_size = int(config['MAX_QUEUE_SIZE'])
        self.extra_flush_threshold = int(config['EXTRA_FLUSH_THRESHOLD'])
        self.flush_interval = int(config['FLUSH_INTERVAL'])
        self.max_retries = int(config['MAX_RETRIES'])
        self.retry_delay = int(config['RETRY_DELAY'])
        self.celery_log_level = config['CELERY_LOG_LEVEL']
        self.celery_log_file = config['CELERY_LOG_FILE']
        self.celery_log_max_bytes = int(config['CELERY_LOG_MAX_BYTES'])
        self.celery_max_log_files = int(config['CELERY_MAX_LOG_FILES'])
        self.flask_log_level = config['FLASK_LOG_LEVEL']
        self.flask_log_file = config['FLASK_LOG_FILE']
        self.flask_log_max_bytes = int(config['FLASK_LOG_MAX_BYTES'])
        self.flask_max_log_files = int(config['FLASK_MAX_LOG_FILES'])

# Create a global instance of AppConfig to use throughout the application
app_config = AppConfig()