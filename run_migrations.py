import pymysql
from pymysql.err import OperationalError, ProgrammingError
import os
import re
import json
import logging
from logging.handlers import RotatingFileHandler
from config.app_config import app_config
import subprocess

# Configure logging
log_file = app_config.migration_log_file
log_level = getattr(logging, app_config.migration_log_level.upper(), None)

logger = logging.getLogger('migration_logger')
logger.setLevel(log_level)

if not os.path.exists(os.path.dirname(log_file)):
    os.makedirs(os.path.dirname(log_file))

file_handler = RotatingFileHandler(log_file, maxBytes=app_config.migration_log_max_bytes, backupCount=app_config.migration_max_log_files)
file_handler.setLevel(log_level)

formatter = logging.Formatter(
    '[%(asctime)s] %(levelname)s in %(module)s: %(message)s'
)

file_handler.setFormatter(formatter)
logger.addHandler(file_handler)

console_handler = logging.StreamHandler()
console_handler.setLevel(log_level)
console_handler.setFormatter(formatter)
logger.addHandler(console_handler)

# Database configuration
db_config = {
    'host': app_config.db_host,
    'port': app_config.db_port,
    'user': app_config.db_user,
    'password': app_config.db_password,
    'database': app_config.db_name
}

def get_current_version(cursor):
    try:
        cursor.execute("SELECT MAX(version) FROM schema_version")
        result = cursor.fetchone()
        return result[0] if result and result[0] else 0
    except (OperationalError, ProgrammingError) as e:
        logger.warning(f"Error getting current version (assuming initial state): {e}")
        return 0
    except Exception as e:
        logger.error(f"Unexpected error getting current version: {e}")
        raise

def check_schema_version_table(cursor):
    try:
        cursor.execute("SHOW TABLES LIKE 'schema_version'")
        return bool(cursor.fetchone())
    except Exception as e:
        logger.error(f"Error checking schema_version table: {e}")
        raise

def apply_migration(cursor, filename):
    try:
        with open(filename, 'r') as file:
            sql_script = file.read()
            # Remove comments
            sql_script = re.sub(r'(--.*?\n)|(/\*.*?\*/)', '', sql_script, flags=re.DOTALL)

            cursor.execute(sql_script)
            logger.info(f"Executed migration script: {filename}")

    except Exception as e:
        logger.error(f"Error applying migration {filename}: {e}")
        raise

def run_migrations():
    try:
        conn = pymysql.connect(**db_config, client_flag=pymysql.constants.CLIENT.MULTI_STATEMENTS)
        cursor = conn.cursor()
        # Check if the schema_version table exists
        schema_version_exists = check_schema_version_table(cursor)

        if not schema_version_exists:
            # Run create_database_schema.py if the schema_version table does not exist
            logger.info("Running initial schema creation script.")
            try:
                result = subprocess.run(['python', 'SQL/create_database_schema.py'], check=True, capture_output=True, text=True)
                logger.info(result.stdout)
                if result.stderr:
                    logger.error(result.stderr)
            except subprocess.CalledProcessError as e:
                logger.error(f"Error running create_database_schema.py: {e.stderr}")
                raise

        # Get the current schema version
        current_version = get_current_version(cursor)

        migration_files = sorted([f for f in os.listdir('migrations') if f.endswith('.sql')])
        migrations_applied = False

        for file in migration_files:
            version = int(file.split('_')[0])
            if version > current_version:
                apply_migration(cursor, f'migrations/{file}')
                logger.info(f"Updating schema_version to {version}")
                try:
                    cursor.execute("INSERT INTO schema_version (version) VALUES (%s)", (version,))
                    conn.commit()
                except pymysql.Error as e:
                    logger.info(f"Failed to insert version {version} into schema_version: {e}")
                    conn.rollback()
                migrations_applied = True
                logger.info(f"Applied migration {file}")

        if not migrations_applied:
            logger.info(f"No new migrations to apply")

        cursor.close()
        conn.close()

    except pymysql.OperationalError as e:
        logger.error(f"Error in run_migrations: {e}")
        raise

if __name__ == '__main__':
    run_migrations()
