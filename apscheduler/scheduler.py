import asyncio
import subprocess
import os
import logging
from logging.handlers import RotatingFileHandler
from logging import StreamHandler
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger
from config.app_config import app_config

# Configuration values
console_log_level_str = app_config.api_console_log_level.upper()
log_level_str = app_config.api_log_level.upper()
log_file = app_config.api_log_file
max_bytes = app_config.api_log_max_bytes
backup_count = app_config.api_max_log_files

# Console logger
console_logger = logging.getLogger("apscheduler_console_logger")
if console_log_level_str == "OFF":
    console_logger.disabled = True
else:
    console_log_level = getattr(logging, console_log_level_str, logging.INFO)
    console_logger.setLevel(console_log_level)
    #handler
    console_handler = logging.StreamHandler()
    console_handler.setLevel(console_log_level)
    # Formatter
    console_formatter = logging.Formatter('[%(asctime)s] %(levelname)s in %(module)s: %(message)s')
    console_handler.setFormatter(console_formatter)
    console_logger.addHandler(console_handler)

# File logger
file_logger = logging.getLogger("apscheduler_file_logger")
if log_level_str == "OFF":
    log_level = logging.NOTSET
else:
    log_level = getattr(logging, log_level_str, logging.INFO)

file_logger.setLevel(log_level)
if not os.path.exists(os.path.dirname(log_file)):
    os.makedirs(os.path.dirname(log_file))
file_handler = RotatingFileHandler(log_file, maxBytes=max_bytes, backupCount=backup_count)
file_handler.setLevel(log_level)
file_formatter = logging.Formatter('[%(asctime)s] %(levelname)s in %(module)s: %(message)s')
file_handler.setFormatter(file_formatter)
file_logger.addHandler(file_handler)

async def run_example_obtain_total_stops():
    try:
        console_logger.info("Starting btain_total_stops.py script")
        file_logger.info("Starting obtain_total_stops.py script")

        # Run the script asynchronously using subprocess
        process = await asyncio.create_subprocess_exec(
            'python', 'SQL/obtain_total_stops.py',
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE
        )
        stdout, stderr = await process.communicate()
        if stdout:
            console_logger.info(f'[stdout]\n{stdout.decode()}')
            file_logger.info(f'[stdout]\n{stdout.decode()}')
        if stderr:
            console_logger.error(f'[stderr]\n{stderr.decode()}')
            file_logger.error(f'[stderr]\n{stderr.decode()}')

        console_logger.info("Completed obtain_total_stops.py script")
        file_logger.info("Completed obtain_total_stops.py script")
    except Exception as e:
        console_logger.error(f'Error running obtain_total_stops.py: {e}')
        file_logger.error(f'Error running obtain_total_stops.py: {e}')

def start_scheduler():
    scheduler = AsyncIOScheduler()
    job = scheduler.add_job(run_example_obtain_total_stops, CronTrigger(hour=app_config.schedule_hour, minute=app_config.schedule_minute))  # Schedule to run daily at midnight

    # Log when the job is added to the scheduler with exact next run time
    next_run_time = job.next_run_time
    console_logger.info(f"Scheduled obtain_total_stops.py to run next at {next_run_time}")
    file_logger.info(f"Scheduled obtain_total_stops.py to run next at {next_run_time}")

    scheduler.start()
    console_logger.info("Scheduler started")
    file_logger.info("Scheduler started")

if __name__ == "__main__":
    start_scheduler()
    try:
        asyncio.get_event_loop().run_forever()
    except (KeyboardInterrupt, SystemExit):
        console_logger.info("Scheduler stopped")
        file_logger.info("Scheduler stopped")
        pass
