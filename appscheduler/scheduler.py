# scheduler.py
import asyncio
import os
import logging
from logging.handlers import RotatingFileHandler
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger
from apscheduler.triggers.interval import IntervalTrigger
from config.app_config import app_config
from SQL.obtain_total_stops import main as obtain_total_stops_main

scheduler = AsyncIOScheduler()

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
    # handler
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
        console_logger.info("Starting obtain_total_stops.py script")
        file_logger.info("Starting obtain_total_stops.py script")

        await asyncio.to_thread(obtain_total_stops_main)

        console_logger.info("Completed obtain_total_stops.py script")
        file_logger.info("Completed obtain_total_stops.py script")
    except Exception as e:
        console_logger.error(f'Error running obtain_total_stops.py: {e}')
        file_logger.error(f'Error running obtain_total_stops.py: {e}')

async def log_job_next_run_time(job_id):
    job = scheduler.get_job(job_id)
    if job:
        next_run_time = job.next_run_time
        console_logger.info(f"Job '{job_id}' scheduled to run next at {next_run_time}")
        file_logger.info(f"Job '{job_id}' scheduled to run next at {next_run_time}")
    else:
        console_logger.error(f"Failed to find job '{job_id}' for logging its next run time")
        file_logger.error(f"Failed to find job '{job_id}' for logging its next run time")

async def log_all_next_run_times():
    """Logs the next run time for all scheduled jobs."""
    try:
        for job in scheduler.get_jobs():
            try:
                console_logger.info(f"Next run time for '{job.id}' is scheduled at {job.next_run_time}")
                file_logger.info(f"Next run time for '{job.id}' is scheduled at {job.next_run_time}")
            except AttributeError:
                console_logger.error(f"Unable to retrieve next run time for job '{job.id}'. Scheduler may not have started.")
                file_logger.error(f"Unable to retrieve next run time for job '{job.id}'. Scheduler may not have started.")
    except Exception as e:
        console_logger.error(f"Failed to iterate through scheduled log jobs: {e}")
        file_logger.error(f"Failed to iterate through scheduled log jobs: {e}")


async def start_scheduler():
    schedule_interval_seconds = app_config.schedule_seconds
    schedule_days = app_config.schedule_days

    if schedule_interval_seconds is not 0 or None:
        interval_seconds = int(schedule_interval_seconds)
        trigger = IntervalTrigger(seconds=interval_seconds)
        job_id = 'obtain_total_stops_seconds'
        job = scheduler.add_job(run_example_obtain_total_stops, trigger, id=job_id)
    elif schedule_days is not 0 or None:
        trigger = IntervalTrigger(days=int(schedule_days))
        job_id = 'obtain_total_stops_days'
        job = scheduler.add_job(run_example_obtain_total_stops, trigger, id=job_id)
    else:
        schedule_hour = app_config.schedule_hour
        schedule_minute = app_config.schedule_minute
        trigger = CronTrigger(hour=schedule_hour, minute=schedule_minute)
        job_id = 'obtain_total_stops_cron'
        job = scheduler.add_job(run_example_obtain_total_stops, trigger, id=job_id)

    await log_job_next_run_time(job_id)

    scheduler.start()
    console_logger.info("Scheduler started")
    file_logger.info("Scheduler started")

async def run_scheduler():
    try:
        await start_scheduler()
        await log_all_next_run_times()
        await asyncio.Event().wait()
    except (KeyboardInterrupt, SystemExit):
        console_logger.info("Scheduler stopped")
        file_logger.info("Scheduler stopped")
