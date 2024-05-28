# scheduler.py
import asyncio
import os
import logging
from logging.handlers import RotatingFileHandler
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.interval import IntervalTrigger
from apscheduler.triggers.cron import CronTrigger
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
        await log_next_run_time('obtain_total_stops')
    except Exception as e:
        console_logger.error(f'Error running obtain_total_stops.py: {e}')
        file_logger.error(f'Error running obtain_total_stops.py: {e}')

async def schedule_job_with_interval(job_function, interval_seconds, job_id):
    if interval_seconds:
        trigger = IntervalTrigger(seconds=int(interval_seconds))

        if scheduler.get_job(job_id):
            try:
                scheduler.reschedule_job(job_id, trigger=trigger)
                job = scheduler.get_job(job_id) # Fetch updated job for accurate logging
                console_logger.info(f"Job completed. Rescheduled job '{job_id}' to run at {job.next_run_time}.")
            except Exception as e:
                console_logger.error(f"Failed to reschedule job '{job_id}': {e}")
        else:
            try:
                scheduler.add_job(job_function, trigger, id=job_id, max_instances=1)
                job = scheduler.get_job(job_id) # Fetch job for accurate logging
                console_logger.info(f"Scheduled new job '{job_id}' to run at {job.next_run_time}.")
            except Exception as e:
                console_logger.error(f"Failed to schedule new job '{job_id}': {e}")

async def schedule_job_with_cron(job_function, schedule_hour, schedule_minute, job_id):
    if schedule_hour and schedule_minute:
        trigger = CronTrigger(hour=int(schedule_hour), minute=int(schedule_minute))

        if scheduler.get_job(job_id):
            try:
                scheduler.reschedule_job(job_id, trigger=trigger)
                job = scheduler.get_job(job_id) # Fetch updated job for accurate logging
                console_logger.info(f"Job completed. Rescheduled job '{job_id}' to run at {job.next_run_time}.")
            except Exception as e:
                console_logger.error(f"Failed to reschedule job '{job_id}': {e}")
        else:
            try:
                scheduler.add_job(job_function, trigger, id=job_id, max_instances=1)
                job = scheduler.get_job(job_id) # Fetch job for accurate logging
                console_logger.info(f"Scheduled new job '{job_id}' to run at {job.next_run_time}.")
            except Exception as e:
                console_logger.error(f"Failed to schedule new job '{job_id}': {e}")

async def log_next_run_time(job_id):
    job = scheduler.get_job(job_id)
    if job:
        next_run_time = job.next_run_time
        if next_run_time:
            console_logger.info(f"Next run time for '{job_id}' is scheduled at {next_run_time}")
            file_logger.info(f"Next run time for '{job_id}' is scheduled at {next_run_time}")
        else:
            console_logger.info(f"Job '{job_id}' has no next run time scheduled")
            file_logger.info(f"Job '{job_id}' has no next run time scheduled")
    else:
        console_logger.error(f"Failed to find job '{job_id}' for logging its next run time")
        file_logger.error(f"Failed to find job '{job_id}' for logging its next run time")


async def log_all_next_run_times():
    """Logs the next run time for all scheduled jobs."""
    try:
        for job in scheduler.get_jobs():
            try:
                next_run_time = job.next_run_time
                if next_run_time:
                    console_logger.info(f"Next run time for '{job.id}' is scheduled at {next_run_time}")
                    file_logger.info(f"Next run time for '{job.id}' is scheduled at {next_run_time}")
                else:
                    console_logger.info(f"Job '{job.id}' has no next run time scheduled")
                    file_logger.info(f"Job '{job.id}' has no next run time scheduled")
            except AttributeError:
                console_logger.error(f"Unable to retrieve next run time for job '{job.id}'. Scheduler may not have started.")
                file_logger.error(f"Unable to retrieve next run time for job '{job.id}'. Scheduler may not have started.")
    except Exception as e:
        console_logger.error(f"Failed to iterate through scheduled log jobs: {e}")
        file_logger.error(f"Failed to iterate through scheduled log jobs: {e}")

async def start_scheduler():
    scheduler.start()
    console_logger.info("Scheduler started")
    file_logger.info("Scheduler started")

    schedule_interval_seconds = app_config.schedule_seconds
    schedule_days = app_config.schedule_days
    schedule_hour = app_config.schedule_hour
    schedule_minute = app_config.schedule_minute
    job_id = 'obtain_total_stops'

    if schedule_interval_seconds:
        await schedule_job_with_interval(run_example_obtain_total_stops, int(schedule_interval_seconds), job_id)
    elif schedule_days:
        await schedule_job_with_interval(run_example_obtain_total_stops, int(schedule_days) * 86400, job_id)  # Convert days to seconds
    elif schedule_hour and schedule_minute:
        await schedule_job_with_cron(run_example_obtain_total_stops, schedule_hour, schedule_minute, job_id)
    else:
        console_logger.info(f"No jobs to be added.")
        file_logger.info(f"No jobs to be added.")

async def run_scheduler():
    try:
        await start_scheduler()
        await log_all_next_run_times()
        await asyncio.Event().wait()
    except (KeyboardInterrupt, SystemExit):
        console_logger.info("Scheduler stopped")
        file_logger.info("Scheduler stopped")
