from datetime import datetime, timedelta
import time

def seconds_until_next_hourly_event():
    """Returns the number of seconds remaining until the start of the next hour."""
    now = datetime.now()
    next_hour = (now + timedelta(hours=1)).replace(minute=2, second=10, microsecond=0)
    return int((next_hour - now).total_seconds())

def hourly_cache_for_daily_event():
    """Returns the number of seconds remaining until the start of the next hour."""
    now = datetime.now()
    next_hour = (now + timedelta(hours=1)).replace(minute=1, second=0, microsecond=0)
    return int((next_hour - now).total_seconds())

def hourly_cache_for_weekly_event():
    """Returns the number of seconds remaining until the start of the next hour."""
    now = datetime.now()
    next_hour = (now + timedelta(hours=1)).replace(minute=21, second=0, microsecond=0)
    return int((next_hour - now).total_seconds())

def hourly_cache_for_monthly_event():
    """Returns the number of seconds remaining until the start of the next hour."""
    now = datetime.now()
    next_hour = (now + timedelta(hours=1)).replace(minute=41, second=0, microsecond=0)
    return int((next_hour - now).total_seconds())

def hourly_cache_for_quest_daily_event():
    """Returns the number of seconds remaining until the start of the next hour."""
    now = datetime.now()
    next_hour = (now + timedelta(hours=1)).replace(minute=11, second=0, microsecond=0)
    return int((next_hour - now).total_seconds())

def hourly_cache_for_quest_weekly_event():
    """Returns the number of seconds remaining until the start of the next hour."""
    now = datetime.now()
    next_hour = (now + timedelta(hours=1)).replace(minute=21, second=0, microsecond=0)
    return int((next_hour - now).total_seconds())

def hourly_cache_for_quest_monthly_event():
    """Returns the number of seconds remaining until the start of the next hour."""
    now = datetime.now()
    next_hour = (now + timedelta(hours=1)).replace(minute=41, second=0, microsecond=0)
    return int((next_hour - now).total_seconds())

"""
The below functions are currently not being used.
"""

def seconds_until_half_hour():
    """Returns the number of seconds remaining until the next half-hour mark."""
    now = datetime.datetime.now()
    # Determine if now.minute is closer to 0 or 30 for the next half-hour mark
    next_half_hour = (now + datetime.timedelta(minutes=30)).replace(minute=0, second=0, microsecond=0)
    if now.minute >= 30:
        # Adjust to the next hour if now.minute is past 30
        next_half_hour = next_half_hour.replace(hour=now.hour + 1)
    return (next_half_hour - now).total_seconds()

def seconds_until_next_hour():
    """Returns the number of seconds remaining until the start of the next hour."""
    now = datetime.now()
    next_hour = (now + timedelta(hours=1)).replace(minute=1, second=0, microsecond=0)
    return int((next_hour - now).total_seconds())

def seconds_until_midnight():
    """Returns the number of seconds remaining until midnight."""
    now = datetime.now()
    midnight = (now + timedelta(days=1)).replace(hour=2, minute=0, second=0, microsecond=0)
    return (midnight - now).seconds

def seconds_until_fourpm():
    """Returns the number of seconds remaining untill 4 pm."""
    now = datetime.now()
    fourpm = (now + timedelta(days=1)).replace(hour=16, minute=0, second=0, microsecond=0)
    return (fourpm - now).seconds

def seconds_until_next_week():
    """Returns the number of seconds remaining until next Monday."""
    now = datetime.now()
    next_week = now + timedelta(days=(7 - now.weekday()))
    next_monday = next_week.replace(hour=2, minute=0, second=0, microsecond=0)
    return int((next_monday - now).total_seconds())

def seconds_until_next_week_fourpm():
    """Returns the number of seconds remaining untill next week's 4 pm."""
    now = datetime.now()
    next_week = now + timedelta(days=(7 - now.weekday()))
    next_monday = next_week.replace(hour=16, minute=0, second=0, microsecond=0)
    return int((next_monday - now).total_seconds())

def seconds_until_next_month():
    """Returns the number of seconds remaining until the start of next month."""
    now = datetime.now()
    if now.month == 12:
        next_month = now.replace(year=now.year+1, month=1, day=1, hour=3, minute=0, second=0, microsecond=0)
    else:
        next_month = now.replace(month=now.month+1, day=1, hour=3, minute=0, second=0, microsecond=0)
    return int((next_month - now).total_seconds())

def seconds_until_next_month_fourpm():
    """Returns the number of seconds remaining until the start of next month at 4 PM."""
    now = datetime.now()
    if now.month == 12:
        next_month = now.replace(year=now.year+1, month=1, day=1, hour=16, minute=0, second=0, microsecond=0)
    else:
        next_month = now.replace(month=now.month+1, day=1, hour=16, minute=0, second=0, microsecond=0)
    return int((next_month - now).total_seconds())
