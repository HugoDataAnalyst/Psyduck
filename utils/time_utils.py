from datetime import datetime, timedelta
import time
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

def seconds_until_next_week():
    """Returns the number of seconds remaining until next Monday."""
    now = datetime.now()
    next_week = now + timedelta(days=(7 - now.weekday()))
    next_monday = next_week.replace(hour=2, minute=0, second=0, microsecond=0)
    return int((next_monday - now).total_seconds())

def seconds_until_next_month():
    """Returns the number of seconds remaining until the start of next month."""
    now = datetime.now()
    if now.month == 12:
        next_month = now.replace(year=now.year+1, month=1, day=1, hour=3, minute=0, second=0, microsecond=0)
    else:
        next_month = now.replace(month=now.month+1, day=1, hour=3, minute=0, second=0, microsecond=0)
    return int((next_month - now).total_seconds())