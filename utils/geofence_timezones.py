import pytz
from geopy.geocoders import Nominatim
from datetime import datetime
from config.app_config import app_config

# Function to determine the timezone based on latitude and longitude
def get_timezone(lat, lon):
    geolocator = Nominatim(user_agent="geoapiExercises")
    location = geolocator.reverse(f"{lat}, {lon}", language='en')
    if location and 'timezone' in location.raw['address']:
        return location.raw['address']['timezone']
    else:
        return app_config.default_timezone

# Function to get current time in geofence timezone
def get_current_time_in_geofence_timezone(geofence):
    if 'geometry' in geofence:
        lat = geofence['geometry']['coordinates'][0][1]
        lon = geofence['geometry']['coordinates'][0][0]
        timezone_str = get_timezone(lat, lon)
        timezone = pytz.timezone(timezone_str)
        return datetime.now(timezone)
    else:
        return datetime.now(pytz.timezone(app_config.default_timezone))
