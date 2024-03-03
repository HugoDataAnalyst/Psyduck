import pymysql
import requests
from shapely.geometry import Point, Polygon
from datetime import datetime
from collections import Counter
import sys

# Database settings
db1_settings = {
    "host": "your_host",
    "user": "your_user",
    "password": "your_password",
    "db": "your_database",
    "port": your_port
}

datacube_settings = {
    "host": "your_host",
    "user": "your_user",
    "password": "your_password",
    "db": "DataCube",
    "port": your_port
}

# API URL for fetching geofence data
api_url = 'your_geofence_api_url'
api_headers = {"Authorization": "Bearer your_bearer_token"}

# Connect to Database1 and fetch data
try:
    print(f"Connecting to {db1_settings['db']} to fetch pokestops...")
    db1_connection = pymysql.connect(**db1_settings)
    try:
        with db1_connection.cursor() as cursor:
            sql = "SELECT DISTINCT id, lat, lon FROM pokestop;"
            cursor.execute(sql)
            pokestops = cursor.fetchall()
        print(f"Fetched {len(pokestops)} pokestops.")
    finally:
        db1_connection.close()
except Exception as e:
    print(f"Error fetching pokestops: {e}")

# Fetch geofence data from an API
try:
    print(f"Fetching geofence data from API...")
    response = requests.get(api_url, headers=api_headers)
    geofences = response.json().get("data", {}).get("features", [])
    print(f"Fetched {len(geofences)} geofences.")
except Exception as e:
    print(f"Error fetching geofence data: {e}")

# Define the function to check if a point is inside a geofence
def is_inside_geofence(lat, lon, geofences):
    point = Point(lon, lat)
    for geofence in geofences:
        polygon = Polygon(geofence["geometry"]["coordinates"][0])
        if point.within(polygon):
            return True, geofence.get("properties", {}).get("name", "Unknown")
    return False, None

# Check if each pokestop is inside any geofence
print(f"Checking if pokestops are inside any geofence...")
stops_with_area = []
total_stops = len(pokestops)
for index, stop in enumerate(pokestops, start=1):
    try:
        inside, area_name = is_inside_geofence(stop[1], stop[2], geofences)
        if inside:
            stops_with_area.append((stop[0], area_name))
        # Update progress on the same line
        print(f"\rProcessed {index}/{total_stops} pokestops...", end="")
        sys.stdout.flush()
    except Exception as e:
        print(f"\nError processing geofence check for stop {stop[0]}: {e}")
print(f"\n{len(stops_with_area)} pokestops matched with geofences.")

# Aggregate the data by area name
area_counts = Counter(area_name for _, area_name in stops_with_area)
print(f"Aggregated pokestops by area: {area_counts}")

# Insert the aggregated data into the DataCube database
try:
    print(f"Inserting aggregated data into the {datacube_settings['db']} database...")
    with pymysql.connect(**datacube_settings) as datacube_connection:
        with datacube_connection.cursor() as cursor:
            cursor.execute("TRUNCATE TABLE total_pokestops")
            today = datetime.now().date()
            for area_name, total_stops in area_counts.items():
                sql = """
                    INSERT INTO total_pokestops (day, total_stops, area_name)
                    VALUES (%s, %s, %s)
                    ON DUPLICATE KEY UPDATE total_stops = VALUES(total_stops);
                """
                cursor.execute(sql, (today, total_stops, area_name))
            datacube_connection.commit()
    print(f"Data inserted into the {datacube_settings['db']} database successfully.")
except Exception as e:
    print(f"Error inserting data into {datacube_settings['db']} database: {e}")
