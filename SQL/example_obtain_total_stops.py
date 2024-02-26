import pymysql
import requests
from shapely.geometry import Point, Polygon
from datetime import datetime
from collections import Counter

# Database settings
db1_settings = {
    "host": "your_host",
    "user": "your_user",
    "password": "your_password",
    "db": "your_database",
    "port": your_port  # Replace with your actual port number
}

datacube_settings = {
    "host": "your_host",
    "user": "your_user",
    "password": "your_password",
    "db": "DataCube",
    "port": your_port  # Replace with your actual port number
}

# API URL for fetching geofence data
api_url = 'your_geofence_api_url'
api_headers = {"Authorization": "Bearer your_bearer_token"}

# Connect to Database1 and fetch data
try:
    print("Connecting to Database1 to fetch pokestops...")
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
    print("Fetching geofence data from API...")
    response = requests.get(api_url, headers=api_headers)
    geofences = response.json().get("data", {}).get("features", [])
    print(f"Fetched {len(geofences)} geofences.")
except Exception as e:
    print(f"Error fetching geofence data: {e}")

# Check if each pokestop is inside any geofence
print("Checking if pokestops are inside any geofence...")
stops_with_area = []
for stop in pokestops:
    try:
        inside, area_name = is_inside_geofence(stop[1], stop[2], geofences)
        if inside:
            stops_with_area.append((stop[0], area_name))
    except Exception as e:
        print(f"Error processing geofence check for stop {stop[0]}: {e}")

print(f"{len(stops_with_area)} pokestops matched with geofences.")

# Aggregate the data by area name
area_counts = Counter(area_name for _, area_name in stops_with_area)
print(f"Aggregated pokestops by area: {area_counts}")

# Insert the aggregated data into the DataCube database
try:
    print("Inserting aggregated data into the DataCube database...")
    with pymysql.connect(**datacube_settings) as datacube_connection:
        with datacube_connection.cursor() as cursor:
            today = datetime.now().date()  # Gets the current date
            for area_name, total_stops in area_counts.items():
                sql = """
                    INSERT INTO total_pokestops (day, total_stops, area_name)
                    VALUES (%s, %s, %s)
                    ON DUPLICATE KEY UPDATE total_stops = total_stops + VALUES(total_stops);
                """
                cursor.execute(sql, (today, total_stops, area_name))
            datacube_connection.commit()
    print("Data inserted into the DataCube database successfully.")
except Exception as e:
    print(f"Error inserting data into DataCube database: {e}")
