import pymysql
import requests
from shapely.geometry import Point, Polygon
from datetime import datetime

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
db1_connection = pymysql.connect(**db1_settings)
try:
    with db1_connection.cursor() as cursor:
        sql = "SELECT DISTINCT id, lat, lon FROM pokestop;"
        cursor.execute(sql)
        pokestops = cursor.fetchall()
finally:
    db1_connection.close()

# Fetch geofence data from an API
response = requests.get(api_url, headers=api_headers)
geofences = response.json().get("data", {}).get("features", [])

# Check if each pokestop is inside any geofence
def is_inside_geofence(lat, lon, geofences):
    point = Point(lon, lat)
    for geofence in geofences:
        polygon = Polygon(geofence["geometry"]["coordinates"][0])
        if point.within(polygon):
            return True, geofence.get("properties", {}).get("name", "Unknown")
    return False, None

stops_with_area = []
for stop in pokestops:
    inside, area_name = is_inside_geofence(stop[1], stop[2], geofences)
    if inside:
        stops_with_area.append((stop[0], area_name))

# Aggregate the data by area name
area_counts = Counter(area_name for _, area_name in stops_with_area)

# Insert the aggregated data into the DataCube database
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
