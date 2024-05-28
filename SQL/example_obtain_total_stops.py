import pymysql
import requests
from shapely.geometry import Point, Polygon
from datetime import datetime
from collections import Counter
from config.app_config import app_config
import sys

# Golbat Database settings
db1_settings = {
    "host": app_config.golbat_host,
    "user": app_config.golbat_user,
    "password": app_config.golbat_password,
    "db": app_config.golbat_database,
    "port": app_config.golbat_port
}

# Psyduck Database Settings
datacube_settings = {
    "host": app_config.db_host,
    "user": app_config.db_user,
    "password": app_config.db_password,
    "db": app_config.db_name,
    "port": app_config.db_port
}

# API URL for fetching geofence data
api_url = app_config.geofence_api_url
api_headers = {"Authorization": f"Bearer {app_config.bearer_token}"}

# Connect to Database1 and fetch data
def fetch_pokestops():
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
        return pokestops
    except Exception as e:
        print(f"Error fetching pokestops: {e}")
        return []

# Fetch geofence data from an API
def fetch_geofences():
    try:
        print(f"Fetching geofence data from API...")
        response = requests.get(api_url, headers=api_headers)
        geofences = response.json().get("data", {}).get("features", [])
        print(f"Fetched {len(geofences)} geofences.")
        return geofences
    except Exception as e:
        print(f"Error fetching geofence data: {e}")
        return []

# Define the function to check if a point is inside a geofence
def is_inside_geofence(lat, lon, geofences):
    point = Point(lon, lat)
    for geofence in geofences:
        polygon = Polygon(geofence["geometry"]["coordinates"][0])
        if point.within(polygon):
            return True, geofence.get("properties", {}).get("name", "Unknown")
    return False, None

# Check if each pokestop is inside any geofence
def process_pokestops(pokestops, geofences):
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
    return stops_with_area

# Insert the aggregated data into the DataCube database
def insert_aggregated_data(area_counts):
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

def main():
    pokestops = fetch_pokestops()
    geofences = fetch_geofences()
    stops_with_area = process_pokestops(pokestops, geofences)
    # Aggregate the data by area name
    area_counts = Counter(area_name for _, area_name in stops_with_area)
    print(f"Aggregated pokestops by area: {area_counts}")
    insert_aggregated_data(area_counts)

if __name__ == "__main__":
    main()
