from flask import Flask, request, jsonify, abort, redirect, url_for
import json
from shapely.geometry import Point, Polygon
import requests
import os
import datetime

app = Flask(__name__)

# Load configuration
with open('config/config.json') as config_file:
    config = json.load(config_file)

GEOFENCE_API_URL = config['GEOFENCE_API_URL']
BEARER_TOKEN = config['BEARER_TOKEN']
ALLOW_WEBHOOK_HOST = config['ALLOW_WEBHOOK_HOST']
RECEIVER_PORT = config['RECEIVER_PORT']
DATABASE_HOST = config['DATABASE_HOST']
DATABASE_PORT = config['DATABASE_PORT']
DATABASE_NAME = config['DATABASE_NAME']
DATABASE_USER = config['DATABASE_USER']
DATABASE_PASSWORD = config['DATABASE_PASSWORD']

def fetch_geofences():
    headers = {"Authorization": f"Bearer {BEARER_TOKEN}"}
    response = requests.get(GEOFENCE_API_URL, headers=headers)
    if response.status_code == 200:
        return response.json().get("data", {}).get("features", [])
    else:
        print(f"Failed to fetch geofences. Status Code: {response.status_code}")
        return []

def is_inside_geofence(lat, lon, geofences):
    point = Point(lon, lat)
    for geofence in geofences:
        polygon = geofence["geometry"]["coordinates"][0]
        if point.within(Polygon(polygon)):
            return True, geofence.get("properties", {}).get("name", "Unknown")
    return False, None

def calculate_despawn_time(disappear_time, first_seen):
    if disappear_time is None or first_seen is None:
        return None
    time_diff = disappear_time - first_seen
    minutes, seconds = divmod(time_diff, 60)
    return f"{minutes}mins {seconds}secs"

@app.before_request
def limit_remote_addr():
    if request.remote_addr != ALLOW_WEBHOOK_HOST:
        abort(403)  # Forbidden

def save_to_file(data, filename="received_data.json"):
    try:
        with open(filename, "r") as file:
            existing_data = json.load(file)
    except (FileNotFoundError, json.JSONDecodeError):
        existing_data = []
    existing_data.append(data)
    with open(filename, "w") as file:
        json.dump(existing_data, file, indent=4)

@app.route('/', methods=['POST'])
def root_redirect():
    return redirect(url_for('receive_data'), code=307)

@app.route('/webhook', methods=['POST'])
def receive_data():
    data = request.json
    geofences = fetch_geofences()

    def filter_criteria(message):
        required_fields = [
            'pokemon_id', 'form', 'latitude', 'longitude',
            'individual_attack', 'individual_defense',
            'individual_stamina'
        ]
        return all(message.get(field) is not None for field in required_fields)

    def extract_pvp_ranks(pvp_data):
        ranks = {f'pvp_{category}_rank': None for category in ['great', 'little', 'ultra']}
        if pvp_data:
            for category in ['great', 'little', 'ultra']:
                category_data = pvp_data.get(category, [])
                ranks[f'pvp_{category}_rank'] = 1 if any(entry.get('rank') == 1 for entry in category_data) else None                
         return ranks

    if isinstance(data, list):
        for item in data:
            if item.get('type') == 'pokemon':
                message = item.get('message', {})
                if filter_criteria(message):
                    lat, lon = message.get('latitude'), message.get('longitude')
                    inside, geofence_name = is_inside_geofence(lat, lon, geofences)
                    if inside:
                        ind_attack = message['individual_attack']
                        ind_defense = message['individual_defense']
                        ind_stamina = message['individual_stamina']

                        if ind_attack == ind_defense == ind_stamina == 15:
                            iv_value = 100
                        elif ind_attack == ind_defense == ind_stamina == 0:
                            iv_value = 0
                        else:
                            iv_value = None

                        despawn_time = calculate_despawn_time(
                            message.get('disappear_time'),
                            message.get('first_seen')
                        )
                        filtered_data = {
                            'pokemon_id': message['pokemon_id'],
                            'form': message['form'],
                            'latitude': lat,
                            'longitude': lon,
                            'iv': iv_value,
                            **extract_pvp_ranks(message.get('pvp', {})),
                            'shiny':message['shiny'],
                            'area_name': geofence_name,
                            'despawn_time': despawn_time
                        }
                        save_to_file(filtered_data)
                        print(f"Data matched and saved for geofence: {geofence_name}")
                    else:
                        print("Data did not match any geofence")
                else:
                    print("Data did not meet filter criteria")
    else:
        print("Received data is not in list format")

    return jsonify({"status": "success"}), 200

if __name__ == '__main__':
    app.run(debug=True, port=RECEIVER_PORT)
