from flask import Flask, request, jsonify, abort, redirect, url_for
import json
from shapely.geometry import Point, Polygon
import requests

app = Flask(__name__)

# Load configuration
with open('config/config.json') as config_file:
    config = json.load(config_file)

GEOFENCE_API_URL = config['GEOFENCE_API_URL']
BEARER_TOKEN = config['BEARER_TOKEN']
ALLOW_WEBHOOK_HOST = config['ALLOW_WEBHOOK_HOST']
RECEIVER_PORT = config['RECEIVER_PORT']

def fetch_geofences():
    headers = {"Authorization": f"Bearer {BEARER_TOKEN}"}
    response = requests.get(GEOFENCE_API_URL, headers=headers)

    if response.status_code == 200:
        return response.json().get("data", {}).get("features", [])
    else:
        print(f"Failed to fetch geofences. Status Code: {response.status_code}")
        return []

def is_inside_geofence(lat, lon, geofences):
    # Round coordinates to 6 decimal places for consistency
    lat = round(lat, 6)
    lon = round(lon, 6)

    for geofence in geofences:
        coordinates = geofence["geometry"]["coordinates"][0]
        polygon = Polygon(coordinates)
        point = Point(lon, lat)

        geofence_name = geofence.get("properties", {}).get("name")

        result = point.within(polygon)
        if result:
            print(f"Checking {lat}, {lon} inside {geofence_name} with coordinates {coordinates}")
            return result, geofence_name

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
    geofences = fetch_geofences()  # Fetch geofences once or cache them as needed

    # Helper functions
    def filter_criteria(message):
        required_fields = [
            'pokemon_id', 'form', 'latitude', 'longitude',
            'cp', 'individual_attack', 'individual_defense',
            'individual_stamina', 'pokemon_level'
        ]
        return all(message.get(field) is not None for field in required_fields)

    def extract_pvp_ranks(pvp_data):
        ranks = {}
        if pvp_data is None:
            return {f'pvp_{category}_rank': 0 for category in ['great', 'little', 'ultra']}
        for category in ['great', 'little', 'ultra']:
            category_data = pvp_data.get(category, [])
            ranks[f'pvp_{category}_rank'] = next((entry.get('rank') for entry in category_data if entry), 0)  # Default to 0 if no rank
        return ranks

    def iv_calculator(ind_attack, ind_defense, ind_stamina):
        total_iv = ind_attack + ind_defense + ind_stamina
        max_iv = 45  # 15 for each stat
        iv_percentage = (total_iv / max_iv) * 100
        return iv_percentage

    if isinstance(data, list):
        for item in data:
            if item.get('type') == 'pokemon':
                message = item.get('message', {})
                if filter_criteria(message):
                    lat = message.get('latitude')
                    lon = message.get('longitude')
                    inside, geofence_name = is_inside_geofence(lat, lon, geofences)
                    if inside:
                        pvp_ranks = extract_pvp_ranks(message.get('pvp', {}))
                        iv_percentage = iv_calculator(
                            message.get('individual_attack'),
                            message.get('individual_defense'),
                            message.get('individual_stamina')
                        )
                        filtered_data = {
                            'pokemon_id': message.get('pokemon_id'),
                            'form': message.get('form'),
                            'latitude': message.get('latitude'),
                            'longitude': message.get('longitude'),
                            'cp': message.get('cp'),
                            'individual_attack': message.get('individual_attack'),
                            'individual_defense': message.get('individual_defense'),
                            'individual_stamina': message.get('individual_stamina'),
                            'iv': iv_percentage,
                            'pokemon_level': message.get('pokemon_level'),
                            'geofence_name': geofence_name,
                            **pvp_ranks
                        }
                        save_to_file(filtered_data)
                        print("Data Matched and saved")
                    else:
                        print("Data did not match any geofence")
                else:
                    print("Data did not meet filter criteria")
    else:
        print("Received data is not in list format")

    return jsonify({"status": "success"}), 200

if __name__ == '__main__':
    app.run(debug=True, port=RECEIVER_PORT)
