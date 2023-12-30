from flask import Flask, request, jsonify, abort, redirect, url_for
from werkzeug.wrappers import Request, Response
import json

app = Flask(__name__)

@app.before_request
def limit_remote_addr():
    if request.remote_addr != '127.0.0.1':
        abort(403)  # Forbidden

def save_to_file(data, filename="received_data.json"):
    # Check if the file exists and read existing data
    try:
        with open(filename, "r") as file:
            existing_data = json.load(file)
    except (FileNotFoundError, json.JSONDecodeError):
        existing_data = []

    # Append new data
    existing_data.append(data)

    # Save updated data back to file
    with open(filename, "w") as file:
        json.dump(existing_data, file, indent=4)

@app.route('/', methods=['POST'])
def root_redirect():
    return redirect(url_for('receive_data'), code=307)



@app.route('/webhook', methods=['POST'])
def receive_data():
    data = request.json

    # Define your filter criteria
    def filter_criteria(message):
        pvp_categories = message.get('pvp', {})
        if pvp_categories is None:
            return False
        rank_one_in_pvp = any(
            any(entry.get('rank') == 1 for entry in pvp_categories.get(category, []) if entry)
            for category in ['great', 'little', 'ultra']
        )

        return (
            message.get('pokemon_id') is not None and
            message.get('form') is not None and
            'latitude' in message and
            'longitude' in message and
            message.get('cp') is not None and
            message.get('individual_attack') is not None and
            message.get('individual_defense') is not None and
            message.get('individual_stamina') is not None and
            message.get('pokemon_level') is not None and
            rank_one_in_pvp
        )

    # Check if data meets the filter criteria
    if isinstance(data, list):
        for item in data:
            if item.get('type') == 'pokemon' and filter_criteria(item.get('message', {})):
                print("Filtered data:", item)
                save_to_file(item)
            else:
                print("Data did not meet filter criteria")
    else:
        print("Received data is not in list format")

    return jsonify({"status": "success"}), 200

if __name__ == '__main__':
    app.run(debug=True)