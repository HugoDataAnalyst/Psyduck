from flask import Flask, request, jsonify, abort, redirect, url_for
import json

app = Flask(__name__)

@app.before_request
def limit_remote_addr():
    if request.remote_addr != '127.0.0.1':
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

    def extract_pvp_ranks(pvp_data):
        ranks = {}
        for category in ['great', 'little', 'ultra']:
            category_data = pvp_data.get(category, [])
            ranks[f'pvp_{category}_rank'] = next((entry.get('rank') for entry in category_data if entry), None)
        return ranks

    if isinstance(data, list):
        for item in data:
            if item.get('type') == 'pokemon':
                message = item.get('message', {})
                pvp_ranks = extract_pvp_ranks(message.get('pvp', {}))
                filtered_data = {
                    'pokemon_id': message.get('pokemon_id'),
                    'form': message.get('form'),
                    'latitude': message.get('latitude'),
                    'longitude': message.get('longitude'),
                    'cp': message.get('cp'),
                    'individual_attack': message.get('individual_attack'),
                    'individual_defense': message.get('individual_defense'),
                    'individual_stamina': message.get('individual_stamina'),
                    'pokemon_level': message.get('pokemon_level'),
                    **pvp_ranks
                }
                print("Filtered data:", filtered_data)
                save_to_file(filtered_data)
            else:
                print("Data did not meet filter criteria")
    else:
        print("Received data is not in list format")

    return jsonify({"status": "success"}), 200

if __name__ == '__main__':
    app.run(debug=True)
