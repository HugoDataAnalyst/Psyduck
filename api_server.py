from flask import Flask, request, jsonify
import mysql.connector
import json

# Load configuration
with open('config/config.json') as config_file:
    config = json.load(config_file)

# Database config
db_config = {
    'host': config['DATABASE_HOST'],
    'user': config['DATABASE_USER'],
    'password': config['DATABASE_PASSWORD'],
    'database': config['DATABASE_NAME']
}

app = Flask(__name__)

API_SECRET_KEY = "YourSecretKeyHere"  # Replace with your secret key

@app.route('/api/pokemon-stats', methods=['GET'])
def pokemon_stats():
    secret = request.args.get('secret')
    
    if not secret or secret != API_SECRET_KEY:
        return jsonify({"error": "Unauthorized access"}), 403

    # Connect to DB and fetch data
    try:
        conn = mysql.connector.connect(**db_config)
        cursor = conn.cursor(dictionary=True)
        query = "SELECT * FROM api_pokemon_area_stats ORDER BY area_name, pokemon_id"
        cursor.execute(query)
        results = cursor.fetchall()

        # Organize results by area_name
        organized_results = {}
        for row in results:
            area = row['area_name']
            if area not in organized_results:
                organized_results[area] = []
            organized_results[area].append(row)

        return jsonify(organized_results)
    except mysql.connector.Error as err:
        return jsonify({"error": str(err)}), 500
    finally:
        if conn.is_connected():
            cursor.close()
            conn.close()

if __name__ == '__main__':
    app.run(debug=True, port=5000)
