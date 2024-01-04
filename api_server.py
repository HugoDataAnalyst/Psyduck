from flask import Flask, request, jsonify
import mysql.connector
import json
from app_config import app_config

# Database config
db_config = {
    'host': app_config.db_host,
    'port': app_config.db_port,
    'user': app_config.db_user,
    'password': app_config.db_password,
    'database': app_config.db_name
}

app = Flask(__name__)

API_SECRET_KEY = app_config.api_secret_key

@app.route('/api/daily-area-pokemon-stats', methods=['GET'])
def pokemon_stats():
    secret = request.args.get('secret')
    
    if not secret or secret != API_SECRET_KEY:
        return jsonify({"error": "Unauthorized access"}), 403

    # Connect to DB and fetch data
    try:
        conn = mysql.connector.connect(**db_config)
        cursor = conn.cursor(dictionary=True)
        query = "SELECT * FROM daily_api_pokemon_area_stats ORDER BY area_name, pokemon_id"
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

@app.route('/api/weekly-area-pokemon-stats', methods=['GET'])
def pokemon_stats():
    secret = request.args.get('secret')
    
    if not secret or secret != API_SECRET_KEY:
        return jsonify({"error": "Unauthorized access"}), 403

    # Connect to DB and fetch data
    try:
        conn = mysql.connector.connect(**db_config)
        cursor = conn.cursor(dictionary=True)
        query = "SELECT * FROM weekly_api_pokemon_area_stats ORDER BY area_name, pokemon_id"
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

@app.route('/api/monthly-area-pokemon-stats', methods=['GET'])
def pokemon_stats():
    secret = request.args.get('secret')
    
    if not secret or secret != API_SECRET_KEY:
        return jsonify({"error": "Unauthorized access"}), 403

    # Connect to DB and fetch data
    try:
        conn = mysql.connector.connect(**db_config)
        cursor = conn.cursor(dictionary=True)
        query = "SELECT * FROM monthly_api_pokemon_area_stats ORDER BY area_name, pokemon_id"
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
    app.run(debug=False, port=app_config.api_port)
