import json
import mysql.connector

with open('../config/config.json') as config_file:
	config = json.load(config_file)

# Database configuration
db_config = {
	'host': config['database']['HOST'],
	'port': config['database']['PORT'],
	'user': config['database']['USER'],
	'password': config['database']['PASSWORD']
}

db_name = config['database']['NAME']

create_database_sql = f"CREATE DATABASE IF NOT EXISTS {db_name} CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci"

create_pokemon_sightings_table_sql = '''
CREATE TABLE IF NOT EXISTS pokemon_sightings (
	id INTEGER PRIMARY KEY AUTO_INCREMENT,
	pokemon_id INTEGER NOT NULL,
	form VARCHAR(15) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci,
	latitude FLOAT,
	longitude FLOAT,
	iv TINYINT,
	pvp_little_rank TINYINT,
	pvp_great_rank TINYINT,
	pvp_ultra_rank TINYINT,
	shiny BOOLEAN,
	area_name VARCHAR(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci,
	despawn_time SMALLINT,
	inserted_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;
'''

create_daily_api_pokemon_area_stats_table_sql = '''
CREATE TABLE IF NOT EXISTS daily_api_pokemon_area_stats (
	pokemon_id INTEGER NOT NULL,
	form VARCHAR(15) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci,
	total INTEGER,
	avg_lat FLOAT,
	avg_lon FLOAT,
	total_iv100 INTEGER,
	total_iv0 INTEGER,
	total_top1_little INTEGER,
	total_top1_great INTEGER,
	total_top1_ultra INTEGER,
	total_shiny INTEGER,
	area_name VARCHAR(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci,
	avg_despawn SMALLINT,
    PRIMARY KEY (pokemon_id, form, area_name)
) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;
'''

create_weekly_api_pokemon_area_stats_table_sql = '''
CREATE TABLE IF NOT EXISTS weekly_api_pokemon_area_stats (
	pokemon_id INTEGER NOT NULL,
	form VARCHAR(15) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci,
	total INTEGER,
	avg_lat FLOAT,
	avg_lon FLOAT,
	total_iv100 INTEGER,
	total_iv0 INTEGER,
	total_top1_little INTEGER,
	total_top1_great INTEGER,
	total_top1_ultra INTEGER,
	total_shiny INTEGER,
	area_name VARCHAR(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci,
	avg_despawn SMALLINT,
    PRIMARY KEY (pokemon_id, form, area_name)
) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;
'''

create_monthly_api_pokemon_area_stats_table_sql = '''
CREATE TABLE IF NOT EXISTS monthly_api_pokemon_area_stats (
	pokemon_id INTEGER NOT NULL,
	form VARCHAR(15) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci,
	total INTEGER,
	avg_lat FLOAT,
	avg_lon FLOAT,
	total_iv100 INTEGER,
	total_iv0 INTEGER,
	total_top1_little INTEGER,
	total_top1_great INTEGER,
	total_top1_ultra INTEGER,
	total_shiny INTEGER,
	area_name VARCHAR(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci,
	avg_despawn SMALLINT,
    PRIMARY KEY (pokemon_id, form, area_name)
) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;
'''

create_daily_event_sql = f'''
CREATE EVENT IF NOT EXISTS update_daily_api_pokemon_area_stats
ON SCHEDULE EVERY 1 DAY
DO
    REPLACE INTO daily_api_pokemon_area_stats
    SELECT
        pokemon_id,
        form,
        COUNT(pokemon_id) AS total,
        AVG(latitude) AS avg_lat,
        AVG(longitude) AS avg_lon,
        SUM(CASE WHEN iv = 100 THEN 1 ELSE NULL END) AS total_iv100,
        SUM(CASE WHEN iv = 0 THEN 1 ELSE NULL END) AS total_iv0,
        SUM(CASE WHEN pvp_little_rank = 1 THEN 1 ELSE NULL END) AS total_top1_little,
        SUM(CASE WHEN pvp_great_rank = 1 THEN 1 ELSE NULL END) AS total_top1_great,
        SUM(CASE WHEN pvp_ultra_rank = 1 THEN 1 ELSE NULL END) AS total_top1_ultra,
        SUM(CASE WHEN shiny = 1 THEN 1 ELSE NULL END) AS total_shiny,
        area_name,
        ROUND(AVG(despawn_time),2) AS avg_despawn
    FROM pokemon_sightings
    WHERE inserted_at >= CURDATE() - INTERVAL 1 DAY AND inserted_at < CURDATE()
    GROUP BY pokemon_id, form, area_name
    ORDER BY area_name, pokemon_id;
'''

create_weekly_event_sql = f'''
CREATE EVENT IF NOT EXISTS update_weekly_api_pokemon_area_stats
ON SCHEDULE EVERY 1 WEEK
STARTS (TIMESTAMP(CURRENT_DATE) + INTERVAL 1 WEEK)
DO
    REPLACE INTO weekly_api_pokemon_area_stats
    SELECT
        pokemon_id,
        form,
        COUNT(pokemon_id) AS total,
        AVG(latitude) AS avg_lat,
        AVG(longitude) AS avg_lon,
        SUM(CASE WHEN iv = 100 THEN 1 ELSE NULL END) AS total_iv100,
        SUM(CASE WHEN iv = 0 THEN 1 ELSE NULL END) AS total_iv0,
        SUM(CASE WHEN pvp_little_rank = 1 THEN 1 ELSE NULL END) AS total_top1_little,
        SUM(CASE WHEN pvp_great_rank = 1 THEN 1 ELSE NULL END) AS total_top1_great,
        SUM(CASE WHEN pvp_ultra_rank = 1 THEN 1 ELSE NULL END) AS total_top1_ultra,
        SUM(CASE WHEN shiny = 1 THEN 1 ELSE NULL END) AS total_shiny,
        area_name,
        ROUND(AVG(despawn_time),2) AS avg_despawn
    FROM pokemon_sightings
    WHERE inserted_at >= CURDATE() - INTERVAL 7 DAY AND inserted_at < CURDATE()
    GROUP BY pokemon_id, form, area_name
    ORDER BY area_name, pokemon_id;
'''

create_monthly_event_sql = f'''
CREATE EVENT IF NOT EXISTS update_monthly_api_pokemon_area_stats
ON SCHEDULE EVERY 1 MONTH
STARTS (TIMESTAMP(CURRENT_DATE) + INTERVAL 1 MONTH)
DO
    REPLACE INTO monthly_api_pokemon_area_stats
    SELECT
        pokemon_id,
        form,
        COUNT(pokemon_id) AS total,
        AVG(latitude) AS avg_lat,
        AVG(longitude) AS avg_lon,
        SUM(CASE WHEN iv = 100 THEN 1 ELSE NULL END) AS total_iv100,
        SUM(CASE WHEN iv = 0 THEN 1 ELSE NULL END) AS total_iv0,
        SUM(CASE WHEN pvp_little_rank = 1 THEN 1 ELSE NULL END) AS total_top1_little,
        SUM(CASE WHEN pvp_great_rank = 1 THEN 1 ELSE NULL END) AS total_top1_great,
        SUM(CASE WHEN pvp_ultra_rank = 1 THEN 1 ELSE NULL END) AS total_top1_ultra,
        SUM(CASE WHEN shiny = 1 THEN 1 ELSE NULL END) AS total_shiny,
        area_name,
        ROUND(AVG(despawn_time),2) AS avg_despawn
    FROM pokemon_sightings
    WHERE inserted_at >= CURDATE() - INTERVAL 1 MONTH AND inserted_at < CURDATE()
    GROUP BY pokemon_id, form, area_name
    ORDER BY area_name, pokemon_id;
'''

def create_database_and_table():
	try:
		conn = mysql.connector.connect(**db_config)
		cursor = conn.cursor()

		# Create Database
		cursor.execute(f"SELECT SCHEMA_NAME FROM INFORMATION_SCHEMA.SCHEMATA WHERE SCHEMA_NAME = '{db_name}'")
		db_exists = cursor.fetchone() is not None
		cursor.execute(create_database_sql)
		if db_exists:
			print(f"Database {db_name} already existed.")
		else:
			print(f"Database {db_name} created.")
		conn.database = db_name

		# Create tables
		cursor.execute(create_pokemon_sightings_table_sql)
		print("pokemon_sightings table checked/created.")
		
		cursor.execute(create_daily_api_pokemon_area_stats_table_sql)
		print("daily_api_pokemon_area_stats table checked/created.")
		
		cursor.execute(create_weekly_api_pokemon_area_stats_table_sql)
		print("weekly_api_pokemon_area_stats table checked/created.")

		cursor.execute(create_monthly_api_pokemon_area_stats_table_sql)
		print("monthly_api_pokemon_area_stats table checked/created.")

		# Create event
		cursor.execute(create_daily_event_sql)
		print("update_daily_api_pokemon_area_stats event created.")

		cursor.execute(create_weekly_event_sql)
		print("update_weekly_api_pokemon_area_stats event created.")

		cursor.execute(create_monthly_event_sql)
		print("update_monthly_api_pokemon_area_stats event created.")

		conn.commit()
		print("Tables & Event created sucessfully")
	except mysql.connector.Error as err:
		print(f"Error: {err}")
	finally:
		if conn.is_connected():
			cursor.close()
			conn.close()

if __name__ == '__main__':
	create_database_and_table()
