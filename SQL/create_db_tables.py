import json
import mysql.connector

with open('../config/config.json') as config_file:
	config = json.load(config_file)

db_config = {
	'host': config['DATABASE_HOST'],
	'port': config['DATABASE_PORT'],
	'user': config['DATABASE_USER'],
	'password': config['DATABASE_PASSWORD']
}

db_name = config['DATABASE_NAME']

create_database_sql = f"CREATE DATABASE IF NOT EXISTS {db_name} CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci"

create_table_sql= '''
CREATE TABLE IF NOT EXISTS pokemon_sightings (
	id INTEGER PRIMARY KEY AUTO_INCREMENT,
	pokemon_id INTEGER NOT NULL,
	form VARCHAR(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci,
	latitude FLOAT,
	longitude FLOAT,
	iv TINYINT,
	pvp_great_rank TINYINT,
	pvp_little_rank TINYINT,
	pvp_ultra_rank TINYINT,
	shiny BOOLEAN,
	area_name VARCHAR(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci,
	despawn_time VARCHAR(255)
) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;
'''

def create_database_and_table():
	try:
		conn = mysql.connector.connect(**db_config)
		cursor = conn.cursor()

		cursor.execute(f"SELECT SCHEMA_NAME FROM INFORMATION_SCHEMA.SCHEMATA WHERE SCHEMA_NAME = '{db_name}'")
		db_exists = cursor.fetchone() is not None

		cursor.execute(create_database_sql)

		if db_exists:
			print(f"Database {db_name} already existed.")
		else:
			print(f"Database {db_name} created.")

		conn.database = db_name

		cursor.execute(create_table_sql)
		conn.commit()
		print("Table created sucessfully")
	except mysql.connector.Error as err:
		print(f"Error: {err}")
	finally:
		if conn.is_connected():
			cursor.close()
			conn.close()

if __name__ == '__main__':
	create_database_and_table()