import json
import mysql.connector
from mysql.connector import Error

with open('../config/config.json') as config_file:
	config = json.load(config_file)

# Database configuration
db_config = {
	'host': config['database']['HOST'],
	'port': config['database']['PORT'],
	'user': config['database']['USER'],
	'password': config['database']['PASSWORD'],
}

db_clean = config['database']['CLEAN'].lower() == 'true'
db_name = config['database']['NAME']
# Database
create_database_sql = f"CREATE DATABASE IF NOT EXISTS {db_name} CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci"

# Raw storage of Pokémons
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
	inserted_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
	INDEX idx_inserted_at (inserted_at),
    INDEX idx_pokemon_id_form_area (pokemon_id, form, area_name),
    INDEX idx_area_name (area_name)
) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;
'''
# Grouped Daily Total Storage
create_grouped_total_daily_pokemon_stats_table_sql = '''
CREATE TABLE IF NOT EXISTS grouped_total_daily_pokemon_stats (
    day DATE,
	pokemon_id INTEGER NOT NULL,
	form VARCHAR(15) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci,
	avg_lat FLOAT,
	avg_lon FLOAT,
	total INTEGER,
	total_iv100 INTEGER,
	total_iv0 INTEGER,
	total_top1_little INTEGER,
	total_top1_great INTEGER,
	total_top1_ultra INTEGER,
	total_shiny INTEGER,
	area_name VARCHAR(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci,
	avg_despawn FLOAT,
    PRIMARY KEY (day, pokemon_id, form, area_name)
) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;
'''
# Daily Total Storage
create_daily_total_storage_pokemon_stats_table_sql = '''
CREATE TABLE IF NOT EXISTS daily_total_storage_pokemon_stats (
    day DATE,
    area_name VARCHAR(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci,
    total INTEGER,
    total_iv100 MEDIUMINT,
    total_iv0 MEDIUMINT,
    total_top1_little MEDIUMINT,
    total_top1_great MEDIUMINT,
    total_top1_ultra MEDIUMINT,
    total_shiny MEDIUMINT,
    avg_despawn FLOAT,
    PRIMARY KEY (day, area_name)
) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;
'''
# Daily API grouped
create_daily_api_pokemon_stats_sql = '''
CREATE TABLE IF NOT EXISTS daily_api_pokemon_stats (
	day DATE,
	pokemon_id INTEGER NOT NULL,
	form VARCHAR(15) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci,
	avg_lat FLOAT,
	avg_lon FLOAT,
	total INTEGER,
	total_iv100 INTEGER,
	total_iv0 INTEGER,
	total_top1_little INTEGER,
	total_top1_great INTEGER,
	total_top1_ultra INTEGER,
	total_shiny INTEGER,
	area_name VARCHAR(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci,
	avg_despawn FLOAT,
    PRIMARY KEY (pokemon_id, form, area_name)
) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;
'''
# Weekly API grouped
create_weekly_api_pokemon_stats_table_sql = '''
CREATE TABLE IF NOT EXISTS weekly_api_pokemon_stats (
	day DATE,
	pokemon_id INTEGER NOT NULL,
	form VARCHAR(15) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci,
	avg_lat FLOAT,
	avg_lon FLOAT,
	total INTEGER,
	total_iv100 INTEGER,
	total_iv0 INTEGER,
	total_top1_little INTEGER,
	total_top1_great INTEGER,
	total_top1_ultra INTEGER,
	total_shiny INTEGER,
	area_name VARCHAR(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci,
	avg_despawn FLOAT,
    PRIMARY KEY (pokemon_id, form, area_name)
) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;
'''
# Monthly API grouped
create_monthly_api_pokemon_stats_table_sql = '''
CREATE TABLE IF NOT EXISTS monthly_api_pokemon_stats (
	day DATE,
	pokemon_id INTEGER NOT NULL,
	form VARCHAR(15) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci,
	avg_lat FLOAT,
	avg_lon FLOAT,
	total BIGINT,
	total_iv100 BIGINT,
	total_iv0 BIGINT,
	total_top1_little BIGINT,
	total_top1_great BIGINT,
	total_top1_ultra BIGINT,
	total_shiny BIGINT,
	area_name VARCHAR(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci,
	avg_despawn FLOAT,
    PRIMARY KEY (pokemon_id, form, area_name)
) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;
'''
# Hourly API total
create_hourly_total_api_pokemon_stats_table_sql = '''
CREATE TABLE IF NOT EXISTS hourly_total_api_pokemon_stats (
	area_name VARCHAR(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci,
	total INTEGER,
	total_iv100 MEDIUMINT,
	total_iv0 MEDIUMINT,
	total_top1_little MEDIUMINT,
	total_top1_great MEDIUMINT,
	total_top1_ultra MEDIUMINT,
	total_shiny MEDIUMINT,
	avg_despawn FLOAT,
	PRIMARY KEY (area_name)
) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;
'''
# Daily API total
create_daily_total_api_pokemon_stats_table_sql = '''
CREATE TABLE IF NOT EXISTS daily_total_api_pokemon_stats (
	day DATE,
	area_name VARCHAR(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci,
	total INTEGER,
	total_iv100 MEDIUMINT,
	total_iv0 MEDIUMINT,
	total_top1_little MEDIUMINT,
	total_top1_great MEDIUMINT,
	total_top1_ultra MEDIUMINT,
	total_shiny MEDIUMINT,
	avg_despawn FLOAT,
	PRIMARY KEY (area_name)
) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;
'''
# API total
create_total_api_pokemon_stats_table_sql = '''
CREATE TABLE IF NOT EXISTS total_api_pokemon_stats (
    area_name VARCHAR(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci,
    total BIGINT,
    total_iv100 BIGINT,
    total_iv0 BIGINT,
    total_top1_little BIGINT,
    total_top1_great BIGINT,
    total_top1_ultra BIGINT,
    total_shiny BIGINT,
    avg_despawn FLOAT,
    PRIMARY KEY (area_name)
) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;
'''
# Use ADDATE (returns current date without time)
# PROCEDURE to clean pokemon_sightings
create_procedure_clean_pokemon_batches = f'''
DROP PROCEDURE IF EXISTS delete_pokemon_sightings_batches;

CREATE PROCEDURE delete_pokemon_sightings_batches()
BEGIN
  DECLARE done INT DEFAULT FALSE;
  DECLARE CONTINUE HANDLER FOR NOT FOUND SET done = TRUE;

  WHILE NOT done DO
    DELETE FROM pokemon_sightings
    WHERE inserted_at < CURDATE() - INTERVAL 1 DAY
    LIMIT 50000;

    IF (ROW_COUNT() = 0) THEN
      SET done = TRUE;
    END IF;
  END WHILE;
END;
'''

# PROCEDURE for hourly_total_updates
create_procedure_update_hourly_total_stats = f'''
CREATE PROCEDURE update_hourly_total_stats()
BEGIN
    CREATE TEMPORARY TABLE IF NOT EXISTS temp_hourly_total_stats AS
    SELECT *
    FROM pokemon_sightings
    WHERE inserted_at >= NOW() - INTERVAL 60 MINUTE;

    CREATE TEMPORARY TABLE IF NOT EXISTS all_area_names AS
    SELECT DISTINCT area_name
    FROM pokemon_sightings;

    REPLACE INTO hourly_total_api_pokemon_stats
    SELECT
        a.area_name,
        COALESCE(t.total, 0) AS total,
        COALESCE(t.total_iv100, NULL) AS total_iv100,
        COALESCE(t.total_iv0, NULL) AS total_iv0,
        COALESCE(t.total_top1_little, NULL) AS total_top1_little,
        COALESCE(t.total_top1_great, NULL) AS total_top1_great,
        COALESCE(t.total_top1_ultra, NULL) AS total_top1_ultra,
        COALESCE(t.total_shiny, NULL) AS total_shiny,
        COALESCE(t.avg_despawn, NULL) AS avg_despawn
    FROM all_area_names a
    LEFT JOIN (
        SELECT
            area_name,
            COUNT(pokemon_id) AS total,
            SUM(CASE WHEN iv = 100 THEN 1 ELSE NULL END) AS total_iv100,
            SUM(CASE WHEN iv = 0 THEN 1 ELSE NULL END) AS total_iv0,
            SUM(CASE WHEN pvp_little_rank = 1 THEN 1 ELSE NULL END) AS total_top1_little,
            SUM(CASE WHEN pvp_great_rank = 1 THEN 1 ELSE NULL END) AS total_top1_great,
            SUM(CASE WHEN pvp_ultra_rank = 1 THEN 1 ELSE NULL END) AS total_top1_ultra,
            SUM(CASE WHEN shiny = 1 THEN 1 ELSE NULL END) AS total_shiny,
            AVG(despawn_time) AS avg_despawn
        FROM temp_hourly_total_stats
        GROUP BY area_name
    ) t ON a.area_name = t.area_name;

    DROP TEMPORARY TABLE IF EXISTS temp_hourly_total_stats;
    DROP TEMPORARY TABLE IF EXISTS all_area_names;
END;
'''
# EVENT to call procedure update_hourly_total_stats()
create_event_update_hourly_total_stats_sql = f'''
CREATE EVENT IF NOT EXISTS event_update_hourly_total_stats
ON SCHEDULE EVERY 1 HOUR
STARTS (TIMESTAMP(CURRENT_DATE) + INTERVAL 1 HOUR)
DO
CALL update_hourly_total_stats();
'''
# EVENT Cleaner using delete_pokemon_sightings_batches
create_event_clean_pokemon_sightings = f'''
CREATE EVENT IF NOT EXISTS clean_pokemon_sightings
ON SCHEDULE EVERY 1 DAY
STARTS ADDDATE(ADDDATE(CURDATE(), INTERVAL 1 DAY), INTERVAL 5 HOUR)
DO
CALL delete_pokemon_sightings_batches();
'''
# EVENT Grouped Daily Storage
create_event_store_daily_grouped_stats_sql = f'''
CREATE EVENT IF NOT EXISTS event_store_daily_grouped_stats
ON SCHEDULE EVERY 1 DAY
STARTS ADDDATE(CURDATE(), INTERVAL 1 DAY)
DO
BEGIN
    CREATE TEMPORARY TABLE IF NOT EXISTS temp_grouped_pokemon_sightings AS
    SELECT *
    FROM pokemon_sightings
    WHERE inserted_at >= CURDATE() - INTERVAL 1 DAY AND inserted_at < CURDATE();

    INSERT INTO grouped_total_daily_pokemon_stats (day, pokemon_id, form, avg_lat, avg_lon, total, total_iv100, total_iv0, total_top1_little, total_top1_great, total_top1_ultra, total_shiny, area_name, avg_despawn)
    SELECT
        CURDATE() - INTERVAL 1 DAY as day,
        pokemon_id,
        form,
        AVG(latitude) AS avg_lat,
        AVG(longitude) AS avg_lon,
        COUNT(pokemon_id) AS total,
        SUM(CASE WHEN iv = 100 THEN 1 ELSE NULL END) AS total_iv100,
        SUM(CASE WHEN iv = 0 THEN 1 ELSE NULL END) AS total_iv0,
        SUM(CASE WHEN pvp_little_rank = 1 THEN 1 ELSE NULL END) AS total_top1_little,
        SUM(CASE WHEN pvp_great_rank = 1 THEN 1 ELSE NULL END) AS total_top1_great,
        SUM(CASE WHEN pvp_ultra_rank = 1 THEN 1 ELSE NULL END) AS total_top1_ultra,
        SUM(CASE WHEN shiny = 1 THEN 1 ELSE NULL END) AS total_shiny,
        area_name,
        AVG(despawn_time) AS avg_despawn
    FROM temp_grouped_pokemon_sightings
    GROUP BY pokemon_id, form, area_name
    ORDER BY area_name, pokemon_id;

    DROP TEMPORARY TABLE IF EXISTS temp_grouped_pokemon_sightings;
END;
'''
# EVENT Total Daily Storage
create_event_store_daily_total_api_stats_sql = f'''
CREATE EVENT IF NOT EXISTS event_store_daily_total_api_stats
ON SCHEDULE EVERY 1 DAY
STARTS ADDDATE(CURDATE(), INTERVAL 1 DAY)
DO
BEGIN
	CREATE TEMPORARY TABLE IF NOT EXISTS temp_total_pokemon_sightings AS
	SELECT *
	FROM pokemon_sightings
	WHERE inserted_at >= CURDATE() - INTERVAL 1 DAY AND inserted_at < CURDATE();

    INSERT INTO daily_total_storage_pokemon_stats (day, area_name, total, total_iv100, total_iv0, total_top1_little, total_top1_great, total_top1_ultra, total_shiny, avg_despawn)
    SELECT
        CURDATE() - INTERVAL 1 DAY as day,
        area_name,
        COUNT(pokemon_id) AS total,
        SUM(CASE WHEN iv = 100 THEN 1 ELSE 0 END) AS total_iv100,
        SUM(CASE WHEN iv = 0 THEN 1 ELSE 0 END) AS total_iv0,
        SUM(CASE WHEN pvp_little_rank = 1 THEN 1 ELSE 0 END) AS total_top1_little,
        SUM(CASE WHEN pvp_great_rank = 1 THEN 1 ELSE 0 END) AS total_top1_great,
        SUM(CASE WHEN pvp_ultra_rank = 1 THEN 1 ELSE 0 END) AS total_top1_ultra,
        SUM(CASE WHEN shiny = 1 THEN 1 ELSE 0 END) AS total_shiny,
        AVG(despawn_time) AS avg_despawn
    FROM temp_total_pokemon_sightings
    GROUP BY area_name;

    DROP TEMPORARY TABLE IF EXISTS temp_total_pokemon_sightings;
END;
'''
# EVENT grouped Daily API
create_event_update_api_daily_stats_sql = f'''
CREATE EVENT IF NOT EXISTS event_update_api_daily_stats
ON SCHEDULE EVERY 1 DAY
STARTS ADDDATE(ADDDATE(CURDATE(), INTERVAL 1 DAY), INTERVAL 1 HOUR)
DO
BEGIN
    DELETE FROM daily_api_pokemon_stats
    WHERE day = CURDATE() - INTERVAL 1 DAY;

    INSERT INTO daily_api_pokemon_stats (day, pokemon_id, form, avg_lat, avg_lon, total, total_iv100, total_iv0, total_top1_little, total_top1_great, total_top1_ultra, total_shiny, area_name, avg_despawn)
    SELECT
        CURDATE() - INTERVAL 1 DAY as day,
        pokemon_id,
        form,
        avg_lat,
        avg_lon,
        total,
        total_iv100,
        total_iv0,
        total_top1_little,
        total_top1_great,
        total_top1_ultra,
        total_shiny,
        area_name,
        avg_despawn
    FROM grouped_total_daily_pokemon_stats
    WHERE day = CURDATE() - INTERVAL 1 DAY
    ORDER BY area_name, pokemon_id;
END;
'''
# EVENT grouped Weekly API -- rework needed
create_event_update_api_weekly_stats_sql = f'''
CREATE EVENT IF NOT EXISTS event_update_api_weekly_stats
ON SCHEDULE EVERY 1 WEEK
STARTS ADDDATE(ADDDATE(CURDATE(), INTERVAL 7 DAY), INTERVAL '1:15' HOUR_MINUTE)
DO
BEGIN
	DELETE FROM weekly_api_pokemon_stats
	WHERE day = CURDATE() - INTERVAL 7 DAY;

    INSERT INTO weekly_api_pokemon_stats (day, pokemon_id, form, avg_lat, avg_lon, total, total_iv100, total_iv0, total_top1_little, total_top1_great, total_top1_ultra, total_shiny, area_name, avg_despawn)
    SELECT
    	CURDATE() - INTERVAL 7 DAY AS day,
        pokemon_id,
        form,
        avg_lat,
        avg_lon,
        total,
        total_iv100,
        total_iv0,
        total_top1_little,
        total_top1_great,
        total_top1_ultra,
        total_shiny,
        area_name,
        avg_despawn
    FROM grouped_total_daily_pokemon_stats
    WHERE day = CURDATE() - INTERVAL 7 DAY
    ORDER BY area_name, pokemon_id;
END;
'''
# EVENT grouped Monthly API -- rework needed
create_event_update_api_monthly_stats_sql = f'''
CREATE EVENT IF NOT EXISTS event_update_api_monthly_stats
ON SCHEDULE EVERY 1 MONTH
STARTS ADDDATE(ADDDATE(CURDATE(), INTERVAL 1 MONTH), INTERVAL '2:10' HOUR_MINUTE)
DO
BEGIN
	DELETE FROM monthly_api_pokemon_stats
	WHERE day = CURDATE() - INTERVAL 1 MONTH;

    INSERT INTO monthly_api_pokemon_stats(day, pokemon_id, form, avg_lat, avg_lon, total, total_iv100, total_iv0, total_top1_little, total_top1_great, total_top1_ultra, total_shiny, area_name, avg_despawn)
    SELECT
    	CURTDATE() - INTERVAL 1 MONTH as day,
        pokemon_id,
        form,
        avg_lat,
        avg_lon,
        total,
        total_iv100,
        total_iv0,
        total_top1_little,
        total_top1_great,
        total_top1_ultra,
        total_shiny,
        area_name,
        avg_despawn
    FROM grouped_total_daily_pokemon_stats
    WHERE day = CURDATE() - INTERVAL 1 MONTH
    ORDER BY area_name, pokemon_id;
END;
'''
# EVENT total Daily API
create_event_update_daily_total_api_stats_sql = f'''
CREATE EVENT IF NOT EXISTS event_update_daily_total_api_stats
ON SCHEDULE EVERY 1 DAY
STARTS ADDDATE(ADDDATE(CURDATE(), INTERVAL 1 DAY), INTERVAL 1 HOUR)
DO
BEGIN
    DELETE FROM daily_api_pokemon_stats
    WHERE day = CURDATE() - INTERVAL 1 DAY;

    INSERT INTO daily_total_api_pokemon_stats (day, area_name, total, total_iv100, total_iv0, total_top1_little, total_top1_great, total_top1_ultra, total_shiny, avg_despawn)
    SELECT
    	CURDATE() - INTERVAL 1 DAY AS day,
        area_name,
        total,
        total_iv100,
        total_iv0,
        total_top1_little,
        total_top1_great,
        total_top1_ultra,
        total_shiny,
        avg_despawn
    FROM daily_total_storage_pokemon_stats
    WHERE day = CURDATE() - INTERVAL 1 DAY;
END;
'''
# EVENT total API
create_event_update_total_api_stats_sql = f'''
CREATE EVENT IF NOT EXISTS event_update_total_api_stats
ON SCHEDULE EVERY 1 DAY
STARTS ADDDATE(ADDDATE(CURDATE(), INTERVAL 1 DAY), INTERVAL '1:15' HOUR_MINUTE)
DO
	INSERT INTO total_api_pokemon_stats (area_name, total, total_iv100, total_iv0, total_top1_little, total_top1_great, total_top1_ultra, total_shiny, avg_despawn)
	SELECT
    	d.area_name,
    	d.total,
    	d.total_iv100,
    	d.total_iv0,
    	d.total_top1_little,
    	d.total_top1_great,
    	d.total_top1_ultra,
    	d.total_shiny,
    	d.avg_despawn
	FROM daily_total_storage_pokemon_stats d
	WHERE d.day = CURDATE() - INTERVAL 1 DAY
	ON DUPLICATE KEY UPDATE
    	total = total_api_pokemon_stats.total + d.total,
    	total_iv100 = total_api_pokemon_stats.total_iv100 + d.total_iv100,
    	total_iv0 = total_api_pokemon_stats.total_iv0 + d.total_iv0,
    	total_top1_little = total_api_pokemon_stats.total_top1_little + d.total_top1_little,
    	total_top1_great = total_api_pokemon_stats.total_top1_great + d.total_top1_great,
    	total_top1_ultra = total_api_pokemon_stats.total_top1_ultra + d.total_top1_ultra,
    	total_shiny = total_api_pokemon_stats.total_shiny + d.total_shiny,
    	avg_despawn = (total_api_pokemon_stats.avg_despawn + d.avg_despawn) / 2;
'''

def create_cursor(connection):
	try:
		if connection.is_connected():
			return connection.cursor()
		else:
			print("Connection is not established. Reconnecting...")
			connection.reconnect(attempts=3, delay=5)

		if connection.is_connected():
			connection.database = db_name
			return connection.cursor()
		else:
			print("Failed to re-establish database connection.")
			return None
	except Error as err:
		print(f"Error creating cursor: {err}")
		return None

def close_cursor(cursor):
	if cursor is not None:
		cursor.close()

def handle_multiple_results(connection):
	"""
	Consumes all remaining results from the connection to avoid 'Commands out of sync' error.
	"""
	cursor = connection.cursor()
	while True:
		try:
			cursor.fetchall()
			if not connection.more_results():
				break
		except mysql.connector.Error as e:
			print("No more results to fetch.")
			break
	cursor.close()

def execute_procedure(conn, procedure_sql, procedure_name):
	try:
			with conn.cursor() as cursor:
			# Check if the procedure already exists
			cursor.execute(f"SELECT ROUTINE_NAME FROM INFORMATION_SCHEMA.ROUTINES WHERE ROUTINE_SCHEMA = '{db_name}' AND ROUTINE_TYPE = 'PROCEDURE' AND ROUTINE_NAME = '{procedure_name}'")
			if cursor.fetchone():
				print(f"Procedure {procedure_name} already exists.")
			else:
				# Execute and create procedure
				print(f"Creating procedure: {procedure_name}")
				cursor.execute(procedure_sql)
				conn.commit()
				print(f"Procedure {procedure_name} created successfully.")
	except Error as e:
		print(f"Error in procedure {procedure_name}: {e}")

def create_database_schema():
	try:
		with mysql.connector.connect(**db_config) as conn:
			if conn.is_connected():
				print("sucessfully connected to the database")
			else:
				print("Database connection failed. Please check your configuration")
				return

			conn.database = db_name


			with conn.cursor() as cursor:
				# Create Database
				cursor.execute(f"SELECT SCHEMA_NAME FROM INFORMATION_SCHEMA.SCHEMATA WHERE SCHEMA_NAME = '{db_name}'")
				db_exists = cursor.fetchone() is not None
				if not db_exists:
					cursor.execute(create_database_sql)
					print(f"Database {db_name} created.")
				else:
					print(f"Database {db_name} already existed.")



				def check_and_create_table(table_sql, table_name):
					cursor.execute(f"SHOW TABLES LIKE '{table_name}'")
					table_exists = cursor.fetchone() is not None
					if table_exists:
						print(f"Table {table_name} already existed.")
					else:
						cursor.execute(table_sql)
						print(f"Table {table_name} created.")

				# Create Raw Table
				check_and_create_table(create_pokemon_sightings_table_sql, 'pokemon_sightings')

				# Create Storage Tables
				check_and_create_table(create_grouped_total_daily_pokemon_stats_table_sql, 'grouped_total_daily_pokemon_stats')
				check_and_create_table(create_daily_total_storage_pokemon_stats_table_sql, 'daily_total_storage_pokemon_stats')

				# Create API Tables
				check_and_create_table(create_daily_api_pokemon_stats_sql, 'daily_api_pokemon_stats')
				check_and_create_table(create_weekly_api_pokemon_stats_table_sql, 'weekly_api_pokemon_stats')
				check_and_create_table(create_monthly_api_pokemon_stats_table_sql, 'monthly_api_pokemon_stats')
				check_and_create_table(create_hourly_total_api_pokemon_stats_table_sql, 'hourly_total_api_pokemon_stats')
				check_and_create_table(create_daily_total_api_pokemon_stats_table_sql, 'daily_total_api_pokemon_stats')
				check_and_create_table(create_total_api_pokemon_stats_table_sql, 'total_api_pokemon_stats')

				execute_procedure(create_procedure_clean_pokemon_batches, 'delete_pokemon_sightings_batches')
				execute_procedure(create_procedure_update_hourly_total_stats, 'update_hourly_total_stats')


				def check_and_create_event(event_sql, event_name):
					cursor.execute(f"SELECT EVENT_NAME FROM INFORMATION_SCHEMA.EVENTS WHERE EVENT_SCHEMA = '{db_name}' AND EVENT_NAME = '{event_name}'")
					event_exists = cursor.fetchone() is not None
					if event_exists:
						print(f"Event {event_name} already existed.")
					else:
						cursor.execute(event_sql)
						print(f"Event {event_name} created.")

				# Create Storage Events
				check_and_create_event(create_event_store_daily_grouped_stats_sql, 'event_store_daily_grouped_stats')
				check_and_create_event(create_event_store_daily_total_api_stats_sql, 'event_store_daily_total_api_stats')

				# Create Updating Events
				check_and_create_event(create_event_update_api_daily_stats_sql, 'event_update_api_daily_stats')
				check_and_create_event(create_event_update_api_weekly_stats_sql, 'event_update_api_weekly_stats')
				check_and_create_event(create_event_update_api_monthly_stats_sql, 'event_update_api_monthly_stats')
				check_and_create_event(create_event_update_hourly_total_stats_sql, 'event_update_hourly_total_stats')
				check_and_create_event(create_event_update_daily_total_api_stats_sql, 'event_update_daily_total_api_stats')
				check_and_create_event(create_event_update_total_api_stats_sql, 'event_update_total_api_stats')

				# Create Cleaning Event if db_clean = true
				if db_clean:
					check_and_create_event(create_event_clean_pokemon_sightings, 'clean_pokemon_sightings')
				else:
					print("Database cleaning event skipped as per configuration.")

			conn.commit()
			print("Schema created sucessfully")

	except Error as err:
		if err.errno == mysql.connector.errorcode.ER_ACCESS_DENIED_ERROR:
			print("Invalid username or password.")
		elif err.errno == mysql.connector.errorcode.ER_BAD_DB_ERROR:
			print("Database does not exist.")
		else:
			print(f"Error: {err}")
	finally:
		if conn.is_connected():
			close_cursor(cursor)
			conn.close()
			print("MySQL connection is closed.")

if __name__ == '__main__':
	create_database_schema()
