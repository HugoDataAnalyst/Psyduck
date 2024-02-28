-- Index for TTH
ALTER TABLE pokemon_sightings
ADD INDEX idx_inserted_at_area_name (inserted_at, area_name);

-- Table for TTH
-- Storage
CREATE TABLE IF NOT EXISTS storage_hourly_pokemon_tth_stats(
    `day_hour` DATETIME NOT NULL,
    area_name VARCHAR(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci NOT NULL,
    tth_5 INT DEFAULT 0,
    tth_10 INT DEFAULT 0,
    tth_15 INT DEFAULT 0,
    tth_20 INT DEFAULT 0,
    tth_25 INT DEFAULT 0,
    tth_30 INT DEFAULT 0,
    tth_35 INT DEFAULT 0,
    tth_40 INT DEFAULT 0,
    tth_45 INT DEFAULT 0,
    tth_50 INT DEFAULT 0,
    tth_55 INT DEFAULT 0,
    tth_55_plus INT DEFAULT 0,
    PRIMARY KEY (area_name, `day_hour`),
    INDEX idx_day_hour (`day_hour`)
) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;

-- Hourly
CREATE TABLE IF NOT EXISTS hourly_pokemon_tth_stats(
    area_name VARCHAR(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci NOT NULL,
    tth_5 INT DEFAULT 0,
    tth_10 INT DEFAULT 0,
    tth_15 INT DEFAULT 0,
    tth_20 INT DEFAULT 0,
    tth_25 INT DEFAULT 0,
    tth_30 INT DEFAULT 0,
    tth_35 INT DEFAULT 0,
    tth_40 INT DEFAULT 0,
    tth_45 INT DEFAULT 0,
    tth_50 INT DEFAULT 0,
    tth_55 INT DEFAULT 0,
    tth_55_plus INT DEFAULT 0,
    PRIMARY KEY (area_name)
) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;


-- Daily
CREATE TABLE IF NOT EXISTS daily_pokemon_tth_stats(
    hour TINYINT,
    area_name VARCHAR(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci NOT NULL,
    total_tth_5 INT DEFAULT 0,
    total_tth_10 INT DEFAULT 0,
    total_tth_15 INT DEFAULT 0,
    total_tth_20 INT DEFAULT 0,
    total_tth_25 INT DEFAULT 0,
    total_tth_30 INT DEFAULT 0,
    total_tth_35 INT DEFAULT 0,
    total_tth_40 INT DEFAULT 0,
    total_tth_45 INT DEFAULT 0,
    total_tth_50 INT DEFAULT 0,
    total_tth_55 INT DEFAULT 0,
    total_tth_55_plus INT DEFAULT 0,
    PRIMARY KEY (area_name, hour),
    INDEX idx_hour (hour)
) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;


-- Procedures
-- Storage
DROP PROCEDURE IF EXISTS store_storage_hourly_pokemon_tth_stats;
CREATE PROCEDURE store_storage_hourly_pokemon_tth_stats()
BEGIN
    DROP TEMPORARY TABLE IF EXISTS temp_spawn_tth_by_area;
    CREATE TEMPORARY TABLE temp_spawn_tth_by_area AS
    SELECT
        STR_TO_DATE(DATE_FORMAT(NOW() - INTERVAL 1 HOUR, '%Y-%m-%d %H:00:00'), '%Y-%m-%d %H:%i:%s') AS `day_hour`,
        area_name,
        SUM(CASE WHEN despawn_time < 300 THEN 1 ELSE 0 END) AS tth_5,
        SUM(CASE WHEN despawn_time >= 300 AND despawn_time < 600 THEN 1 ELSE 0 END) AS tth_10,
        SUM(CASE WHEN despawn_time >= 600 AND despawn_time < 900 THEN 1 ELSE 0 END) AS tth_15,
        SUM(CASE WHEN despawn_time >= 900 AND despawn_time < 1200 THEN 1 ELSE 0 END) AS tth_20,
        SUM(CASE WHEN despawn_time >= 1200 AND despawn_time < 1500 THEN 1 ELSE 0 END) AS tth_25,
        SUM(CASE WHEN despawn_time >= 1500 AND despawn_time < 1800 THEN 1 ELSE 0 END) AS tth_30,
        SUM(CASE WHEN despawn_time >= 1800 AND despawn_time < 2100 THEN 1 ELSE 0 END) AS tth_35,
        SUM(CASE WHEN despawn_time >= 2100 AND despawn_time < 2400 THEN 1 ELSE 0 END) AS tth_40,
        SUM(CASE WHEN despawn_time >= 2400 AND despawn_time < 2700 THEN 1 ELSE 0 END) AS tth_45,
        SUM(CASE WHEN despawn_time >= 2700 AND despawn_time < 3000 THEN 1 ELSE 0 END) AS tth_50,
        SUM(CASE WHEN despawn_time >= 3000 AND despawn_time < 3300 THEN 1 ELSE 0 END) AS tth_55,
        SUM(CASE WHEN despawn_time >= 3300 THEN 1 ELSE 0 END) AS tth_55_plus
    FROM pokemon_sightings
    WHERE inserted_at >= NOW() - INTERVAL 1 HOUR
    GROUP BY area_name;

    INSERT INTO storage_hourly_pokemon_tth_stats (`day_hour`, area_name, tth_5, tth_10, tth_15, tth_20, tth_25, tth_30, tth_35, tth_40, tth_45, tth_50, tth_55, tth_55_plus)
    SELECT
    	`day_hour`,
    	area_name,
    	tth_5,
    	tth_10,
    	tth_15,
    	tth_20,
    	tth_25,
    	tth_30,
    	tth_35,
    	tth_40,
    	tth_45,
    	tth_50,
    	tth_55,
    	tth_55_plus
    FROM temp_spawn_tth_by_area;

    DROP TEMPORARY TABLE IF EXISTS temp_spawn_tth_by_area;
END;

-- Hourly
DROP PROCEDURE IF EXISTS update_hourly_pokemon_tth_stats;
CREATE PROCEDURE update_hourly_pokemon_tth_stats()
BEGIN
	TRUNCATE TABLE hourly_pokemon_tth_stats;

	INSERT INTO hourly_pokemon_tth_stats (area_name, tth_5, tth_10, tth_15, tth_20, tth_25, tth_30,	tth_35, tth_40, tth_45, tth_50, tth_55, tth_55_plus)
	SELECT
    	area_name,
    	tth_5,
    	tth_10,
    	tth_15,
    	tth_20,
    	tth_25,
    	tth_30,
    	tth_35,
    	tth_40,
    	tth_45,
    	tth_50,
    	tth_55,
    	tth_55_plus
    FROM storage_hourly_pokemon_tth_stats
    WHERE `day_hour` = DATE_FORMAT(NOW() - INTERVAL 1 HOUR, '%Y-%m-%d %H:00:00');
END;
-- Daily
DROP PROCEDURE IF EXISTS update_daily_pokemon_tth_stats;
CREATE PROCEDURE update_daily_pokemon_tth_stats()
BEGIN
	DROP TEMPORARY TABLE IF EXISTS temp_daily_pokemon_tth;
	CREATE TEMPORARY TABLE IF NOT EXISTS temp_daily_pokemon_tth AS
	SELECT
		HOUR(`day_hour`) AS hour_of_day,
		area_name,
		SUM(tth_5) AS total_tth_5,
		SUM(tth_10) AS total_tth_10,
		SUM(tth_15) AS total_tth_15,
		SUM(tth_20) AS total_tth_20,
		SUM(tth_25) AS total_tth_25,
		SUM(tth_30) AS total_tth_30,
		SUM(tth_35) AS total_tth_35,
		SUM(tth_40) AS total_tth_40,
		SUM(tth_45) AS total_tth_45,
		SUM(tth_50) AS total_tth_50,
		SUM(tth_55) AS total_tth_55,
		SUM(tth_55_plus) AS total_tth_55_plus
	FROM storage_hourly_pokemon_tth_stats
	WHERE DATE(`day_hour`) = CURDATE() - INTERVAL 1 DAY
	GROUP BY hour_of_day, area_name;

	TRUNCATE TABLE daily_pokemon_tth_stats;

	INSERT INTO daily_pokemon_tth_stats (hour, area_name, total_tth_5, total_tth_10, total_tth_15, total_tth_20, total_tth_25, total_tth_30, total_tth_35, total_tth_40, total_tth_45, total_tth_50, total_tth_55, total_tth_55_plus)
	SELECT
        hour_of_day,
        area_name,
        total_tth_5,
        total_tth_10,
        total_tth_15,
        total_tth_20,
        total_tth_25,
        total_tth_30,
        total_tth_35,
        total_tth_40,
        total_tth_45,
        total_tth_50,
        total_tth_55,
        total_tth_55_plus
	FROM temp_daily_pokemon_tth;
	DROP TEMPORARY TABLE IF EXISTS temp_daily_pokemon_tth;
END;

-- Events
-- Storage
CREATE EVENT IF NOT EXISTS event_store_hourly_pokemon_tth_stats
ON SCHEDULE EVERY 1 HOUR
STARTS (CURRENT_TIMESTAMP + INTERVAL 1 HOUR - INTERVAL MINUTE(CURRENT_TIMESTAMP) MINUTE - INTERVAL SECOND(CURRENT_TIMESTAMP) SECOND)
DO
CALL store_storage_hourly_pokemon_tth_stats();

-- Hourly
CREATE EVENT IF NOT EXISTS event_update_hourly_pokemon_tth_stats
ON SCHEDULE EVERY 1 HOUR
STARTS (CURRENT_TIMESTAMP + INTERVAL 1 HOUR - INTERVAL MINUTE(CURRENT_TIMESTAMP) MINUTE - INTERVAL SECOND(CURRENT_TIMESTAMP) SECOND + INTERVAL 1 MINUTE)
DO
CALL update_hourly_pokemon_tth_stats();

-- Daily
CREATE EVENT IF NOT EXISTS event_update_daily_pokemon_tth_stats
ON SCHEDULE EVERY 1 DAY
STARTS ADDDATE(ADDDATE(CURDATE(), INTERVAL 1 DAY), INTERVAL '02:00:00' HOUR_SECOND)
DO
CALL update_daily_pokemon_tth_stats();