-- Let's rename the old tables accordingly
RENAME TABLE daily_api_pokemon_stats TO daily_pokemon_grouped_stats;
RENAME TABLE weekly_api_pokemon_stats TO weekly_pokemon_grouped_stats;
RENAME TABLE monthly_api_pokemon_stats TO monthly_pokemon_grouped_stats;
RENAME TABLE daily_total_storage_pokemon_stats TO storage_pokemon_total_stats;
RENAME TABLE daily_total_api_pokemon_stats TO daily_pokemon_total_stats;
RENAME TABLE grouped_total_daily_pokemon_stats TO storage_pokemon_grouped_stats;
RENAME TABLE hourly_total_api_pokemon_stats TO hourly_pokemon_total_stats;
RENAME TABLE total_api_pokemon_stats TO pokemon_total_stats;

-- Redo Events and Procedures
DROP EVENT IF EXISTS event_update_api_daily_stats;

DROP PROCEDURE IF EXISTS update_daily_pokemon_grouped_stats;
CREATE PROCEDURE update_daily_pokemon_grouped_stats()
BEGIN
    TRUNCATE TABLE daily_pokemon_grouped_stats;

    INSERT INTO daily_pokemon_grouped_stats (day, pokemon_id, form, avg_lat, avg_lon, total, total_iv100, total_iv0, total_top1_little, total_top1_great, total_top1_ultra, total_shiny, area_name, avg_despawn)
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
    FROM storage_pokemon_grouped_stats
    WHERE day = CURDATE() - INTERVAL 1 DAY
    ORDER BY area_name, pokemon_id;
END;

CREATE EVENT IF NOT EXISTS event_update_daily_pokemon_grouped_stats
ON SCHEDULE EVERY 1 DAY
STARTS ADDDATE(ADDDATE(CURDATE(), INTERVAL 1 DAY), INTERVAL 1 HOUR)
DO
CALL update_daily_pokemon_grouped_stats();


DROP EVENT IF EXISTS event_update_api_weekly_stats;

DROP PROCEDURE IF EXISTS update_weekly_pokemon_grouped_stats;
CREATE PROCEDURE update_weekly_pokemon_grouped_stats()
BEGIN
    TRUNCATE TABLE weekly_pokemon_grouped_stats;

    INSERT INTO weekly_pokemon_grouped_stats (day, pokemon_id, form, avg_lat, avg_lon, total, total_iv100, total_iv0, total_top1_little, total_top1_great, total_top1_ultra, total_shiny, area_name, avg_despawn)
    SELECT
        DATE_SUB(CURDATE(), INTERVAL DAYOFWEEK(CURDATE()) - 1 DAY) as day,
        pokemon_id,
        form,
        AVG(avg_lat) as avg_lat,
        AVG(avg_lon) as avg_lon,
        SUM(total) as total,
        SUM(total_iv100) as total_iv100,
        SUM(total_iv0) as total_iv0,
        SUM(total_top1_little) as total_top1_little,
        SUM(total_top1_great) as total_top1_great,
        SUM(total_top1_ultra) as total_top1_ultra,
        SUM(total_shiny) as total_shiny,
        area_name,
        AVG(avg_despawn) as avg_despawn
    FROM storage_pokemon_grouped_stats
    WHERE day >= DATE_SUB(CURDATE(), INTERVAL DAYOFWEEK(CURDATE()) + 6 DAY) AND day < DATE_SUB(CURDATE(), INTERVAL DAYOFWEEK(CURDATE()) - 1 DAY)
    GROUP BY pokemon_id, form, area_name
    ORDER BY area_name, pokemon_id;
END;

CREATE EVENT IF NOT EXISTS event_update_weekly_pokemon_grouped_stats
ON SCHEDULE EVERY 1 WEEK
STARTS STR_TO_DATE(CONCAT(DATE_FORMAT(CURDATE() + INTERVAL 8 - DAYOFWEEK(CURDATE()) DAY, '%Y-%m-%d'), ' 01:15:00'), '%Y-%m-%d %H:%i:%s')
DO
CALL update_weekly_pokemon_grouped_stats();


DROP EVENT IF EXISTS event_update_api_monthly_stats;

DROP PROCEDURE IF EXISTS update_monthly_pokemon_grouped_stats;
CREATE PROCEDURE update_monthly_pokemon_grouped_stats()
BEGIN
    TRUNCATE TABLE monthly_pokemon_grouped_stats;

    INSERT INTO monthly_pokemon_grouped_stats(day, pokemon_id, form, avg_lat, avg_lon, total, total_iv100, total_iv0, total_top1_little, total_top1_great, total_top1_ultra, total_shiny, area_name, avg_despawn)
    SELECT
        LAST_DAY(CURDATE() - INTERVAL 1 MONTH) as day,
        pokemon_id,
        form,
        AVG(avg_lat) as avg_lat,
        AVG(avg_lon) as avg_lon,
        SUM(total) as total,
        SUM(total_iv100) as total_iv100,
        SUM(total_iv0) as total_iv0,
        SUM(total_top1_little) as total_top1_little,
        SUM(total_top1_great) as total_top1_great,
        SUM(total_top1_ultra) as total_top1_ultra,
        SUM(total_shiny) as total_shiny,
        area_name,
        AVG(avg_despawn) as avg_despawn
    FROM storage_pokemon_grouped_stats
    WHERE day >= CURDATE() - INTERVAL 1 MONTH
    GROUP BY pokemon_id, form, area_name
    ORDER BY area_name, pokemon_id;
END;

CREATE EVENT IF NOT EXISTS event_update_monthly_pokemon_grouped_stats
ON SCHEDULE EVERY 1 MONTH
STARTS STR_TO_DATE(CONCAT(YEAR(NOW()), '-', MONTH(NOW()) + (DAY(NOW()) > 1), '-01 02:10:00'), '%Y-%m-%d %H:%i:%s')
DO
CALL update_monthly_pokemon_grouped_stats();


DROP EVENT IF EXISTS event_store_daily_grouped_stats;

DROP PROCEDURE IF EXISTS store_pokemon_grouped_stats;
CREATE PROCEDURE store_pokemon_grouped_stats()
BEGIN
    CREATE TEMPORARY TABLE IF NOT EXISTS temp_grouped_pokemon_sightings AS
    SELECT *
    FROM pokemon_sightings
    WHERE inserted_at >= CURDATE() - INTERVAL 1 DAY AND inserted_at < CURDATE();

    INSERT INTO storage_pokemon_grouped_stats (day, pokemon_id, form, avg_lat, avg_lon, total, total_iv100, total_iv0, total_top1_little, total_top1_great, total_top1_ultra, total_shiny, area_name, avg_despawn)
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

CREATE EVENT IF NOT EXISTS store_storage_pokemon_grouped_stats
ON SCHEDULE EVERY 1 DAY
STARTS ADDDATE(CURDATE(), INTERVAL 1 DAY)
DO
CALL store_pokemon_grouped_stats();


DROP EVENT IF EXISTS event_store_daily_total_api_stats;

DROP PROCEDURE IF EXISTS store_pokemon_total_stats;
CREATE PROCEDURE store_pokemon_total_stats()
BEGIN
	CREATE TEMPORARY TABLE IF NOT EXISTS temp_total_pokemon_sightings AS
	SELECT *
	FROM pokemon_sightings
	WHERE inserted_at >= CURDATE() - INTERVAL 1 DAY AND inserted_at < CURDATE();

    INSERT INTO storage_pokemon_total_stats (day, area_name, total, total_iv100, total_iv0, total_top1_little, total_top1_great, total_top1_ultra, total_shiny, avg_despawn)
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

CREATE EVENT IF NOT EXISTS store_storage_pokemon_total_stats
ON SCHEDULE EVERY 1 DAY
STARTS ADDDATE(CURDATE(), INTERVAL 1 DAY)
DO
CALL store_pokemon_total_stats();


DROP EVENT IF EXISTS event_update_daily_total_api_stats;

DROP PROCEDURE IF EXISTS update_daily_pokemon_total_stats;
CREATE PROCEDURE update_daily_pokemon_total_stats()
BEGIN
    TRUNCATE TABLE daily_pokemon_total_stats;

    INSERT INTO daily_pokemon_total_stats (day, area_name, total, total_iv100, total_iv0, total_top1_little, total_top1_great, total_top1_ultra, total_shiny, avg_despawn)
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
    FROM storage_pokemon_total_stats
    WHERE day = CURDATE() - INTERVAL 1 DAY;
END;

CREATE EVENT IF NOT EXISTS event_update_daily_pokemon_total_stats
ON SCHEDULE EVERY 1 DAY
STARTS ADDDATE(ADDDATE(CURDATE(), INTERVAL 1 DAY), INTERVAL 1 HOUR)
DO
CALL update_daily_pokemon_total_stats();


DROP EVENT IF EXISTS event_update_hourly_total_stats;

DROP PROCEDURE IF EXISTS update_hourly_total_stats;

DROP PROCEDURE IF EXISTS update_hourly_pokemon_total_stats;
CREATE PROCEDURE update_hourly_pokemon_total_stats()
BEGIN
    CREATE TEMPORARY TABLE IF NOT EXISTS temp_hourly_total_stats AS
    SELECT *
    FROM pokemon_sightings
    WHERE inserted_at >= NOW() - INTERVAL 60 MINUTE;

    CREATE TEMPORARY TABLE IF NOT EXISTS all_area_names AS
    SELECT DISTINCT area_name
    FROM pokemon_sightings;

    REPLACE INTO hourly_pokemon_total_stats
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

CREATE EVENT IF NOT EXISTS event_update_hourly_pokemon_total_stats
ON SCHEDULE EVERY 1 HOUR
STARTS (CURRENT_TIMESTAMP + INTERVAL 1 HOUR - INTERVAL MINUTE(CURRENT_TIMESTAMP) MINUTE - INTERVAL SECOND(CURRENT_TIMESTAMP) SECOND)
DO
CALL update_hourly_pokemon_total_stats();


DROP EVENT IF EXISTS event_update_total_api_stats;

DROP PROCEDURE IF EXISTS update_pokemon_total_stats;
CREATE PROCEDURE update_pokemon_total_stats()
BEGIN
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
	FROM storage_pokemon_total_stats d
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
END;

CREATE EVENT IF NOT EXISTS event_update_pokemon_total_stats
ON SCHEDULE EVERY 1 DAY
STARTS ADDDATE(ADDDATE(CURDATE(), INTERVAL 1 DAY), INTERVAL '1:15' HOUR_MINUTE)
DO
CALL update_pokemon_total_stats();

-- Add the schema_version 0
INSERT INTO schema_version (version)
SELECT 0
FROM (SELECT 1) AS dummy
LEFT JOIN schema_version sv ON sv.version = 0
WHERE sv.version IS NULL;
