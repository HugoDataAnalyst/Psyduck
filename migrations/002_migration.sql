-- Properly update event for api_monthly_stats

ALTER EVENT event_update_api_monthly_stats
ON SCHEDULE EVERY 1 MONTH
STARTS STR_TO_DATE(CONCAT(YEAR(NOW()), '-', MONTH(NOW()) + (DAY(NOW()) > 1), '-01 02:10:00'), '%Y-%m-%d %H:%i:%s')
DO
BEGIN
    TRUNCATE TABLE monthly_api_pokemon_stats;

    INSERT INTO monthly_api_pokemon_stats(day, pokemon_id, form, avg_lat, avg_lon, total, total_iv100, total_iv0, total_top1_little, total_top1_great, total_top1_ultra, total_shiny, area_name, avg_despawn)
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
    FROM grouped_total_daily_pokemon_stats
    WHERE day >= CURDATE() - INTERVAL 1 MONTH
    GROUP BY pokemon_id, form, area_name
    ORDER BY area_name, pokemon_id;
END;

-- Correct monthly timer for surge

ALTER EVENT event_update_monthly_surge_stats
ON SCHEDULE EVERY 1 MONTH
STARTS STR_TO_DATE(CONCAT(YEAR(NOW()), '-', MONTH(NOW()) + (DAY(NOW()) > 1), '-01 02:10:00'), '%Y-%m-%d %H:%i:%s')
DO
CALL update_monthly_surge_stats();

-- Correct weekly events as well.

ALTER EVENT event_update_api_weekly_stats
ON SCHEDULE EVERY 1 WEEK
STARTS STR_TO_DATE(CONCAT(DATE_FORMAT(CURDATE() + INTERVAL 8 - DAYOFWEEK(CURDATE()) DAY, '%Y-%m-%d'), ' 01:15:00'), '%Y-%m-%d %H:%i:%s')
DO
BEGIN
    TRUNCATE TABLE weekly_api_pokemon_stats;

    INSERT INTO weekly_api_pokemon_stats (day, pokemon_id, form, avg_lat, avg_lon, total, total_iv100, total_iv0, total_top1_little, total_top1_great, total_top1_ultra, total_shiny, area_name, avg_despawn)
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
    FROM grouped_total_daily_pokemon_stats
    WHERE day >= DATE_SUB(CURDATE(), INTERVAL DAYOFWEEK(CURDATE()) + 6 DAY) AND day < DATE_SUB(CURDATE(), INTERVAL DAYOFWEEK(CURDATE()) - 1 DAY)
    GROUP BY pokemon_id, form, area_name
    ORDER BY area_name, pokemon_id;
END;

-- surge weekly event

ALTER EVENT event_update_weekly_surge_stats
ON SCHEDULE EVERY 1 WEEK
STARTS STR_TO_DATE(CONCAT(DATE_FORMAT(CURDATE() + INTERVAL 8 - DAYOFWEEK(CURDATE()) DAY, '%Y-%m-%d'), ' 01:15:00'), '%Y-%m-%d %H:%i:%s')
DO
CALL update_weekly_surge_stats();

-- Correct procedure for surge monthly

DROP PROCEDURE IF EXISTS update_monthly_surge_stats;

CREATE PROCEDURE update_monthly_surge_stats()
BEGIN

    CREATE TEMPORARY TABLE IF NOT EXISTS temp_monthly_surge AS
    SELECT
        HOUR(hour) AS hour_of_day,
        SUM(total_iv100) AS sum_total_iv100,
        SUM(total_iv0) AS sum_total_iv0,
        SUM(total_top1_little) AS sum_total_top1_little,
        SUM(total_top1_great) AS sum_total_top1_great,
        SUM(total_top1_ultra) AS sum_total_top1_ultra,
        SUM(total_shiny) AS sum_total_shiny
    FROM hourly_surge_storage_pokemon_stats
    WHERE hour >= NOW() - INTERVAL 1 MONTH AND hour < NOW()
    GROUP BY HOUR(hour);

    TRUNCATE TABLE monthly_surge_pokemon_stats;

    INSERT INTO monthly_surge_pokemon_stats (hour, total_iv100, total_iv0, total_top1_little, total_top1_great, total_top1_ultra, total_shiny)
    SELECT
        hour_of_day,
        sum_total_iv100,
        sum_total_iv0,
        sum_total_top1_little,
        sum_total_top1_great,
        sum_total_top1_ultra,
        sum_total_shiny
    FROM temp_monthly_surge;

    DROP TEMPORARY TABLE IF EXISTS temp_monthly_surge;
END;

-- Dynamic hour updates instead of using current date as a start point
-- Dynamic hour for surge

DROP EVENT IF EXISTS event_update_hourly_total_stats;

CREATE EVENT IF NOT EXISTS event_update_hourly_total_stats
ON SCHEDULE EVERY 1 HOUR
STARTS (CURRENT_TIMESTAMP + INTERVAL 1 HOUR - INTERVAL MINUTE(CURRENT_TIMESTAMP) MINUTE + INTERVAL 2 MINUTE - INTERVAL SECOND(CURRENT_TIMESTAMP) SECOND)
DO
  CALL update_hourly_total_stats();

DROP EVENT IF EXISTS event_update_hourly_surge_stats;

CREATE EVENT IF NOT EXISTS event_update_hourly_surge_stats
ON SCHEDULE EVERY 1 HOUR
STARTS (CURRENT_TIMESTAMP + INTERVAL 1 HOUR - INTERVAL MINUTE(CURRENT_TIMESTAMP) MINUTE + INTERVAL 2 MINUTE - INTERVAL SECOND(CURRENT_TIMESTAMP) SECOND)
DO
  CALL update_hourly_surge_stats();
