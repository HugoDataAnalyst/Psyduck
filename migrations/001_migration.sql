-- Create storage table
CREATE TABLE IF NOT EXISTS hourly_surge_storage_pokemon_stats (
    hour DATETIME,
    total_iv100 MEDIUMINT,
    total_iv0 MEDIUMINT,
    total_top1_little MEDIUMINT,
    total_top1_great MEDIUMINT,
    total_top1_ultra MEDIUMINT,
    total_shiny MEDIUMINT,
    PRIMARY KEY (hour)
) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;

-- Create PROCEDURE for hourly updating storage surge
DELIMITER //

CREATE PROCEDURE update_hourly_surge_stats()
BEGIN
    CREATE TEMPORARY TABLE IF NOT EXISTS temp_hourly_surge_stats AS
    SELECT *
    FROM pokemon_sightings
    WHERE inserted_at >= NOW() - INTERVAL 60 MINUTE;

    INSERT INTO hourly_surge_storage_pokemon_stats (hour, total_iv100, total_iv0, total_top1_little, total_top1_great, total_top1_ultra, total_shiny)
    SELECT
        STR_TO_DATE(DATE_FORMAT(NOW(), '%Y-%m-%d %H:00:00'), '%Y-%m-%d %H:%i:%s') AS hour, 
        SUM(CASE WHEN iv = 100 THEN 1 ELSE NULL END) AS total_iv100,
        SUM(CASE WHEN iv = 0 THEN 1 ELSE NULL END) AS total_iv0,
        SUM(CASE WHEN pvp_little_rank = 1 THEN 1 ELSE NULL END) AS total_top1_little,
        SUM(CASE WHEN pvp_great_rank = 1 THEN 1 ELSE NULL END) AS total_top1_great,
        SUM(CASE WHEN pvp_ultra_rank = 1 THEN 1 ELSE NULL END) AS total_top1_ultra,
        SUM(CASE WHEN shiny = 1 THEN 1 ELSE NULL END) AS total_shiny
    FROM temp_hourly_surge_stats
    ON DUPLICATE KEY UPDATE
        total_iv100 = VALUES(total_iv100),
        total_iv0 = VALUES (total_iv0),
        total_top1_little = VALUES(total_top1_little),
        total_top1_great = VALUES(total_top1_great),
        total_top1_ultra = VALUES(total_top1_ultra),
        total_shiny = VALUES(total_shiny);

    DROP TEMPORARY TABLE IF EXISTS temp_hourly_surge_stats;
END //

DELIMITER ;
-- Create EVENT to run the hourly update storage procedure
DELIMITER //

CREATE EVENT IF NOT EXISTS event_update_hourly_surge_stats
ON SCHEDULE EVERY 1 HOUR
STARTS (TIMESTAMP(CURRENT_DATE) + INTERVAL 1 HOUR)
DO
    CALL update_hourly_surge_stats();

//

DELIMITER ;

-- Create TABLE for daily surge
CREATE TABLE IF NOT EXISTS daily_surge_pokemon_stats (
    hour TINYINT,
    total_iv100 MEDIUMINT,
    total_iv0 MEDIUMINT,
    total_top1_little MEDIUMINT,
    total_top1_great MEDIUMINT,
    total_top1_ultra MEDIUMINT,
    total_shiny MEDIUMINT,
    PRIMARY KEY (hour)
) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;    
-- Procedure for daily surge
DELIMITER //

CREATE PROCEDURE update_daily_surge_stats()
BEGIN

    CREATE TEMPORARY TABLE IF NOT EXISTS temp_daily_surge AS
    SELECT
        HOUR(hour) AS hour_of_day,
        SUM(total_iv100) AS sum_total_iv100,
        SUM(total_iv0) AS sum_total_iv0,
        SUM(total_top1_little) AS sum_total_top1_little,
        SUM(total_top1_great) AS sum_total_top1_great,
        SUM(total_top1_ultra) AS sum_total_top1_ultra,
        SUM(total_shiny) AS sum_total_shiny
    FROM hourly_surge_storage_pokemon_stats
    WHERE hour >= NOW() - INTERVAL 1 DAY AND hour < NOW()
    GROUP BY HOUR(hour);

    TRUNCATE TABLE daily_surge_pokemon_stats;

    INSERT INTO daily_surge_pokemon_stats (hour, total_iv100, total_iv0, total_top1_little, total_top1_great, total_top1_ultra, total_shiny)
    SELECT 
        hour_of_day,
        sum_total_iv100,
        sum_total_iv0,
        sum_total_top1_little,
        sum_total_top1_great,
        sum_total_top1_ultra,
        sum_total_shiny
    FROM temp_daily_surge;

    DROP TEMPORARY TABLE IF EXISTS temp_daily_surge;
END //

DELIMITER ;

-- Event for update_daily_surge_stats
DELIMITER //

CREATE EVENT IF NOT EXISTS event_update_daily_surge_stats
ON SCHEDULE EVERY 1 DAY
STARTS ADDDATE(CURDATE(), INTERVAL 1 DAY)
DO
CALL update_daily_surge_stats();

//

DELIMITER ;

-- Create Table for weekly surge
CREATE TABLE IF NOT EXISTS weekly_surge_pokemon_stats (
    hour TINYINT,
    total_iv100 INTEGER,
    total_iv0 INTEGER,
    total_top1_little INTEGER,
    total_top1_great INTEGER,
    total_top1_ultra INTEGER,
    total_shiny INTEGER,
    PRIMARY KEY (hour)
) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;

DELIMITER //

-- Create PROCEDURE for weekly surge update
CREATE PROCEDURE update_weekly_surge_stats()
BEGIN

    CREATE TEMPORARY TABLE IF NOT EXISTS temp_weekly_surge AS
    SELECT
        HOUR(hour) AS hour_of_day,
        SUM(total_iv100) AS sum_total_iv100,
        SUM(total_iv0) AS sum_total_iv0,
        SUM(total_top1_little) AS sum_total_top1_little,
        SUM(total_top1_great) AS sum_total_top1_great,
        SUM(total_top1_ultra) AS sum_total_top1_ultra,
        SUM(total_shiny) AS sum_total_shiny
    FROM hourly_surge_storage_pokemon_stats
    WHERE hour >= NOW() - INTERVAL 1 WEEK AND hour < NOW()
    GROUP BY HOUR(hour);

    TRUNCATE TABLE weekly_surge_pokemon_stats;

    INSERT INTO weekly_surge_pokemon_stats (hour, total_iv100, total_iv0, total_top1_little, total_top1_great, total_top1_ultra, total_shiny)
    SELECT 
        hour_of_day,
        sum_total_iv100,
        sum_total_iv0,
        sum_total_top1_little,
        sum_total_top1_great,
        sum_total_top1_ultra,
        sum_total_shiny
    FROM temp_weekly_surge;

    DROP TEMPORARY TABLE IF EXISTS temp_weekly_surge;
END //

DELIMITER ;

-- Create EVENT for weekly surge Procedure update
DELIMITER //

CREATE EVENT IF NOT EXISTS event_update_weekly_surge_stats
ON SCHEDULE EVERY 1 WEEK
STARTS ADDDATE(ADDDATE(CURDATE(), INTERVAL 7 DAY), INTERVAL '1:05' HOUR_MINUTE)
DO
CALL update_weekly_surge_stats();

//

DELIMITER ;

-- Create Table for Monthly surge
CREATE TABLE IF NOT EXISTS monthly_surge_pokemon_stats (
    hour TINYINT,
    total_iv100 BIGINT,
    total_iv0 BIGINT,
    total_top1_little BIGINT,
    total_top1_great BIGINT,
    total_top1_ultra BIGINT,
    total_shiny BIGINT,
    PRIMARY KEY (hour)
) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;

-- Create Procedure for monthly surge update
DELIMITER //

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
    FROM temp_weekly_surge;

    DROP TEMPORARY TABLE IF EXISTS temp_monthly_surge;
END //

DELIMITER ;

-- Create EVENT for monthly surge Procedure update
DELIMITER //

CREATE EVENT IF NOT EXISTS event_update_monthly_surge_stats
ON SCHEDULE EVERY 1 MONTH
STARTS ADDDATE(ADDDATE(CURDATE(), INTERVAL 1 MONTH), INTERVAL '1:10' HOUR_MINUTE)
DO
CALL update_monthly_surge_stats();

//

DELIMITER ;