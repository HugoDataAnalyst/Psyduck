-- Correct day DATE in Quests (its the same day Data)

-- Add logic for double scan of quests.

ALTER TABLE storage_quest_total_stats
ADD COLUMN scanned INT DEFAULT 0;

ALTER TABLE storage_quest_grouped_stats
ADD COLUMN scanned INT DEFAULT 0;


ALTER TABLE daily_quest_total_stats
ADD COLUMN scanned INT DEFAULT 0;

ALTER TABLE quest_total_stats
ADD COLUMN scanned INT DEFAULT 0;

ALTER TABLE daily_quest_grouped_stats
ADD COLUMN scanned INT DEFAULT 0;

ALTER TABLE weekly_quest_grouped_stats
ADD COLUMN scanned INT DEFAULT 0;

ALTER TABLE monthly_quest_grouped_stats
ADD COLUMN scanned INT DEFAULT 0;

-- Correct Storage Keys for grouped Quests
ALTER TABLE storage_quest_total_stats
DROP PRIMARY KEY,
ADD PRIMARY KEY (day, area_name, scanned);

ALTER TABLE storage_quest_grouped_stats
DROP PRIMARY KEY,
ADD PRIMARY KEY (day, area_name, ar_type, normal_type, reward_ar_type, reward_normal_type, reward_ar_item_id, reward_ar_item_amount, reward_normal_item_id, reward_normal_item_amount, reward_ar_poke_id, reward_ar_poke_form, reward_normal_poke_id, reward_normal_poke_form, scanned);

ALTER TABLE daily_quest_grouped_stats
DROP PRIMARY KEY,
ADD PRIMARY KEY (day, area_name, ar_type, normal_type, reward_ar_type, reward_normal_type, reward_ar_item_id, reward_ar_item_amount, reward_normal_item_id, reward_normal_item_amount, reward_ar_poke_id, reward_ar_poke_form, reward_normal_poke_id, reward_normal_poke_form, scanned);

ALTER TABLE weekly_quest_grouped_stats
DROP PRIMARY KEY,
ADD PRIMARY KEY (day, area_name, ar_type, normal_type, reward_ar_type, reward_normal_type, reward_ar_item_id, reward_ar_item_amount, reward_normal_item_id, reward_normal_item_amount, reward_ar_poke_id, reward_ar_poke_form, reward_normal_poke_id, reward_normal_poke_form, scanned);

ALTER TABLE monthly_quest_grouped_stats
DROP PRIMARY KEY,
ADD PRIMARY KEY (day, area_name, ar_type, normal_type, reward_ar_type, reward_normal_type, reward_ar_item_id, reward_ar_item_amount, reward_normal_item_id, reward_normal_item_amount, reward_ar_poke_id, reward_ar_poke_form, reward_normal_poke_id, reward_normal_poke_form, scanned);


ALTER TABLE daily_quest_total_stats
DROP PRIMARY KEY,
ADD PRIMARY KEY (day, area_name, scanned);


ALTER TABLE quest_total_stats
DROP PRIMARY KEY,
ADD PRIMARY KEY (area_name, scanned);

-- Update Quest Storage Procedure

DROP PROCEDURE IF EXISTS store_quest_total_stats;
CREATE PROCEDURE store_quest_total_stats()
BEGIN
    DROP TEMPORARY TABLE IF EXISTS temp_store_total_quest_sightings;
    CREATE TEMPORARY TABLE temp_store_total_quest_sightings AS
    SELECT qs.*,
           CASE
               WHEN HOUR(qs.inserted_at) >= 21 OR HOUR(qs.inserted_at) < 5 THEN 1
               WHEN HOUR(qs.inserted_at) >= 7 AND HOUR(qs.inserted_at) < 14 THEN 2
               ELSE 0
           END AS scanned
    FROM quest_sightings qs
    WHERE qs.inserted_at >= NOW() - INTERVAL 1 DAY AND qs.inserted_at < NOW();

    INSERT INTO storage_quest_total_stats(day, area_name, total_stops, ar, normal, scanned)
    SELECT
        DATE(NOW()) as day,
        tqs.area_name,
        (
            SELECT tp.total_stops
            FROM total_pokestops tp
            WHERE tp.area_name = tqs.area_name
            AND DATE(tp.day) = DATE(NOW() - INTERVAL 1 DAY)
            LIMIT 1
        ) AS total_stops,
        COUNT(tqs.ar_type) AS ar,
        COUNT(tqs.normal_type) AS normal,
        tqs.scanned
    FROM temp_store_total_quest_sightings tqs
    GROUP BY tqs.area_name, tqs.scanned
    ORDER BY tqs.area_name ASC, tqs.scanned ASC;

    DROP TEMPORARY TABLE IF EXISTS temp_store_total_quest_sightings;
END;


DROP PROCEDURE IF EXISTS store_quest_grouped_stats;
CREATE PROCEDURE store_quest_grouped_stats()
BEGIN
    DROP TEMPORARY TABLE IF EXISTS temp_store_grouped_quest_sightings;
    CREATE TEMPORARY TABLE temp_store_grouped_quest_sightings AS
    SELECT *,
           CASE
               WHEN HOUR(inserted_at) >= 22 OR HOUR(inserted_at) < 5 THEN 1
               WHEN HOUR(inserted_at) >= 7 AND HOUR(inserted_at) < 14 THEN 2
               ELSE 0
           END AS scanned
    FROM quest_sightings
    WHERE inserted_at >= NOW() - INTERVAL 1 DAY AND inserted_at < NOW();

    INSERT INTO storage_quest_grouped_stats (day, area_name, ar_type, normal_type, reward_ar_type, reward_normal_type, reward_ar_item_id, reward_ar_item_amount, reward_normal_item_id, reward_normal_item_amount, reward_ar_poke_id, reward_ar_poke_form, reward_normal_poke_id, reward_normal_poke_form, total, scanned)
    SELECT
        DATE(NOW()) as day,
        area_name,
        COALESCE(ar_type,0) AS ar_type,
        COALESCE(normal_type,0) AS normal_type,
        COALESCE(reward_ar_type,0) AS reward_ar_type,
        COALESCE(reward_normal_type,0) AS reward_normal_type,
        COALESCE(reward_ar_item_id,0) AS reward_ar_item_id,
        COALESCE(reward_ar_item_amount,0) AS reward_ar_item_amount,
        COALESCE(reward_normal_item_id,0) AS reward_normal_item_id,
        COALESCE(reward_normal_item_amount,0) AS reward_normal_item_amount,
        COALESCE(reward_ar_poke_id,0) AS reward_ar_poke_id,
        COALESCE(reward_ar_poke_form,0) AS reward_ar_poke_form,
        COALESCE(reward_normal_poke_id,0) AS reward_normal_poke_id,
        COALESCE(reward_normal_poke_form,0) AS reward_normal_poke_form,
        COUNT(*) AS total,
        COALESCE(scanned,0) AS scanned
    FROM temp_store_grouped_quest_sightings
    GROUP BY ar_type, reward_ar_type, normal_type, reward_normal_type, reward_ar_item_id, reward_ar_item_amount, reward_normal_item_id, reward_normal_item_amount, reward_ar_poke_id, reward_ar_poke_form, reward_normal_poke_id, reward_normal_poke_form, area_name, scanned;

    DROP TEMPORARY TABLE IF EXISTS temp_store_grouped_quest_sightings;
END;

-- Quest Total Updates

DROP PROCEDURE IF EXISTS update_daily_quest_total_stats;
CREATE PROCEDURE update_daily_quest_total_stats()
BEGIN
    TRUNCATE TABLE daily_quest_total_stats;

    INSERT INTO daily_quest_total_stats (day, area_name, total_stops, ar, normal, scanned)
    SELECT
        DATE(NOW()) AS day,
        area_name,
        total_stops,
        ar,
        normal,
        scanned
    FROM storage_quest_total_stats
    WHERE day = CURDATE();
END;

DROP PROCEDURE IF EXISTS update_total_quest_total_stats;
CREATE PROCEDURE update_total_quest_total_stats()
BEGIN
    INSERT INTO quest_total_stats (area_name, ar, normal, scanned)
    SELECT
        d.area_name,
        d.ar,
        d.normal,
        d.scanned
    FROM storage_quest_total_stats d
    WHERE d.day = CURDATE()
    ON DUPLICATE KEY UPDATE
        ar = quest_total_stats.ar + d.ar,
        normal = quest_total_stats.normal + d.normal;
END;

-- Quest grouped updates

DROP PROCEDURE IF EXISTS update_daily_quest_grouped_stats;
CREATE PROCEDURE update_daily_quest_grouped_stats()
BEGIN
    TRUNCATE TABLE daily_quest_grouped_stats;

    INSERT INTO daily_quest_grouped_stats (day, area_name, ar_type, normal_type, reward_ar_type, reward_normal_type, reward_ar_item_id, reward_ar_item_amount, reward_normal_item_id, reward_normal_item_amount, reward_ar_poke_id, reward_ar_poke_form, reward_normal_poke_id, reward_normal_poke_form, total, scanned)
    SELECT
        DATE(NOW()) as day,
        area_name,
        ar_type,
        normal_type,
        reward_ar_type,
        reward_normal_type,
        reward_ar_item_id,
        reward_ar_item_amount,
        reward_normal_item_id,
        reward_normal_item_amount,
        reward_ar_poke_id,
        reward_ar_poke_form,
        reward_normal_poke_id,
        reward_normal_poke_form,
        total,
        scanned
    FROM storage_quest_grouped_stats
    WHERE day = CURDATE()
    GROUP BY ar_type, reward_ar_type, normal_type, reward_normal_type, reward_ar_item_id, reward_ar_item_amount, reward_normal_item_id, reward_normal_item_amount, reward_ar_poke_id, reward_ar_poke_form, reward_normal_poke_id, reward_normal_poke_form, area_name, total, scanned
    ORDER BY area_name, ar_type, normal_type, reward_ar_item_id, reward_normal_item_id, reward_ar_poke_id, reward_normal_poke_id ASC;
END;

DROP PROCEDURE IF EXISTS update_weekly_quest_grouped_stats;
CREATE PROCEDURE update_weekly_quest_grouped_stats()
BEGIN
    TRUNCATE TABLE weekly_quest_grouped_stats;

    INSERT INTO weekly_quest_grouped_stats (day, area_name, ar_type, normal_type, reward_ar_type, reward_normal_type, reward_ar_item_id, reward_ar_item_amount, reward_normal_item_id, reward_normal_item_amount, reward_ar_poke_id, reward_ar_poke_form, reward_normal_poke_id, reward_normal_poke_form, total, scanned)
    SELECT
        DATE_SUB(CURDATE(), INTERVAL DAYOFWEEK(CURDATE()) - 1 DAY) as day,
        area_name,
        ar_type,
        normal_type,
        reward_ar_type,
        reward_normal_type,
        reward_ar_item_id,
        reward_ar_item_amount,
        reward_normal_item_id,
        reward_normal_item_amount,
        reward_ar_poke_id,
        reward_ar_poke_form,
        reward_normal_poke_id,
        reward_normal_poke_form,
        total,
        scanned
    FROM storage_quest_grouped_stats
    WHERE day >= DATE_SUB(CURDATE(), INTERVAL DAYOFWEEK(CURDATE()) + 6 DAY) AND day < DATE_SUB(CURDATE(), INTERVAL DAYOFWEEK(CURDATE()) - 1 DAY)
    GROUP BY ar_type, reward_ar_type, normal_type, reward_normal_type, reward_ar_item_id, reward_ar_item_amount, reward_normal_item_id, reward_normal_item_amount, reward_ar_poke_id, reward_ar_poke_form, reward_normal_poke_id, reward_normal_poke_form, area_name, total, scanned
    ORDER BY area_name, ar_type, normal_type, reward_ar_item_id, reward_normal_item_id, reward_ar_poke_id, reward_normal_poke_id ASC;
END;

DROP PROCEDURE IF EXISTS update_monthly_quest_grouped_stats;
CREATE PROCEDURE update_monthly_quest_grouped_stats()
BEGIN
    TRUNCATE TABLE monthly_quest_grouped_stats;

    INSERT INTO monthly_quest_grouped_stats (day, area_name, ar_type, normal_type, reward_ar_type, reward_normal_type, reward_ar_item_id, reward_ar_item_amount, reward_normal_item_id, reward_normal_item_amount, reward_ar_poke_id, reward_ar_poke_form, reward_normal_poke_id, reward_normal_poke_form, total, scanned)
    SELECT
        LAST_DAY(CURDATE() - INTERVAL 1 MONTH) as day,
        area_name,
        ar_type,
        normal_type,
        reward_ar_type,
        reward_normal_type,
        reward_ar_item_id,
        reward_ar_item_amount,
        reward_normal_item_id,
        reward_normal_item_amount,
        reward_ar_poke_id,
        reward_ar_poke_form,
        reward_normal_poke_id,
        reward_normal_poke_form,
        total,
        scanned
    FROM storage_quest_grouped_stats
    WHERE day >= CURDATE() - INTERVAL 1 MONTH
    GROUP BY ar_type, reward_ar_type, normal_type, reward_normal_type, reward_ar_item_id, reward_ar_item_amount, reward_normal_item_id, reward_normal_item_amount, reward_ar_poke_id, reward_ar_poke_form, reward_normal_poke_id, reward_normal_poke_form, area_name, total, scanned
    ORDER BY area_name, ar_type, normal_type, reward_ar_item_id, reward_normal_item_id, reward_ar_poke_id, reward_normal_poke_id ASC;
END;

-- Invasion
ALTER TABLE storage_invasion_grouped_stats
DROP PRIMARY KEY,
ADD PRIMARY KEY (day, area_name, grunt);

ALTER TABLE daily_invasion_grouped_stats
DROP PRIMARY KEY,
ADD PRIMARY KEY (day, area_name, grunt);

ALTER TABLE weekly_invasion_grouped_stats
DROP PRIMARY KEY,
ADD PRIMARY KEY (day, area_name, grunt);

ALTER TABLE monthly_invasion_grouped_stats
DROP PRIMARY KEY,
ADD PRIMARY KEY (day, area_name, grunt);

DROP PROCEDURE IF EXISTS update_daily_invasion_grouped_stats;
CREATE PROCEDURE update_daily_invasion_grouped_stats()
BEGIN
    TRUNCATE TABLE daily_invasion_grouped_stats;

    INSERT INTO daily_invasion_grouped_stats (day, area_name, display_type, grunt, total_grunts)
    SELECT
        CURDATE() - INTERVAL 1 DAY as day,
        area_name,
        display_type,
        grunt,
        total_grunts
    FROM storage_invasion_grouped_stats
    WHERE day = CURDATE() - INTERVAL 1 DAY
    GROUP BY display_type, grunt, area_name, total_grunts
    ORDER BY area_name, display_type, grunt ASC;
END;

-- Raid Procedure
DROP PROCEDURE IF EXISTS update_daily_raid_grouped_stats;
CREATE PROCEDURE update_daily_raid_grouped_stats()
BEGIN
    TRUNCATE TABLE daily_raid_grouped_stats;

    INSERT INTO daily_raid_grouped_stats (day, area_name, level, pokemon_id, form, costume, ex_raid_eligible, is_exclusive, total)
    SELECT
        CURDATE() - INTERVAL 1 DAY as day,
        area_name,
        level,
        pokemon_id,
        form,
        costume,
        ex_raid_eligible,
        is_exclusive,
        total
    FROM storage_raid_grouped_stats
    WHERE day = CURDATE() - INTERVAL 1 DAY
    GROUP BY level, pokemon_id, form, costume, ex_raid_eligible, is_exclusive, area_name, total
    ORDER BY area_name, level, pokemon_id ASC;
END;

-- Cleaning Section Quests

DROP PROCEDURE IF EXISTS delete_quest_sightings_batches;
DROP EVENT IF EXISTS clean_quest_sightings;

CREATE PROCEDURE delete_quest_sightings_batches()
BEGIN
  TRUNCATE TABLE quest_sightings;
END;

CREATE EVENT IF NOT EXISTS clean_quest_sightings
ON SCHEDULE EVERY 1 DAY
STARTS ADDDATE(ADDDATE(CURDATE(), INTERVAL 1 DAY), INTERVAL '16:00:00' HOUR_SECOND)
DO
CALL delete_quest_sightings_batches();
