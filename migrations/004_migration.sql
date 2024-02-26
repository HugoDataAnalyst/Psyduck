-- UPDATE QUEST TABLE WITH INDEXES FOR ALL GROUP BY CLAUSES
ALTER TABLE quest_sightings
ADD INDEX idx_grouped (ar_type, reward_ar_type, normal_type, reward_normal_type, reward_ar_item_id, reward_ar_item_amount, reward_normal_item_id, reward_normal_item_amount, reward_ar_poke_id, reward_ar_poke_form, reward_normal_poke_id, reward_normal_poke_form, area_name);

-- UPDATER RAID TABLE WITH INDEXES FOR ALL GROUP BY CLAUSES
ALTER TABLE raid_sightings
ADD INDEX idx_grouped (level, pokemon_id, form, costume, ex_raid_eligible, is_exclusive, area_name);

-- UPDATER INVASION TABLE WITH INDEXES FOR ALL GROUP BY CLAUSES
ALTER TABLE invasion_sightings
ADD INDEX idx_grouped (display_type, grunt, area_name),
ADD INDEX idx_inserted_at (inserted_at);

-- CREATE TABLE for Total_Pokestops from each Area
CREATE TABLE IF NOT EXISTS total_pokestops (
	day DATE,
	area_name VARCHAR(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci,
	total_stops INTEGER,
	PRIMARY KEY (day, area_name)
) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;

-- CREATE TABLES for totals/grouped data for quests/raids/invasions
-- Storage for weekly/monthly Grouped

-- Quest
CREATE TABLE IF NOT EXISTS storage_quest_grouped_stats (
	day DATE,
	area_name VARCHAR(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci,
	ar_type SMALLINT,
	normal_type SMALLINT,
	reward_ar_type SMALLINT,
	reward_normal_type SMALLINT,
	reward_ar_item_id SMALLINT,
	reward_ar_item_amount SMALLINT,
	reward_normal_item_id SMALLINT,
	reward_normal_item_amount SMALLINT,
	reward_ar_poke_id SMALLINT,
	reward_ar_poke_form VARCHAR(15) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci,
	reward_normal_poke_id SMALLINT,
	reward_normal_poke_form VARCHAR(15) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci,
	total MEDIUMINT,
	PRIMARY KEY (day, area_name)
) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;

-- Raid
CREATE TABLE iF NOT EXISTS storage_raid_grouped_stats (
	day DATE,
	area_name VARCHAR(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci,
	level TINYINT,
	pokemon_id SMALLINT,
	form VARCHAR(15) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci,
	costume VARCHAR(50) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci,
	ex_raid_eligible TINYINT,
	is_exclusive TINYINT,
	total INTEGER,
	PRIMARY KEY (day, area_name)
) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;

-- Invasion
CREATE TABLE IF NOT EXISTS storage_invasion_grouped_stats (
	day DATE,
	area_name VARCHAR(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci,
	display_type TINYINT,
	grunt TINYINT,
	total_grunts MEDIUMINT,
	PRIMARY KEY (day, area_name)
) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;

-- Storage for Totals
-- Quest
CREATE TABLE IF NOT EXISTS storage_quest_total_stats (
	day DATE,
	area_name VARCHAR(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci,
	total_stops INTEGER,
	ar INTEGER,
	normal INTEGER,
	PRIMARY KEY (day, area_name),
	INDEX idx_area_name (area_name)
) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;

-- Raid
CREATE TABLE IF NOT EXISTS storage_raid_total_stats (
	day DATE,
	area_name VARCHAR(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci,
	total INTEGER,
	total_ex_raid INTEGER,
	total_exclusive INTEGER,
	PRIMARY KEY (day, area_name),
	INDEX idx_area_name (area_name)
) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;

-- Invasion
CREATE TABLE IF NOT EXISTS storage_invasion_total_stats (
	day DATE,
	area_name VARCHAR(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci,
	total_grunts INTEGER,
	total_confirmed INTEGER,
	total_unconfirmed INTEGER,
	PRIMARY	KEY (day, area_name),
	INDEX idx_area_name (area_name)
) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;

-- Grouped 
-- Quest Daily Grouped
CREATE TABLE IF NOT EXISTS daily_quest_grouped_stats(
	day DATE,
	area_name VARCHAR(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci,
	ar_type SMALLINT,
	normal_type SMALLINT,
	reward_ar_type SMALLINT,
	reward_normal_type SMALLINT,
	reward_ar_item_id SMALLINT,
	reward_ar_item_amount SMALLINT,
	reward_normal_item_id SMALLINT,
	reward_normal_item_amount SMALLINT,
	reward_ar_poke_id SMALLINT,
	reward_ar_poke_form VARCHAR(15) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci,
	reward_normal_poke_id SMALLINT,
	reward_normal_poke_form VARCHAR(15) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci,
	total MEDIUMINT,
	PRIMARY KEY (day, area_name)
) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;

-- Quest Weekly Grouped
CREATE TABLE IF NOT EXISTS weekly_quest_grouped_stats(
	day DATE,
	area_name VARCHAR(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci,
	ar_type SMALLINT,
	normal_type SMALLINT,
	reward_ar_type SMALLINT,
	reward_normal_type SMALLINT,
	reward_ar_item_id SMALLINT,
	reward_ar_item_amount SMALLINT,
	reward_normal_item_id SMALLINT,
	reward_normal_item_amount SMALLINT,
	reward_ar_poke_id SMALLINT,
	reward_ar_poke_form VARCHAR(15) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci,
	reward_normal_poke_id SMALLINT,
	reward_normal_poke_form VARCHAR(15) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci,
	total MEDIUMINT,
	PRIMARY KEY (day, area_name)
) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;

-- Quest Monthly Grouped
CREATE TABLE IF NOT EXISTS monthly_quest_grouped_stats(
	day DATE,
	area_name VARCHAR(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci,
	ar_type SMALLINT,
	normal_type SMALLINT,
	reward_ar_type SMALLINT,
	reward_normal_type SMALLINT,
	reward_ar_item_id SMALLINT,
	reward_ar_item_amount SMALLINT,
	reward_normal_item_id SMALLINT,
	reward_normal_item_amount SMALLINT,
	reward_ar_poke_id SMALLINT,
	reward_ar_poke_form VARCHAR(15) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci,
	reward_normal_poke_id SMALLINT,
	reward_normal_poke_form VARCHAR(15) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci,
	total MEDIUMINT,
	PRIMARY KEY (day, area_name)
) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;

-- Raid Daily Grouped
CREATE TABLE IF NOT EXISTS daily_raid_grouped_stats(
	day DATE,
	area_name VARCHAR(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci,
	level TINYINT,
	pokemon_id SMALLINT,
	form VARCHAR(15) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci,
	costume VARCHAR(50) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci,
	ex_raid_eligible TINYINT,
	is_exclusive TINYINT,
	total INTEGER,
	PRIMARY KEY (day, area_name)
) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;

-- Raid Weekly Grouped
CREATE TABLE IF NOT EXISTS weekly_raid_grouped_stats(
	day DATE,
	area_name VARCHAR(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci,
	level TINYINT,
	pokemon_id SMALLINT,
	form VARCHAR(15) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci,
	costume VARCHAR(50) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci,
	ex_raid_eligible TINYINT,
	is_exclusive TINYINT,
	total INTEGER,
	PRIMARY KEY (day, area_name)
) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;

-- Raid Monthly Grouped
CREATE TABLE IF NOT EXISTS monthly_raid_grouped_stats(
	day DATE,
	area_name VARCHAR(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci,
	level TINYINT,
	pokemon_id SMALLINT,
	form VARCHAR(15) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci,
	costume VARCHAR(50) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci,
	ex_raid_eligible TINYINT,
	is_exclusive TINYINT,
	total INTEGER,
	PRIMARY KEY (day, area_name)
) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;

-- Invasion Daily Grouped
CREATE TABLE IF NOT EXISTS daily_invasion_grouped_stats(
	day DATE,
	area_name VARCHAR(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci,
	display_type TINYINT,
	grunt TINYINT,
	total_grunts MEDIUMINT,
	PRIMARY KEY (day, area_name)
) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;

-- Invasion Weekly Grouped
CREATE TABLE IF NOT EXISTS weekly_invasion_grouped_stats(
	day DATE,
	area_name VARCHAR(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci,
	display_type TINYINT,
	grunt TINYINT,
	total_grunts MEDIUMINT,
	PRIMARY KEY (day, area_name)
) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;

-- Invasion Monthly Grouped
CREATE TABLE IF NOT EXISTS monthly_invasion_grouped_stats(
	day DATE,
	area_name VARCHAR(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci,
	display_type TINYINT,
	grunt TINYINT,
	total_grunts MEDIUMINT,
	PRIMARY KEY (day, area_name)
) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;

-- Totals - REVISIT THIS SECTION
-- Quest Daily Total
CREATE TABLE IF NOT EXISTS daily_quest_total_stats (
	day DATE,
	area_name VARCHAR(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci,
	total_stops INTEGER,
	ar INTEGER,
	normal INTEGER,
	PRIMARY KEY (area_name),
	INDEX idx_area_name (area_name)
) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;

-- Quest Total of Totals
CREATE TABLE IF NOT EXISTS quest_total_stats (
	area_name VARCHAR(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci,
	ar BIGINT,
	normal BIGINT,
	PRIMARY KEY (area_name),
	INDEX idx_area_name (area_name)
) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;

-- Raid Hourly Total
CREATE TABLE IF NOT EXISTS hourly_raid_total_stats (
	area_name VARCHAR(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci,
	total MEDIUMINT,
	total_ex_raid MEDIUMINT,
	total_exclusive MEDIUMINT,
	PRIMARY KEY (area_name),
	INDEX idx_area_name (area_name)
) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;

-- Raid Daily Total
CREATE TABLE IF NOT EXISTS daily_raid_total_stats
	day DATE,
	area_name VARCHAR(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci,
	total INTEGER,
	total_ex_raid INTEGER,
	total_exclusive INTEGER,
	PRIMARY KEY (area_name),
	INDEX idx_area_name (area_name)
) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;

-- Raid Total of Totals
CREATE TABLE IF NOT EXISTS raid_total_stats (
	area_name VARCHAR(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci,
	total BIGINT,
	total_ex_raid BIGINT,
	total_exclusive BIGINT,
	PRIMARY KEY (area_name),
	INDEX idx_area_name (area_name)
) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;

-- Invasion Hourly Total
CREATE TABLE IF NOT EXISTS hourly_invasion_total_stats (
	area_name VARCHAR(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci,
	total_grunts MEDIUMINT,
	total_confirmed MEDIUMINT,
	total_unconfirmed MEDIUMINT,
	PRIMARY	KEY (area_name),
	INDEX idx_area_name (area_name)
) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;

-- Invasion Daily Total
CREATE TABLE IF NOT EXISTS daily_invasion_total_stats (
	day DATE,
	area_name VARCHAR(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci,
	total_grunts INTEGER,
	total_confirmed INTEGER,
	total_unconfirmed INTEGER,
	PRIMARY	KEY (area_name),
	INDEX idx_area_name (area_name)
) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;

-- Invasion Total of Totals
CREATE TABLE IF NOT EXISTS invasion_total_stats(
	area_name VARCHAR(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci,
	total_grunts BIGINT,
	total_confirmed BIGINT,
	total_unconfirmed BIGINT,
	PRIMARY	KEY (area_name),
	INDEX idx_area_name (area_name)
) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;

-- CREATE DATA STORAGE PROCEDURES
-- GROUPED
-- QUEST
CREATE PROCEDURE store_quest_grouped_stats()
BEGIN
	DROP TEMPORARY TABLE IF EXISTS temp_store_grouped_quest_sightings;
	CREATE TEMPORARY TABLE temp_store_grouped_quest_sightings AS
	SELECT *
	FROM quest_sightings
	WHERE inserted_at >= CURDATE() - INTERVAL 1 DAY AND inserted_at < CURDATE();

    INSERT INTO storage_quest_grouped_stats (day, area_name, ar_type, normal_type, reward_ar_type, reward_normal_type, reward_ar_item_id, reward_ar_item_amount, reward_normal_item_id, reward_normal_item_amount, reward_ar_poke_id, reward_ar_poke_form, reward_normal_poke_id, reward_normal_poke_form, total)
    SELECT
        CURDATE() - INTERVAL 1 DAY as day,
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
		COUNT(*) AS total
    FROM temp_store_grouped_quest_sightings
    GROUP BY ar_type, reward_ar_type, normal_type, reward_normal_type, reward_ar_item_id, reward_ar_item_amount, reward_normal_item_id, reward_normal_item_amount, reward_ar_poke_id, reward_ar_poke_form, reward_normal_poke_id, reward_normal_poke_form, area_name;

    DROP TEMPORARY TABLE IF EXISTS temp_store_grouped_quest_sightings;
END;

-- RAID
CREATE PROCEDURE store_raid_grouped_stats()
BEGIN
	DROP TEMPORARY TABLE IF EXISTS temp_store_grouped_raid_sightings;
	CREATE TEMPORARY TABLE temp_store_grouped_raid_sightings AS
	SELECT *
	FROM raid_sightings
	WHERE inserted_at >= CURDATE() - INTERVAL 1 DAY AND inserted_at < CURDATE();

    INSERT INTO storage_raid_grouped_stats (day, area_name, level, pokemon_id, form, costume, ex_raid_eligible, is_exclusive, total)
    SELECT
        CURDATE() - INTERVAL 1 DAY as day,
        area_name,
        level,
        pokemon_id,
        form,
        costume,
        ex_raid_eligible,
        is_exclusive,
		COUNT(*) AS total
    FROM temp_store_grouped_raid_sightings
    GROUP BY level, pokemon_id, form, costume, ex_raid_eligible, is_exclusive, area_name;

    DROP TEMPORARY TABLE IF EXISTS temp_store_grouped_raid_sightings;
END;

-- INVASION
CREATE PROCEDURE store_invasion_grouped_stats()
BEGIN
	DROP TEMPORARY TABLE IF EXISTS temp_store_grouped_invasion_sightings;
	CREATE TEMPORARY TABLE temp_store_grouped_invasion_sightings AS
	SELECT *
	FROM invasion_sightings
	WHERE inserted_at >= CURDATE() - INTERVAL 1 DAY AND inserted_at < CURDATE();

    INSERT INTO storage_invasion_grouped_stats (day, area_name, display_type, grunt, total_grunts)
    SELECT
        CURDATE() - INTERVAL 1 DAY as day,
        area_name,
        display_type,
        grunt,
        SUM(CASE WHEN confirmed = 0 THEN 1 ELSE 0 END) AS total_grunts
    FROM temp_store_grouped_invasion_sightings
    GROUP BY display_type, grunt, area_name;

    DROP TEMPORARY TABLE IF EXISTS temp_store_grouped_invasion_sightings;
END;

-- TOTALS
-- QUEST
CREATE PROCEDURE store_quest_total_stats()
BEGIN
    DROP TEMPORARY TABLE IF EXISTS temp_store_total_quest_sightings;
    CREATE TEMPORARY TABLE temp_store_total_quest_sightings AS
    SELECT qs.*
    FROM quest_sightings qs
    WHERE qs.inserted_at >= CURDATE() - INTERVAL 1 DAY AND qs.inserted_at < CURDATE();

    INSERT INTO storage_quest_total_stats (day, area_name, total_stops, ar, normal)
    SELECT
        CURDATE() - INTERVAL 1 DAY as day,
        tqs.area_name,
        COALESCE(tp.total_stops, 0) AS total_stops,
        COUNT(tqs.ar_type) AS ar,
        COUNT(tqs.normal_type) AS normal
    FROM temp_store_total_quest_sightings tqs
    LEFT JOIN total_pokestops tp ON tqs.area_name = tp.area_name AND tp.day = CURDATE() - INTERVAL 1 DAY
    GROUP BY tqs.area_name;

    DROP TEMPORARY TABLE IF EXISTS temp_store_total_quest_sightings;
END;

-- RAID
CREATE PROCEDURE store_raid_total_stats()
BEGIN
	DROP TEMPORARY TABLE IF EXISTS temp_store_total_raid_sightings;
	CREATE TEMPORARY TABLE temp_store_total_raid_sightings AS
	SELECT *
	FROM raid_sightings
	WHERE inserted_at >= CURDATE() - INTERVAL 1 DAY AND inserted_at < CURDATE();

    INSERT INTO storage_raid_total_stats (day, area_name, total, total_ex_raid, total_exclusive)
    SELECT
        CURDATE() - INTERVAL 1 DAY as day,
        area_name,
        COUNT(*) AS total,
        SUM(CASE WHEN ex_raid_eligible = 1 THEN 1 ELSE 0 END) AS total_ex_raid,
        SUM(CASE WHEN is_exclusive = 1 THEN 1 ELSE 0 END) AS total_exclusive
    FROM temp_store_total_raid_sightings
    GROUP BY area_name;

    DROP TEMPORARY TABLE IF EXISTS temp_store_total_raid_sightings;
END;

-- INVASION
CREATE PROCEDURE store_invasion_total_stats()
BEGIN
	DROP TEMPORARY TABLE IF EXISTS temp_store_total_invasion_sightings;
	CREATE TEMPORARY TABLE temp_store_total_invasion_sightings AS
	SELECT *
	FROM invasion_sightings
	WHERE inserted_at >= CURDATE() - INTERVAL 1 DAY AND inserted_at < CURDATE();

    INSERT INTO storage_invasion_total_stats (day, area_name, total_grunts, total_confirmed, total_unconfirmed)
    SELECT
        CURDATE() - INTERVAL 1 DAY as day,
        area_name,
        SUM(CASE WHEN confirmed = 0 THEN 1 ELSE 0 END) AS total_grunts,
        SUM(CASE WHEN confirmed = 1 THEN 1 ELSE 0 END) AS total_confirmed,
        SUM(CASE WHEN confirmed = 0 THEN 1 ELSE 0 END) - SUM(CASE WHEN confirmed = 1 THEN 1 ELSE 0 END) AS total_unconfirmed
    FROM temp_store_total_invasion_sightings
    GROUP BY area_name;

    DROP TEMPORARY TABLE IF EXISTS temp_store_total_invasion_sightings;
END;

-- CREATE GROUPED PROCEDURES
-- Quest Section
CREATE PROCEDURE update_daily_quest_grouped_stats()
BEGIN
    TRUNCATE TABLE daily_quest_grouped_stats;

    INSERT INTO daily_quest_grouped_stats (day, area_name, ar_type, normal_type, reward_ar_type, reward_normal_type, reward_ar_item_id, reward_ar_item_amount, reward_normal_item_id, reward_normal_item_amount, reward_ar_poke_id, reward_ar_poke_form, reward_normal_poke_id, reward_normal_poke_form, total)
    SELECT
        CURDATE() - INTERVAL 1 DAY as day,
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
		total
    FROM storage_quest_grouped_stats
    WHERE day = CURDATE() - INTERVAL 1 DAY
    GROUP BY ar_type, reward_ar_type, normal_type, reward_normal_type, reward_ar_item_id, reward_ar_item_amount, reward_normal_item_id, reward_normal_item_amount, reward_ar_poke_id, reward_ar_poke_form, reward_normal_poke_id, reward_normal_poke_form, area_name
	ORDER BY area_name, ar_type, normal_type, reward_ar_item_id, reward_normal_item_id, reward_ar_poke_id, reward_normal_poke_id ASC
END;

CREATE PROCEDURE update_weekly_quest_grouped_stats()
BEGIN
    TRUNCATE TABLE weekly_quest_grouped_stats;

    INSERT INTO weekly_quest_grouped_stats (day, area_name, ar_type, normal_type, reward_ar_type, reward_normal_type, reward_ar_item_id, reward_ar_item_amount, reward_normal_item_id, reward_normal_item_amount, reward_ar_poke_id, reward_ar_poke_form, reward_normal_poke_id, reward_normal_poke_form, total)
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
		SUM(total) as total
    FROM storage_quest_grouped_stats
    WHERE day >= DATE_SUB(CURDATE(), INTERVAL DAYOFWEEK(CURDATE()) + 6 DAY) AND day < DATE_SUB(CURDATE(), INTERVAL DAYOFWEEK(CURDATE()) - 1 DAY)
    GROUP BY ar_type, reward_ar_type, normal_type, reward_normal_type, reward_ar_item_id, reward_ar_item_amount, reward_normal_item_id, reward_normal_item_amount, reward_ar_poke_id, reward_ar_poke_form, reward_normal_poke_id, reward_normal_poke_form, area_name
    ORDER BY area_name, ar_type, normal_type, reward_ar_item_id, reward_normal_item_id, reward_ar_poke_id, reward_normal_poke_id ASC
END;

CREATE PROCEDURE update_monthly_quest_grouped_stats
BEGIN
	TRUNCATE TABLE monthly_quest_grouped_stats;

	INSERT INTO monthly_quest_grouped_stats (day, area_name, ar_type, normal_type, reward_ar_type, reward_normal_type, reward_ar_item_id, reward_ar_item_amount, reward_normal_item_id, reward_normal_item_amount, reward_ar_poke_id, reward_ar_poke_form, reward_normal_poke_id, reward_normal_poke_form, total)
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
		SUM(total) as total
    FROM storage_quest_grouped_stats
    WHERE day >= CURDATE() - INTERVAL 1 MONTH
    GROUP BY ar_type, reward_ar_type, normal_type, reward_normal_type, reward_ar_item_id, reward_ar_item_amount, reward_normal_item_id, reward_normal_item_amount, reward_ar_poke_id, reward_ar_poke_form, reward_normal_poke_id, reward_normal_poke_form, area_name
    ORDER BY area_name, ar_type, normal_type, reward_ar_item_id, reward_normal_item_id, reward_ar_poke_id, reward_normal_poke_id ASC
END;

-- Raid Section
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
    GROUP BY level, pokemon_id, form, costume, ex_raid_eligible, is_exclusive, area_name
	ORDER BY area_name, level, pokemon_id ASC
END;

CREATE PROCEDURE update_weekly_raid_grouped_stats()
BEGIN
    TRUNCATE TABLE weekly_raid_grouped_stats;

    INSERT INTO weekly_raid_grouped_stats (day, area_name, level, pokemon_id, form, costume, ex_raid_eligible, is_exclusive, total)
    SELECT
        DATE_SUB(CURDATE(), INTERVAL DAYOFWEEK(CURDATE()) - 1 DAY) as day,
        area_name,
        level,
        pokemon_id,
		form,
		costume,
		ex_raid_eligible,
		is_exclusive,
		SUM(total) AS total
    FROM storage_raid_grouped_stats
    WHERE day >= DATE_SUB(CURDATE(), INTERVAL DAYOFWEEK(CURDATE()) + 6 DAY) AND day < DATE_SUB(CURDATE(), INTERVAL DAYOFWEEK(CURDATE()) - 1 DAY)
    GROUP BY level, pokemon_id, form, costume, ex_raid_eligible, is_exclusive, area_name
	ORDER BY area_name, level, pokemon_id ASC
END;

CREATE PROCEDURE update_monthly_raid_grouped_stats()
BEGIN
    TRUNCATE TABLE monthly_raid_grouped_stats;

    INSERT INTO monthly_raid_grouped_stats (day, area_name, level, pokemon_id, form, costume, ex_raid_eligible, is_exclusive, total)
    SELECT
        LAST_DAY(CURDATE() - INTERVAL 1 MONTH) as day,
        area_name,
        level,
        pokemon_id,
		form,
		costume,
		ex_raid_eligible,
		is_exclusive,
		SUM(total) AS total
    FROM storage_raid_grouped_stats
    WHERE day >= CURDATE() - INTERVAL 1 MONTH
    GROUP BY level, pokemon_id, form, costume, ex_raid_eligible, is_exclusive, area_name
	ORDER BY area_name, level, pokemon_id ASC
END;

-- Invasion Section
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
    GROUP BY display_type, grunt, area_name
	ORDER BY area_name, display_type, grunt ASC
END;

CREATE PROCEDURE update_weekly_invasion_grouped_stats()
BEGIN
    TRUNCATE TABLE weekly_invasion_grouped_stats;

    INSERT INTO weekly_invasion_grouped_stats (day, area_name, display_type, grunt, total_grunts)
    SELECT
        DATE_SUB(CURDATE(), INTERVAL DAYOFWEEK(CURDATE()) - 1 DAY) as day,
        area_name,
        display_type,
        grunt,
        SUM(total_grunts) as total_grunts
    FROM storage_invasion_grouped_stats
    WHERE day >= DATE_SUB(CURDATE(), INTERVAL DAYOFWEEK(CURDATE()) + 6 DAY) AND day < DATE_SUB(CURDATE(), INTERVAL DAYOFWEEK(CURDATE()) - 1 DAY)
    GROUP BY display_type, grunt, area_name
	ORDER BY area_name, display_type, grunt ASC
END;

CREATE PROCEDURE update_monthly_invasion_grouped_stats()
BEGIN
    TRUNCATE TABLE weekly_invasion_grouped_stats;

    INSERT INTO weekly_invasion_grouped_stats (day, area_name, display_type, grunt, total_grunts)
    SELECT
        LAST_DAY(CURDATE() - INTERVAL 1 MONTH) as day,
        area_name,
        display_type,
        grunt,
        SUM(total_grunts) as total_grunts
    FROM storage_invasion_grouped_stats
    WHERE day >= CURDATE() - INTERVAL 1 MONTH
    GROUP BY display_type, grunt, area_name
	ORDER BY area_name, display_type, grunt ASC
END;

-- CREATE TOTAL PROCEDURES
-- Quest Section
CREATE PROCEDURE update_daily_quest_total_stats()
BEGIN
    TRUNCATE TABLE daily_quest_total_stats;

    INSERT INTO daily_quest_total_stats (day, area_name, total_stops, ar, normal)
    SELECT
    	CURDATE() - INTERVAL 1 DAY AS day,
        area_name,
        total_stops,
        ar,
        normal
    FROM storage_quest_total_stats
    WHERE day = CURDATE() - INTERVAL 1 DAY;
END;

CREATE PROCEDURE update_total_quest_total_stats()
BEGIN
	INSERT INTO quest_total_stats (area_name, ar, normal)
	SELECT
    	d.area_name,
    	d.ar,
    	d.normal
	FROM storage_quest_total_stats d
	WHERE d.day = CURDATE() - INTERVAL 1 DAY
	ON DUPLICATE KEY UPDATE
    	ar = quest_total_stats.ar + d.ar,
    	normal = quest_total_stats.normal + d.normal
END;

-- Raid Section
CREATE PROCEDURE update_hourly_raid_total_stats()
BEGIN
	DROP TEMPORARY TABLE IF EXISTS temp_hourly_raid_total_stats;
    CREATE TEMPORARY TABLE temp_hourly_raid_total_stats AS
    SELECT *
    FROM raid_sightings
    WHERE inserted_at >= NOW() - INTERVAL 60 MINUTE;

    DROP TEMPORARY TABLE IF EXISTS all_raid_area_names;
    CREATE TEMPORARY TABLE all_raid_area_names AS
    SELECT DISTINCT area_name
    FROM raid_sightings;

    REPLACE INTO hourly_raid_total_stats (area_name, total, total_ex_raid, total_exclusive)
    SELECT
        a.area_name,
        COALESCE(t.total, 0) AS total,
        COALESCE(t.total_ex_raid, 0) AS total_ex_raid,
        COALESCE(t.total_exclusive, 0) AS total_exclusive
    FROM all_raid_area_names a
    LEFT JOIN (
        SELECT
            area_name,
            COUNT(*) AS total,
            SUM(CASE WHEN ex_raid_eligible = 1 THEN 1 ELSE 0 END) AS total_ex_raid,
            SUM(CASE WHEN is_exclusive = 1 THEN 1 ELSE 0 END) AS total_exclusive
        FROM temp_hourly_raid_total_stats
        GROUP BY area_name
    ) t ON a.area_name = t.area_name;

    DROP TEMPORARY TABLE IF EXISTS temp_hourly_raid_total_stats;
    DROP TEMPORARY TABLE IF EXISTS all_raid_area_names;
END;

CREATE PROCEDURE update_daily_raid_total_stats()
BEGIN
    TRUNCATE TABLE daily_raid_total_stats;

    INSERT INTO daily_raid_total_stats (day, area_name, total, total_ex_raid, total_exclusive)
    SELECT
    	CURDATE() - INTERVAL 1 DAY AS day,
        area_name,
        total,
        total_ex_raid,
        total_exclusive
    FROM storage_raid_total_stats
    WHERE day = CURDATE() - INTERVAL 1 DAY;
END;

CREATE PROCEDURE update_total_raid_total_stats()
BEGIN
	INSERT INTO raid_total_stats (area_name, total, total_ex_raid, total_exclusive)
	SELECT
    	d.area_name,
    	d.total,
    	d.total_ex_raid,
    	d.total_exclusive
	FROM storage_raid_total_stats d
	WHERE d.day = CURDATE() - INTERVAL 1 DAY
	ON DUPLICATE KEY UPDATE
    	total = raid_total_stats.total + d.total,
    	total_ex_raid = raid_total_stats.total_ex_raid + d.total_ex_raid,
    	total_exclusive = raid_total_stats.total_exclusive + d.total_exclusive
END;

-- Invasion Section
CREATE PROCEDURE update_hourly_invasion_total_stats()
BEGIN
	DROP TEMPORARY TABLE IF EXISTS temp_hourly_invasion_total_stats;
    CREATE TEMPORARY TABLE temp_hourly_invasion_total_stats AS
    SELECT *
    FROM invasion_sightings
    WHERE inserted_at >= NOW() - INTERVAL 60 MINUTE;

    DROP TEMPORARY TABLE IF EXISTS all_invasion_area_names;
    CREATE TEMPORARY TABLE all_invasion_area_names AS
    SELECT DISTINCT area_name
    FROM invasion_sightings;

    REPLACE INTO hourly_invasion_total_stats (area_name, total_grunts, total_confirmed, total_unconfirmed)
    SELECT
        a.area_name,
        COALESCE(t.total_grunts, 0) AS total_grunts,
        COALESCE(t.total_confirmed, 0) AS total_confirmed,
        COALESCE(t.total_unconfirmed, 0) AS total_unconfirmed
    FROM all_invasion_area_names a
    LEFT JOIN (
        SELECT
            area_name,
            SUM(CASE WHEN confirmed = 0 THEN 1 ELSE 0 END) AS total_grunts,
            SUM(CASE WHEN confirmed = 1 THEN 1 ELSE 0 END) AS total_confirmed,
			SUM(CASE WHEN confirmed = 0 THEN 1 ELSE 0 END) - SUM(CASE WHEN confirmed = 1 THEN 1 ELSE 0 END) AS total_unconfirmed
        FROM temp_hourly_invasion_total_stats
        GROUP BY area_name
    ) t ON a.area_name = t.area_name;

    DROP TEMPORARY TABLE IF EXISTS temp_hourly_invasion_total_stats;
    DROP TEMPORARY TABLE IF EXISTS all_invasion_area_names;
END;

CREATE PROCEDURE update_daily_invasion_total_stats()
BEGIN
    TRUNCATE TABLE daily_invasion_total_stats;

    INSERT INTO daily_invasion_total_stats (day, area_name, total_grunts, total_confirmed, total_unconfirmed)
    SELECT
    	CURDATE() - INTERVAL 1 DAY AS day,
        area_name,
        total_grunts,
        total_confirmed,
        total_unconfirmed
    FROM storage_invasion_total_stats
    WHERE day = CURDATE() - INTERVAL 1 DAY;
END;

CREATE PROCEDURE update_total_invasion_total_stats()
BEGIN
	INSERT INTO invasion_total_stats (area_name, total_grunts, total_confirmed, total_unconfirmed)
	SELECT
    	d.area_name,
    	d.total_grunts,
    	d.total_confirmed,
    	d.total_unconfirmed
	FROM storage_invasion_total_stats d
	WHERE d.day = CURDATE() - INTERVAL 1 DAY
	ON DUPLICATE KEY UPDATE
    	total_grunts = invasion_total_stats.total_grunts + d.total_grunts,
    	total_confirmed = invasion_total_stats.total_confirmed + d.total_confirmed,
    	total_unconfirmed = invasion_total_stats.total_unconfirmed + d.total_unconfirmed
END;


-- CREATE DATA STORAGE EVENTS
-- Quest
CREATE EVENT IF NOT EXISTS store_storage_quest_grouped
ON SCHEDULE EVERY 1 DAY
STARTS ADDDATE(ADDDATE(CURDATE(), INTERVAL 1 DAY), INTERVAL '15:00:00' HOUR_SECOND)

DO
CALL store_quest_grouped_stats();

CREATE EVENT IF NOT EXISTS store_storage_quest_total
ON SCHEDULE EVERY 1 DAY
STARTS ADDDATE(ADDDATE(CURDATE(), INTERVAL 1 DAY), INTERVAL '15:00:00' HOUR_SECOND)

DO
CALL store_quest_total_stats();

-- Raid
CREATE EVENT IF NOT EXISTS store_storage_raid_grouped
ON SCHEDULE EVERY 1 DAY
STARTS ADDDATE(ADDDATE(CURDATE(), INTERVAL 1 DAY), INTERVAL '00:00:00' HOUR_SECOND)

DO
CALL store_raid_grouped_stats();


CREATE EVENT IF NOT EXISTS store_storage_raid_total
ON SCHEDULE EVERY 1 DAY
STARTS ADDDATE(ADDDATE(CURDATE(), INTERVAL 1 DAY), INTERVAL '00:00:00' HOUR_SECOND)

DO
CALL store_raid_total_stats();

-- Invasion
CREATE EVENT IF NOT EXISTS store_storage_invasion_grouped
ON SCHEDULE EVERY 1 DAY
STARTS ADDDATE(ADDDATE(CURDATE(), INTERVAL 1 DAY), INTERVAL '00:00:00' HOUR_SECOND)

DO
CALL store_invasion_grouped_stats();

CREATE EVENT IF NOT EXISTS store_storage_invasion_total
ON SCHEDULE EVERY 1 DAY
STARTS ADDDATE(ADDDATE(CURDATE(), INTERVAL 1 DAY), INTERVAL '00:00:00' HOUR_SECOND)

DO
CALL store_invasion_total_stats();

-- CREATE GROUPED EVENTS
-- Quest Section
-- Daily
CREATE EVENT IF NOT EXISTS event_update_daily_quest_grouped
ON SCHEDULE EVERY 1 DAY
STARTS ADDDATE(ADDDATE(CURDATE(), INTERVAL 1 DAY), INTERVAL '15:10:00' HOUR_SECOND)

DO
CALL update_daily_quest_grouped_stats();

-- Weekly
CREATE EVENT IF NOT EXISTS event_update_weekly_quest_grouped
ON SCHEDULE EVERY 1 WEEK
STARTS STR_TO_DATE(CONCAT(DATE_FORMAT(CURDATE() + INTERVAL 8 - DAYOFWEEK(CURDATE()) DAY, '%Y-%m-%d'), ' 15:20:00'), '%Y-%m-%d %H:%i:%s')

DO
CALL update_weekly_quest_grouped_stats();

-- Monthly
CREATE EVENT IF NOT EXISTS event_update_monthly_quest_grouped
ON SCHEDULE EVERY 1 MONTH
STARTS STR_TO_DATE(CONCAT(YEAR(NOW()), '-', MONTH(NOW()) + (DAY(NOW()) > 1), '-01 15:30:00'), '%Y-%m-%d %H:%i:%s')

DO
CALL update_monthly_quest_grouped_stats();

-- Raid Section
-- Daily
CREATE EVENT IF NOT EXISTS event_update_daily_raid_grouped
ON SCHEDULE EVERY 1 DAY
STARTS ADDDATE(ADDDATE(CURDATE(), INTERVAL 1 DAY), INTERVAL '02:00:00' HOUR_SECOND)

DO
CALL update_daily_raid_grouped_stats();

-- Weekly
CREATE EVENT IF NOT EXISTS event_update_weekly_raid_grouped
ON SCHEDULE EVERY 1 WEEK
STARTS STR_TO_DATE(CONCAT(DATE_FORMAT(CURDATE() + INTERVAL 8 - DAYOFWEEK(CURDATE()) DAY, '%Y-%m-%d'), ' 02:10:00'), '%Y-%m-%d %H:%i:%s')

DO
CALL update_weekly_raid_grouped_stats();
-- Monthly
CREATE EVENT IF NOT EXISTS event_update_monthly_raid_grouped
ON SCHEDULE EVERY 1 MONTH
STARTS STR_TO_DATE(CONCAT(YEAR(NOW()), '-', MONTH(NOW()) + (DAY(NOW()) > 1), '-01 03:00:00'), '%Y-%m-%d %H:%i:%s')

DO
CALL update_monthly_raid_grouped_stats();

-- Invasion Section
-- Daily
CREATE EVENT IF NOT EXISTS event_update_daily_invasion_grouped
ON SCHEDULE EVERY 1 DAY
STARTS ADDDATE(ADDDATE(CURDATE(), INTERVAL 1 DAY), INTERVAL '02:00:00' HOUR_SECOND)

DO
CALL update_daily_invasion_grouped_stats();

-- Weekly
CREATE EVENT IF NOT EXISTS event_update_weekly_invasion_grouped
ON SCHEDULE EVERY 1 WEEK
STARTS STR_TO_DATE(CONCAT(DATE_FORMAT(CURDATE() + INTERVAL 8 - DAYOFWEEK(CURDATE()) DAY, '%Y-%m-%d'), ' 02:10:00'), '%Y-%m-%d %H:%i:%s')

DO
CALL update_weekly_invasion_grouped_stats();

-- Monthly
CREATE EVENT IF NOT EXISTS event_update_monthly_invasion_grouped
ON SCHEDULE EVERY 1 MONTH
STARTS STR_TO_DATE(CONCAT(YEAR(NOW()), '-', MONTH(NOW()) + (DAY(NOW()) > 1), '-01 03:00:00'), '%Y-%m-%d %H:%i:%s')

DO
CALL update_monthly_invasion_grouped_stats();

-- CREATE TOTAL EVENTS
-- Quest Section
-- Daily
CREATE EVENT IF NOT EXISTS event_update_daily_quest_total
ON SCHEDULE EVERY 1 DAY
STARTS ADDDATE(ADDDATE(CURDATE(), INTERVAL 1 DAY), INTERVAL '15:10:00' HOUR_SECOND)

DO
CALL update_daily_quest_total_stats();

-- Total
CREATE EVENT IF NOT EXISTS event_update_total_quest_total
ON SCHEDULE EVERY 1 DAY
STARTS ADDDATE(ADDDATE(CURDATE(), INTERVAL 1 DAY), INTERVAL '15:15:00' HOUR_SECOND)

DO
CALL update_total_quest_total_stats();

-- Raid Section
-- Hourly
CREATE EVENT IF NOT EXISTS event_update_hourly_raid_total
ON SCHEDULE EVERY 1 HOUR
STARTS (CURRENT_TIMESTAMP + INTERVAL 1 HOUR - INTERVAL MINUTE(CURRENT_TIMESTAMP) MINUTE - INTERVAL SECOND(CURRENT_TIMESTAMP) SECOND)
DO
CALL update_hourly_raid_total_stats();

-- Daily
CREATE EVENT IF NOT EXISTS event_update_daily_raid_total
ON SCHEDULE EVERY 1 DAY
STARTS ADDDATE(ADDDATE(CURDATE(), INTERVAL 1 DAY), INTERVAL '02:00:00' HOUR_SECOND)

DO
CALL update_daily_raid_total_stats();

-- Total
CREATE EVENT IF NOT EXISTS event_update_total_raid_total
ON SCHEDULE EVERY 1 DAY
STARTS ADDDATE(ADDDATE(CURDATE(), INTERVAL 1 DAY), INTERVAL '02:10:00' HOUR_SECOND)

DO
CALL update_total_raid_total_stats();

-- Invasion Section
-- Hourly
CREATE EVENT IF NOT EXISTS event_update_hourly_invasion_total
ON SCHEDULE EVERY 1 HOUR
STARTS (CURRENT_TIMESTAMP + INTERVAL 1 HOUR - INTERVAL MINUTE(CURRENT_TIMESTAMP) MINUTE - INTERVAL SECOND(CURRENT_TIMESTAMP) SECOND)

DO
CALL update_hourly_invasion_total_stats();

-- Daily
CREATE EVENT IF NOT EXISTS event_update_daily_invasion_total
ON SCHEDULE EVERY 1 DAY
STARTS ADDDATE(ADDDATE(CURDATE(), INTERVAL 1 DAY), INTERVAL '02:00:00' HOUR_SECOND)

DO
CALL update_daily_invasion_total_stats();

-- Total
CREATE EVENT IF NOT EXISTS event_update_total_invasion_total
ON SCHEDULE EVERY 1 DAY
STARTS ADDDATE(ADDDATE(CURDATE(), INTERVAL 1 DAY), INTERVAL '02:10:00' HOUR_SECOND)

DO
CALL update_total_invasion_total_stats();

-- CREATE CLEANING PROCEDURES
-- Quest Section
CREATE PROCEDURE delete_quest_sightings_batches()
BEGIN
  DECLARE done INT DEFAULT FALSE;
  DECLARE CONTINUE HANDLER FOR NOT FOUND SET done = TRUE;

  WHILE NOT done DO
    DELETE FROM quest_sightings
    WHERE inserted_at < CURDATE() - INTERVAL 1 DAY
    LIMIT 50000;

    IF (ROW_COUNT() = 0) THEN
      SET done = TRUE;
    END IF;
  END WHILE;
END;

-- Raid Section
CREATE PROCEDURE delete_raid_sightings_batches()
BEGIN
  DECLARE done INT DEFAULT FALSE;
  DECLARE CONTINUE HANDLER FOR NOT FOUND SET done = TRUE;

  WHILE NOT done DO
    DELETE FROM raid_sightings
    WHERE inserted_at < CURDATE() - INTERVAL 1 DAY
    LIMIT 50000;

    IF (ROW_COUNT() = 0) THEN
      SET done = TRUE;
    END IF;
  END WHILE;
END;

-- Invasion Section
CREATE PROCEDURE delete_invasion_sightings_batches()
BEGIN
  DECLARE done INT DEFAULT FALSE;
  DECLARE CONTINUE HANDLER FOR NOT FOUND SET done = TRUE;

  WHILE NOT done DO
    DELETE FROM invasion_sightings
    WHERE inserted_at < CURDATE() - INTERVAL 1 DAY
    LIMIT 50000;

    IF (ROW_COUNT() = 0) THEN
      SET done = TRUE;
    END IF;
  END WHILE;
END;

-- CREATE CLEANING EVENTS
-- Quest Section
CREATE EVENT IF NOT EXISTS clean_quest_sightings
ON SCHEDULE EVERY 1 DAY
STARTS ADDDATE(ADDDATE(CURDATE(), INTERVAL 1 DAY), INTERVAL '16:00:00' HOUR_SECOND)

DO
CALL delete_quest_sightings_batches();

-- Raid Section
CREATE EVENT IF NOT EXISTS clean_raid_sightings
ON SCHEDULE EVERY 1 DAY
STARTS ADDDATE(ADDDATE(CURDATE(), INTERVAL 1 DAY), INTERVAL '06:00:00' HOUR_SECOND)

DO
CALL delete_raid_sightings_batches();

-- Invasion Section
CREATE EVENT IF NOT EXISTS clean_invasion_sightings
ON SCHEDULE EVERY 1 DAY
STARTS ADDDATE(ADDDATE(CURDATE(), INTERVAL 1 DAY), INTERVAL '05:05:00' HOUR_SECOND)

DO
CALL delete_invasion_sightings_batches();