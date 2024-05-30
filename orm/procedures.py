from config.app_config import app_config
from orm.queries import DatabaseOperations

class ProcedureGenerator:
    def __init__(self):
        self.db_ops = DatabaseOperations()
        self.db_timezone_offset = app_config.db_timezone_offset

    async def generate_store_pokemon_total_sql(self, area_names, timezone_offset):
        offset_diff = timezone_offset - self.db_timezone_offset
        procedure_name = f"store_pokemon_total_{abs(offset_diff)}"
        area_names_str = ', '.join([f"'{name}'" for name in area_names])

        drop_procedure_sql = f"DROP PROCEDURE IF EXISTS {procedure_name};"

        create_procedure_sql=f"""
        CREATE PROCEDURE {procedure_name}()
        BEGIN
            CREATE TEMPORARY TABLE IF NOT EXISTS temp_total_pokemon_sightings AS
            SELECT *
            FROM pokemon_sightings
            WHERE area_name IN ({area_names_str})
              AND inserted_at >= CURDATE() - INTERVAL 1 DAY AND inserted_at < CURDATE();

            REPLACE INTO storage_pokemon_total_stats (day, area_name, total, total_iv100, total_iv0, total_top1_little, total_top1_great, total_top1_ultra, total_shiny, avg_despawn)
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

            DROP TEMPORARY TABLE IF NOT EXISTS temp_total_pokemon_sightings;
        END;
        """
        return drop_procedure_sql, create_procedure_sql

    async def generate_store_pokemon_grouped_sql(self, area_names, timezone_offset):
        offset_diff = timezone_offset - self.db_timezone_offset
        procedure_name = f"store_pokemon_grouped_{abs(offset_diff)}"
        area_names_str = ', '.join([f"'{name}'" for name in area_names])
        sql = f"""
        DELIMITER $$
        DROP PROCEDURE IF EXISTS {procedure_name}$$
        CREATE PROCEDURE {procedure_name}()
        BEGIN
            CREATE TEMPORARY TABLE IF NOT EXISTS temp_grouped_pokemon_sightings AS
            SELECT *
            FROM pokemon_sightings
            WHERE area_name IN ({area_names_str})
              AND inserted_at >= CURDATE() - INTERVAL 1 DAY AND inserted_at < CURDATE();

            REPLACE INTO storage_pokemon_grouped_stats (day, pokemon_id, form, avg_lat, avg_lon, total, total_iv100, total_iv0, total_top1_little, total_top1_great, total_top1_ultra, total_shiny, area_name, avg_despawn)
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

            DROP TEMPORARY TABLE IF NOT EXISTS temp_grouped_pokemon_sightings;
        END$$
        DELIMITER ;
        """
        return sql

    async def generate_store_quest_total_sql(self, area_names, timezone_offset):
        offset_diff = timezone_offset - self.db_timezone_offset
        procedure_name = f"store_quest_total_{abs(offset_diff)}"
        area_names_str = ', '.join([f"'{name}'" for name in area_names])
        sql = f"""
        DELIMITER $$

        DROP PROCEDURE IF EXISTS {procedure_name}$$
        CREATE PROCEDURE {procedure_name}()
        BEGIN
            DROP TEMPORARY TABLE IF NOT EXISTS temp_store_total_quest_sightings;
            CREATE TEMPORARY TABLE temp_store_total_quest_sightings AS
            SELECT qs.*,
                   CASE
                       WHEN HOUR(qs.inserted_at) >= 21 OR HOUR(qs.inserted_at) < 5 THEN 1
                       WHEN HOUR(qs.inserted_at) >= 7 AND HOUR(qs.inserted_at) < 14 THEN 2
                       ELSE 0
                   END AS scanned
            FROM quest_sightings qs
            WHERE qs.area_name IN ({area_names_str})
              AND qs.inserted_at >= NOW() - INTERVAL 1 DAY AND qs.inserted_at < NOW();

            REPLACE INTO storage_quest_total_stats(day, area_name, total_stops, ar, normal, scanned)
            SELECT
                DATE(NOW()) as day,
                tqs.area_name,
                (
                    SELECT tp.total_stops
                    FROM total_pokestops tp
                    WHERE tp.area_name = tqs.area_name
                    LIMIT 1
                ) AS total_stops,
                COUNT(tqs.ar_type) AS ar,
                COUNT(tqs.normal_type) AS normal,
                tqs.scanned
            FROM temp_store_total_quest_sightings tqs
            GROUP BY tqs.area_name, tqs.scanned
            ORDER BY tqs.area_name ASC, tqs.scanned ASC;

            DROP TEMPORARY TABLE IF NOT EXISTS temp_store_total_quest_sightings;
        END$$
        DELIMITER ;
        """
        return sql

    async def generate_store_quest_grouped_sql(self, area_names, timezone_offset):
        offset_diff = timezone_offset - self.db_timezone_offset
        procedure_name = f"store_quest_grouped_{abs(offset_diff)}"
        area_names_str = ', '.join([f"'{name}'" for name in area_names])
        sql = f"""
        DELIMITER $$

        DROP PROCEDURE IF EXISTS {procedure_name}$$
        CREATE PROCEDURE {procedure_name}()
        BEGIN
            DROP TEMPORARY TABLE IF NOT EXISTS temp_store_grouped_quest_sightings;
            CREATE TEMPORARY TABLE temp_store_grouped_quest_sightings AS
            SELECT *,
                   CASE
                       WHEN HOUR(inserted_at) >= 22 OR HOUR(inserted_at) < 5 THEN 1
                       WHEN HOUR(inserted_at) >= 7 AND HOUR(inserted_at) < 14 THEN 2
                       ELSE 0
                   END AS scanned
            FROM quest_sightings
            WHERE area_name IN ({area_names_str})
              AND inserted_at >= NOW() - INTERVAL 1 DAY AND inserted_at < NOW();

            REPLACE INTO storage_quest_grouped_stats (day, area_name, ar_type, normal_type, reward_ar_type, reward_normal_type, reward_ar_item_id, reward_ar_item_amount, reward_normal_item_id, reward_normal_item_amount, reward_ar_poke_id, reward_ar_poke_form, reward_normal_poke_id, reward_normal_poke_form, total, scanned)
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

            DROP TEMPORARY TABLE IF NOT EXISTS temp_store_grouped_quest_sightings;
        END$$
        DELIMITER ;
        """
        return sql

    async def generate_store_raid_total_sql(self, area_names, timezone_offset):
        offset_diff = timezone_offset - self.db_timezone_offset
        procedure_name = f"store_raid_total_{abs(offset_diff)}"
        area_names_str = ', '.join([f"'{name}'" for name in area_names])
        sql = f"""
        DELIMITER $$

        DROP PROCEDURE IF EXISTS {procedure_name}$$
        CREATE PROCEDURE {procedure_name}()
        BEGIN
            DROP TEMPORARY TABLE IF NOT EXISTS temp_store_total_raid_sightings;
            CREATE TEMPORARY TABLE temp_store_total_raid_sightings AS
            SELECT *
            FROM raid_sightings
            WHERE area_name IN ({area_names_str})
              AND inserted_at >= CURDATE() - INTERVAL 1 DAY AND inserted_at < CURDATE();

            REPLACE INTO storage_raid_total_stats (day, area_name, total, total_ex_raid, total_exclusive)
            SELECT
                CURDATE() - INTERVAL 1 DAY as day,
                area_name,
                COUNT(*) AS total,
                SUM(CASE WHEN ex_raid_eligible = 1 THEN 1 ELSE 0 END) AS total_ex_raid,
                SUM(CASE WHEN is_exclusive = 1 THEN 1 ELSE 0 END) AS total_exclusive
            FROM temp_store_total_raid_sightings
            GROUP BY area_name;

            DROP TEMPORARY TABLE IF NOT EXISTS temp_store_total_raid_sightings;
        END$$
        DELIMITER ;
        """
        return sql

    async def generate_store_raid_grouped_sql(self, area_names, timezone_offset):
        offset_diff = timezone_offset - self.db_timezone_offset
        procedure_name = f"store_raid_grouped_{abs(offset_diff)}"
        area_names_str = ', '.join([f"'{name}'" for name in area_names])
        sql = f"""
        DELIMITER$$

        DROP PROCEDURE IF EXISTS {procedure_name}$$
        CREATE PROCEDURE {procedure_name}()
        BEGIN
            DROP TEMPORARY TABLE IF NOT EXISTS temp_store_grouped_raid_sightings;
            CREATE TEMPORARY TABLE temp_store_grouped_raid_sightings AS
            SELECT *
            FROM raid_sightings
            WHERE area_name IN ({area_names_str})
              AND inserted_at >= CURDATE() - INTERVAL 1 DAY AND inserted_at < CURDATE();

            REPLACE INTO storage_raid_grouped_stats (day, area_name, level, pokemon_id, form, costume, ex_raid_eligible, is_exclusive, total)
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

            DROP TEMPORARY TABLE IF NOT EXISTS temp_store_grouped_raid_sightings;
        END$$
        DELIMITER ;
        """
        return sql

    async def generate_store_invasion_total_sql(self, area_names, timezone_offset):
        offset_diff = timezone_offset - self.db_timezone_offset
        procedure_name = f"store_invasion_total_{abs(offset_diff)}"
        area_names_str = ', '.join([f"'{name}'" for name in area_names])
        sql = f"""
        DELIMITER$$
        DROP PROCEDURE IF EXISTS {procedure_name}$$
        CREATE PROCEDURE {procedure_name}()
        BEGIN
            DROP TEMPORARY TABLE IF NOT EXISTS temp_store_total_invasion_sightings;
            CREATE TEMPORARY TABLE temp_store_total_invasion_sightings AS
            SELECT *
            FROM invasion_sightings
            WHERE area_name IN ({area_names_str})
              AND inserted_at >= CURDATE() - INTERVAL 1 DAY AND inserted_at < CURDATE();

            REPLACE INTO storage_invasion_total_stats (day, area_name, total_grunts, total_confirmed, total_unconfirmed)
            SELECT
                CURDATE() - INTERVAL 1 DAY as day,
                area_name,
                SUM(CASE WHEN confirmed = 0 THEN 1 ELSE 0 END) AS total_grunts,
                SUM(CASE WHEN confirmed = 1 THEN 1 ELSE 0 END) AS total_confirmed,
                SUM(CASE WHEN confirmed = 0 THEN 1 ELSE 0 END) - SUM(CASE WHEN confirmed = 1 THEN 1 ELSE 0 END) AS total_unconfirmed
            FROM temp_store_total_invasion_sightings
            GROUP BY area_name;

            DROP TEMPORARY TABLE IF NOT EXISTS temp_store_total_invasion_sightings;
        END$$
        DELIMITER ;
        """
        return sql

    async def generate_store_invasion_grouped_sql(self, area_names, timezone_offset):
        offset_diff = timezone_offset - self.db_timezone_offset
        procedure_name = f"store_invasion_grouped_{abs(offset_diff)}"
        area_names_str = ', '.join([f"'{name}'" for name in area_names])
        sql = f"""
        DELIMITER$$

        DROP PROCEDURE IF EXISTS {procedure_name}$$
        CREATE PROCEDURE {procedure_name}()
        BEGIN
            DROP TEMPORARY TABLE IF NOT EXISTS temp_store_grouped_invasion_sightings;
            CREATE TEMPORARY TABLE temp_store_grouped_invasion_sightings AS
            SELECT *
            FROM invasion_sightings
            WHERE area_name IN ({area_names_str})
              AND inserted_at >= CURDATE() - INTERVAL 1 DAY AND inserted_at < CURDATE();

            REPLACE INTO storage_invasion_grouped_stats (day, area_name, display_type, grunt, total_grunts)
            SELECT
                CURDATE() - INTERVAL 1 DAY as day,
                area_name,
                display_type,
                grunt,
                SUM(CASE WHEN confirmed = 0 THEN 1 ELSE 0 END) AS total_grunts
            FROM temp_store_grouped_invasion_sightings
            GROUP BY display_type, grunt, area_name;

            DROP TEMPORARY TABLE IF NOT EXISTS temp_store_grouped_invasion_sightings;
        END$$
        DELIMTIER ;
        """
        return sql

    async def generate_store_hourly_pokemon_tth_procedure(self):
        sql = f"""
        DELIMITER $$
        DROP PROCEDURE IF EXISTS store_hourly_pokemon_tth$$
        CREATE PROCEDURE store_hourly_pokemon_tth()
        BEGIN
            -- Create temporary table with adjusted times for the past hour in the local time zone of each area
            CREATE TEMPORARY TABLE IF NOT EXISTS temp_hourly_tth_stats AS
            SELECT ps.*
            FROM pokemon_sightings ps
            JOIN area_time_zones atz ON ps.area_name = atz.area_name
            WHERE ps.inserted_at >= CONVERT_TZ(NOW(), @@session.time_zone, '+01:00') - INTERVAL 60 MINUTE + INTERVAL atz.time_zone_offset MINUTE;

            -- Create temporary table for TTH stats
            CREATE TEMPORARY TABLE IF NOT EXISTS temp_spawn_tth_by_area AS
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
            FROM temp_hourly_tth_stats
            GROUP BY area_name;

            -- Insert or update the hourly TTH stats
            REPLACE INTO storage_hourly_pokemon_tth_stats (`day_hour`, area_name, tth_5, tth_10, tth_15, tth_20, tth_25, tth_30, tth_35, tth_40, tth_45, tth_50, tth_55, tth_55_plus)
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

            -- Drop temporary tables
            DROP TEMPORARY TABLE IF EXISTS temp_hourly_tth_stats;
            DROP TEMPORARY TABLE IF EXISTS temp_spawn_tth_by_area;
        END$$
        DELIMITER ;
        """
        return sql
