from config.app_config import app_config
from orm.queries import DatabaseOperations

class ProcedureGenerator:
    def __init__(self):
        self.db_ops = DatabaseOperations()
        self.db_timezone_offset = app_config.db_timezone_offset

    def format_timezone_offset(self, offset_minutes):
        hours_offset = offset_minutes // 60
        minutes_offset = offset_minutes % 60
        sign = '+' if offset_minutes >= 0 else '-'
        formatted_offset = f"{sign}{abs(hours_offset):02}:{abs(minutes_offset):02}"
        return formatted_offset


    # Storage Procedures
    async def generate_store_pokemon_total_sql(self, area_names, timezone_offset):

        procedure_name = f"store_pokemon_total_{timezone_offset}"
        area_names_str = ', '.join([f"'{name}'" for name in area_names])

        # Get the formatted timezone offset
        formatted_offset = self.format_timezone_offset(self.db_timezone_offset)

        drop_procedure_sql = f"DROP PROCEDURE IF EXISTS {procedure_name};"

        create_procedure_sql=f"""
        CREATE PROCEDURE {procedure_name}()
        BEGIN
            CREATE TEMPORARY TABLE IF NOT EXISTS temp_total_pokemon_sightings_{timezone_offset} AS
            SELECT ps.*
            FROM pokemon_sightings ps
            JOIN area_time_zones atz ON ps.area_name = atz.area_name
            WHERE ps.area_name IN ({area_names_str})
            AND ps.inserted_at >= CONVERT_TZ(NOW() - INTERVAL 1 DAY, '{formatted_offset}', atz.time_zone)
            AND ps.inserted_at < CONVERT_TZ(NOW(), '{formatted_offset}', atz.time_zone);

            INSERT INTO storage_pokemon_total_stats (day, area_name, total, total_iv100, total_iv0, total_top1_little, total_top1_great, total_top1_ultra, total_shiny, avg_despawn)
            SELECT
                CONVERT_TZ(NOW() - INTERVAL 1 DAY, '{formatted_offset}', atz.time_zone) as day,
                ps.area_name,
                COUNT(ps.pokemon_id) AS total,
                SUM(CASE WHEN ps.iv = 100 THEN 1 ELSE 0 END) AS total_iv100,
                SUM(CASE WHEN ps.iv = 0 THEN 1 ELSE 0 END) AS total_iv0,
                SUM(CASE WHEN ps.pvp_little_rank = 1 THEN 1 ELSE 0 END) AS total_top1_little,
                SUM(CASE WHEN ps.pvp_great_rank = 1 THEN 1 ELSE 0 END) AS total_top1_great,
                SUM(CASE WHEN ps.pvp_ultra_rank = 1 THEN 1 ELSE 0 END) AS total_top1_ultra,
                SUM(CASE WHEN ps.shiny = 1 THEN 1 ELSE 0 END) AS total_shiny,
                AVG(ps.despawn_time) AS avg_despawn
            FROM temp_total_pokemon_sightings_{timezone_offset} ps
            JOIN area_time_zones atz ON ps.area_name = atz.area_name
            GROUP BY ps.area_name;

            DROP TEMPORARY TABLE IF EXISTS temp_total_pokemon_sightings_{timezone_offset};
        END;
        """
        return drop_procedure_sql, create_procedure_sql


    async def generate_store_pokemon_grouped_sql(self, area_names, timezone_offset):

        procedure_name = f"store_pokemon_grouped_{timezone_offset}"
        area_names_str = ', '.join([f"'{name}'" for name in area_names])

        # Get the formatted timezone offset
        formatted_offset = self.format_timezone_offset(self.db_timezone_offset)

        drop_procedure_sql = f"DROP PROCEDURE IF EXISTS {procedure_name};"

        create_procedure_sql = f"""
        CREATE PROCEDURE {procedure_name}()
        BEGIN
            DROP TEMPORARY TABLE IF EXISTS temp_grouped_pokemon_sightings_{timezone_offset};
            CREATE TEMPORARY TABLE temp_grouped_pokemon_sightings_{timezone_offset} AS
            SELECT *
            FROM pokemon_sightings
            JOIN area_time_zones atz ON ps.area_name = atz.area_name
            WHERE ps.area_name IN ({area_names_str})
            AND ps.inserted_at >= CONVERT_TZ(NOW() - INTERVAL 1 DAY, '{formatted_offset}', atz.time_zone)
            AND ps.inserted_at < CONVERT_TZ(NOW(), '{formatted_offset}', atz.time_zone);

            INSERT INTO storage_pokemon_grouped_stats (day, pokemon_id, form, avg_lat, avg_lon, total, total_iv100, total_iv0, total_top1_little, total_top1_great, total_top1_ultra, total_shiny, area_name, avg_despawn)
            SELECT
                CONVERT_TZ(NOW() - INTERVAL 1 DAY, '{formatted_offset}', atz.time_zone) as day,
                pokemon_id,
                form,
                AVG(latitude) AS avg_lat,
                AVG(longitude) AS avg_lon,
                COUNT(pokemon_id) AS total,
                SUM(CASE WHEN iv = 100 THEN 1 ELSE 0 END) AS total_iv100,
                SUM(CASE WHEN iv = 0 THEN 1 ELSE 0 END) AS total_iv0,
                SUM(CASE WHEN pvp_little_rank = 1 THEN 1 ELSE 0 END) AS total_top1_little,
                SUM(CASE WHEN pvp_great_rank = 1 THEN 1 ELSE 0 END) AS total_top1_great,
                SUM(CASE WHEN pvp_ultra_rank = 1 THEN 1 ELSE 0 END) AS total_top1_ultra,
                SUM(CASE WHEN shiny = 1 THEN 1 ELSE 0 END) AS total_shiny,
                area_name,
                AVG(despawn_time) AS avg_despawn
            FROM temp_grouped_pokemon_sightings_{timezone_offset}
            GROUP BY pokemon_id, form, area_name
            ORDER BY area_name, pokemon_id;

            DROP TEMPORARY TABLE IF EXISTS temp_grouped_pokemon_sightings_{timezone_offset};
        END;
        """
        return drop_procedure_sql, create_procedure_sql

    async def generate_store_quest_total_sql(self, area_names, timezone_offset):

        procedure_name = f"store_quest_total_{timezone_offset}"
        area_names_str = ', '.join([f"'{name}'" for name in area_names])

        # Get the formatted timezone offset
        formatted_offset = self.format_timezone_offset(self.db_timezone_offset)


        drop_procedure_sql = f"DROP PROCEDURE IF EXISTS {procedure_name};"

        create_procedure_sql=f"""
        CREATE PROCEDURE {procedure_name}()
        BEGIN
            DROP TEMPORARY TABLE IF EXISTS temp_store_total_quest_sightings_{timezone_offset};
            CREATE TEMPORARY TABLE temp_store_total_quest_sightings_{timezone_offset} AS
            SELECT qs.*,
                   CASE
                       WHEN HOUR(qs.inserted_at) >= 00 OR HOUR(qs.inserted_at) < 5 THEN 1
                       WHEN HOUR(qs.inserted_at) >= 10 AND HOUR(qs.inserted_at) < 14 THEN 2
                       ELSE 0
                   END AS scanned
            FROM quest_sightings qs
            JOIN area_time_zones atz ON qs.area_name = atz.area_name
            WHERE qs.area_name IN ({area_names_str})
            AND qs.inserted_at >= CONVERT_TZ(NOW() - INTERVAL 1 DAY, '{formatted_offset}', atz.time_zone)
            AND qs.inserted_at < CONVERT_TZ(NOW(), '{formatted_offset}', atz.time_zone);

            INSERT INTO storage_quest_total_stats(day, area_name, total_stops, ar, normal, scanned)
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
            FROM temp_store_total_quest_sightings_{timezone_offset} tqs
            GROUP BY tqs.area_name, tqs.scanned
            ORDER BY tqs.area_name ASC, tqs.scanned ASC;

            DROP TEMPORARY TABLE IF EXISTS temp_store_total_quest_sightings_{timezone_offset};
        END;
        """
        return drop_procedure_sql, create_procedure_sql

    async def generate_store_quest_grouped_sql(self, area_names, timezone_offset):

        procedure_name = f"store_quest_grouped_{timezone_offset}"
        area_names_str = ', '.join([f"'{name}'" for name in area_names])

        # Get the formatted timezone offset
        formatted_offset = self.format_timezone_offset(self.db_timezone_offset)

        drop_procedure_sql = f"DROP PROCEDURE IF EXISTS {procedure_name};"

        create_procedure_sql=f"""
        CREATE PROCEDURE {procedure_name}()
        BEGIN
            DROP TEMPORARY TABLE IF EXISTS temp_store_grouped_quest_sightings_{timezone_offset};
            CREATE TEMPORARY TABLE temp_store_grouped_quest_sightings_{timezone_offset} AS
            SELECT *,
                   CASE
                       WHEN HOUR(inserted_at) >= 22 OR HOUR(inserted_at) < 5 THEN 1
                       WHEN HOUR(inserted_at) >= 7 AND HOUR(inserted_at) < 14 THEN 2
                       ELSE 0
                   END AS scanned
            FROM quest_sightings
            JOIN area_time_zones atz ON qs.area_name = atz.area_name
            WHERE qs.area_name IN ({area_names_str})
            AND qs.inserted_at >= CONVERT_TZ(NOW() - INTERVAL 1 DAY, '{formatted_offset}', atz.time_zone)
            AND qs.inserted_at < CONVERT_TZ(NOW(), '{formatted_offset}', atz.time_zone);

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
            FROM temp_store_grouped_quest_sightings_{timezone_offset}
            GROUP BY ar_type, reward_ar_type, normal_type, reward_normal_type, reward_ar_item_id, reward_ar_item_amount, reward_normal_item_id, reward_normal_item_amount, reward_ar_poke_id, reward_ar_poke_form, reward_normal_poke_id, reward_normal_poke_form, area_name, scanned;

            DROP TEMPORARY TABLE IF EXISTS temp_store_grouped_quest_sightings_{timezone_offset};
        END;
        """
        return drop_procedure_sql, create_procedure_sql

    async def generate_store_raid_total_sql(self, area_names, timezone_offset):

        procedure_name = f"store_raid_total_{timezone_offset}"
        area_names_str = ', '.join([f"'{name}'" for name in area_names])

        # Get the formatted timezone offset
        formatted_offset = self.format_timezone_offset(self.db_timezone_offset)

        drop_procedure_sql = f"DROP PROCEDURE IF EXISTS {procedure_name};"

        create_procedure_sql=f"""
        CREATE PROCEDURE {procedure_name}()
        BEGIN
            DROP TEMPORARY TABLE IF EXISTS temp_store_total_raid_sightings_{timezone_offset};
            CREATE TEMPORARY TABLE temp_store_total_raid_sightings_{timezone_offset} AS
            SELECT rs.*
            FROM raid_sightings rs
            JOIN area_time_zones atz ON rs.area_name = atz.area_name
            WHERE rs.area_name IN ({area_names_str})
            AND rs.inserted_at >= CONVERT_TZ(NOW() - INTERVAL 1 DAY, '{formatted_offset}', atz.time_zone)
            AND rs.inserted_at < CONVERT_TZ(NOW(), '{formatted_offset}', atz.time_zone);

            INSERT INTO storage_raid_total_stats (day, area_name, total, total_ex_raid, total_exclusive)
            SELECT
                CONVERT_TZ(NOW() - INTERVAL 1 DAY, '{formatted_offset}', atz.time_zone) as day,
                area_name,
                COUNT(*) AS total,
                SUM(CASE WHEN ex_raid_eligible = 1 THEN 1 ELSE 0 END) AS total_ex_raid,
                SUM(CASE WHEN is_exclusive = 1 THEN 1 ELSE 0 END) AS total_exclusive
            FROM temp_store_total_raid_sightings_{timezone_offset}
            GROUP BY area_name;

            DROP TEMPORARY TABLE IF EXISTS temp_store_total_raid_sightings_{timezone_offset};
        END;
        """
        return drop_procedure_sql, create_procedure_sql

    async def generate_store_raid_grouped_sql(self, area_names, timezone_offset):

        procedure_name = f"store_raid_grouped_{timezone_offset}"
        area_names_str = ', '.join([f"'{name}'" for name in area_names])

        # Get the formatted timezone offset
        formatted_offset = self.format_timezone_offset(self.db_timezone_offset)

        drop_procedure_sql = f"DROP PROCEDURE IF EXISTS {procedure_name};"

        create_procedure_sql=f"""
        CREATE PROCEDURE {procedure_name}()
        BEGIN
            DROP TEMPORARY TABLE IF EXISTS temp_store_grouped_raid_sightings_{timezone_offset};
            CREATE TEMPORARY TABLE temp_store_grouped_raid_sightings_{timezone_offset} AS
            SELECT rs.*
            FROM raid_sightings rs
            JOIN area_time_zones atz ON rs.area_name = atz.area_name
            WHERE rs.area_name IN ({area_names_str})
            AND rs.inserted_at >= CONVERT_TZ(NOW() - INTERVAL 1 DAY, '{formatted_offset}', atz.time_zone)
            AND rs.inserted_at < CONVERT_TZ(NOW(), '{formatted_offset}', atz.time_zone);

            INSERT INTO storage_raid_grouped_stats (day, area_name, level, pokemon_id, form, costume, ex_raid_eligible, is_exclusive, total)
            SELECT
                CONVERT_TZ(NOW() - INTERVAL 1 DAY, '{formatted_offset}', atz.time_zone) as day,
                area_name,
                level,
                pokemon_id,
                form,
                costume,
                ex_raid_eligible,
                is_exclusive,
                COUNT(*) AS total
            FROM temp_store_grouped_raid_sightings_{timezone_offset}
            GROUP BY level, pokemon_id, form, costume, ex_raid_eligible, is_exclusive, area_name;

            DROP TEMPORARY TABLE IF EXISTS temp_store_grouped_raid_sightings_{timezone_offset};
        END;
        """
        return drop_procedure_sql, create_procedure_sql

    async def generate_store_invasion_total_sql(self, area_names, timezone_offset):

        procedure_name = f"store_invasion_total_{timezone_offset}"
        area_names_str = ', '.join([f"'{name}'" for name in area_names])

        # Get the formatted timezone offset
        formatted_offset = self.format_timezone_offset(self.db_timezone_offset)

        drop_procedure_sql = f"DROP PROCEDURE IF EXISTS {procedure_name};"

        create_procedure_sql=f"""
        CREATE PROCEDURE {procedure_name}()
        BEGIN
            DROP TEMPORARY TABLE IF EXISTS temp_store_total_invasion_sightings_{timezone_offset};
            CREATE TEMPORARY TABLE temp_store_total_invasion_sightings_{timezone_offset} AS
            SELECT inv.*
            FROM invasion_sightings inv
            JOIN area_time_zones atz ON inv.area_name = atz.area_name
            WHERE inv.area_name IN ({area_names_str})
            AND inv.inserted_at >= CONVERT_TZ(NOW() - INTERVAL 1 DAY, '{formatted_offset}', atz.time_zone)
            AND inv.inserted_at < CONVERT_TZ(NOW(), '{formatted_offset}', atz.time_zone);

            INSERT INTO storage_invasion_total_stats (day, area_name, total_grunts, total_confirmed, total_unconfirmed)
            SELECT
                CONVERT_TZ(NOW() - INTERVAL 1 DAY, '{formatted_offset}', atz.time_zone) as day,
                area_name,
                SUM(CASE WHEN confirmed = 0 THEN 1 ELSE 0 END) AS total_grunts,
                SUM(CASE WHEN confirmed = 1 THEN 1 ELSE 0 END) AS total_confirmed,
                SUM(CASE WHEN confirmed = 0 THEN 1 ELSE 0 END) - SUM(CASE WHEN confirmed = 1 THEN 1 ELSE 0 END) AS total_unconfirmed
            FROM temp_store_total_invasion_sightings_{timezone_offset}
            GROUP BY area_name;

            DROP TEMPORARY TABLE IF EXISTS temp_store_total_invasion_sightings_{timezone_offset};
        END;
        """
        return drop_procedure_sql, create_procedure_sql

    async def generate_store_invasion_grouped_sql(self, area_names, timezone_offset):

        procedure_name = f"store_invasion_grouped_{timezone_offset}"
        area_names_str = ', '.join([f"'{name}'" for name in area_names])

        # Get the formatted timezone offset
        formatted_offset = self.format_timezone_offset(self.db_timezone_offset)

        drop_procedure_sql = f"DROP PROCEDURE IF EXISTS {procedure_name};"

        create_procedure_sql=f"""
        CREATE PROCEDURE {procedure_name}()
        BEGIN
            DROP TEMPORARY TABLE IF EXISTS temp_store_grouped_invasion_sightings_{timezone_offset};
            CREATE TEMPORARY TABLE temp_store_grouped_invasion_sightings_{timezone_offset} AS
            SELECT inv.*
            FROM invasion_sightings inv
            JOIN area_time_zones atz ON inv.area_name = atz.area_name
            WHERE inv.area_name IN ({area_names_str})
            AND inv.inserted_at >= CONVERT_TZ(NOW() - INTERVAL 1 DAY, '{formatted_offset}', atz.time_zone)
            AND inv.inserted_at < CONVERT_TZ(NOW(), '{formatted_offset}', atz.time_zone);

            INSERT INTO storage_invasion_grouped_stats (day, area_name, display_type, grunt, total_grunts)
            SELECT
                CONVERT_TZ(NOW() - INTERVAL 1 DAY, '{formatted_offset}', atz.time_zone) as day,
                area_name,
                display_type,
                grunt,
                SUM(CASE WHEN confirmed = 0 THEN 1 ELSE 0 END) AS total_grunts
            FROM temp_store_grouped_invasion_sightings_{timezone_offset}
            GROUP BY display_type, grunt, area_name;

            DROP TEMPORARY TABLE IF EXISTS temp_store_grouped_invasion_sightings_{timezone_offset};
        END;
        """
        return drop_procedure_sql, create_procedure_sql

    async def generate_store_hourly_pokemon_tth_procedure(self):

        drop_procedure_sql = f"DROP PROCEDURE IF EXISTS store_hourly_pokemon_tth;"

        create_procedure_sql=f"""
        CREATE PROCEDURE store_hourly_pokemon_tth()
        BEGIN
            -- Create temporary table with adjusted times for the past hour in the local time zone of each area
            CREATE TEMPORARY TABLE IF NOT EXISTS temp_hourly_tth_stats AS
            SELECT ps.*
            FROM pokemon_sightings ps
            JOIN area_time_zones atz ON ps.area_name = atz.area_name
            WHERE ps.inserted_at >= UTC_TIMESTAMP() - INTERVAL 60 MINUTE + INTERVAL atz.time_zone_offset MINUTE
            AND ps.inserted_at < UTC_TIMESTAMP() + INTERVAL atz.time_zone_offset MINUTE;

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

            -- Drop temporary tables
            DROP TEMPORARY TABLE IF EXISTS temp_hourly_tth_stats;
            DROP TEMPORARY TABLE IF EXISTS temp_spawn_tth_by_area;
        END;
        """
        return drop_procedure_sql, create_procedure_sql

    # Update Pokemon Total Procedures

    async def generate_update_hourly_pokemon_total_procedure(self):

        drop_procedure_sql = f"DROP PROCEDURE IF EXISTS update_hourly_pokemon_total_stats;"

        create_procedure_sql=f"""
        CREATE PROCEDURE update_hourly_pokemon_total_stats()
        BEGIN
            CREATE TEMPORARY TABLE IF NOT EXISTS temp_hourly_pokemon_total_stats AS
            SELECT ps.*
            FROM pokemon_sightings ps
            JOIN area_time_zones atz ON ps.area_name = atz.area_name
            WHERE ps.inserted_at >= UTC_TIMESTAMP() - INTERVAL 60 MINUTE + INTERVAL atz.time_zone_offset MINUTE
            AND ps.inserted_at < UTC_TIMESTAMP() + INTERVAL atz.time_zone_offset MINUTE;

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
                FROM temp_hourly_pokemon_total_stats
                GROUP BY area_name
            ) t ON a.area_name = t.area_name;

            DROP TEMPORARY TABLE IF EXISTS temp_hourly_pokemon_total_stats;
            DROP TEMPORARY TABLE IF EXISTS all_area_names;
        END;
        """
        return drop_procedure_sql, create_procedure_sql

    async def generate_update_daily_pokemon_total_procedure(self, area_names, timezone_offset):
        procedure_name = f"update_daily_pokemon_total_stats_{timezone_offset}"
        area_names_str = ', '.join([f"'{name}'" for name in area_names])

        drop_procedure_sql = f"DROP PROCEDURE IF EXISTS {procedure_name};"

        create_procedure_sql=f"""
        CREATE PROCEDURE {procedure_name}()
        BEGIN
            REPLACE INTO daily_pokemon_total_stats (day, area_name, total, total_iv100, total_iv0, total_top1_little, total_top1_great, total_top1_ultra, total_shiny, avg_despawn)
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
            WHERE day = CURDATE() - INTERVAL 1 DAY AND area_name IN ({area_names_str});
        END;
        """
        return drop_procedure_sql, create_procedure_sql

    async def generate_update_total_pokemon_total_procedure(self, area_names, timezone_offset):
        procedure_name = f"update_total_pokemon_total_stats_{timezone_offset}"
        area_names_str = ', '.join([f"'{name}'" for name in area_names])

        drop_procedure_sql = f"DROP PROCEDURE IF EXISTS {procedure_name};"
        create_procedure_sql=f"""
        CREATE PROCEDURE {procedure_name}()
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
            WHERE d.day = CURDATE() - INTERVAL 1 DAY AND area_name IN ({area_names_str})
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
        """
        return drop_procedure_sql, create_procedure_sql

    # Update Pokemon Grouped Procedures

    async def generate_update_daily_pokemon_grouped_procedure(self, area_names, timezone_offset):
        procedure_name = f"update_daily_pokemon_grouped_stats_{timezone_offset}"
        area_names_str = ', '.join([f"'{name}'" for name in area_names])

        drop_procedure_sql = f"DROP PROCEDURE IF EXISTS {procedure_name};"
        create_procedure_sql=f"""
        CREATE PROCEDURE {procedure_name}()
        BEGIN
            REPLACE INTO daily_pokemon_grouped_stats (day, pokemon_id, form, avg_lat, avg_lon, total, total_iv100, total_iv0, total_top1_little, total_top1_great, total_top1_ultra, total_shiny, area_name, avg_despawn)
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
            WHERE area_name IN ({area_names_str}) AND day = CURDATE() - INTERVAL 1 DAY
            ORDER BY area_name, pokemon_id;
        END;
        """
        return drop_procedure_sql, create_procedure_sql

    async def generate_update_weekly_pokemon_grouped_procedure(self, area_names, timezone_offset):
        procedure_name = f"update_weekly_pokemon_grouped_stats_{timezone_offset}"
        area_names_str = ', '.join([f"'{name}'" for name in area_names])

        drop_procedure_sql = f"DROP PROCEDURE IF EXISTS {procedure_name};"
        create_procedure_sql=f"""
        CREATE PROCEDURE {procedure_name}()
        BEGIN
            REPLACE INTO weekly_pokemon_grouped_stats (day, pokemon_id, form, avg_lat, avg_lon, total, total_iv100, total_iv0, total_top1_little, total_top1_great, total_top1_ultra, total_shiny, area_name, avg_despawn)
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
            WHERE area_name IN ({area_names_str}) AND day >= DATE_SUB(CURDATE(), INTERVAL DAYOFWEEK(CURDATE()) + 6 DAY) AND day < DATE_SUB(CURDATE(), INTERVAL DAYOFWEEK(CURDATE()) - 1 DAY)
            GROUP BY pokemon_id, form, area_name
            ORDER BY area_name, pokemon_id;
        END;
        """
        return drop_procedure_sql, create_procedure_sql

    async def generate_udpate_monthly_pokemon_grouped_procedure(self, area_names, timezone_offset):
        procedure_name = f"update_monthly_pokemon_grouped_stats_{timezone_offset}"
        area_names_str = ', '.join([f"'{name}'" for name in area_names])

        drop_procedure_sql = f"DROP PROCEDURE IF EXISTS {procedure_name};"
        create_procedure_sql=f"""
        CREATE PROCEDURE {procedure_name}()
        BEGIN
            REPLACE INTO monthly_pokemon_grouped_stats(day, pokemon_id, form, avg_lat, avg_lon, total, total_iv100, total_iv0, total_top1_little, total_top1_great, total_top1_ultra, total_shiny, area_name, avg_despawn)
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
            WHERE area_name IN ({area_names_str}) AND day >= CURDATE() - INTERVAL 1 MONTH
            GROUP BY pokemon_id, form, area_name
            ORDER BY area_name, pokemon_id;
        END;
        """
        return drop_procedure_sql, create_procedure_sql

    # Update Quest Total Procedures

    async def generate_update_daily_quest_total_procedure(self, area_names, timezone_offset):
        procedure_name = f"update_daily_quest_total_stats_{timezone_offset}"
        area_names_str = ', '.join([f"'{name}'" for name in area_names])

        drop_procedure_sql = f"DROP PROCEDURE IF EXISTS {procedure_name};"
        create_procedure_sql=f"""
        CREATE PROCEDURE {procedure_name}()
        BEGIN
            REPLACE INTO daily_quest_total_stats (day, area_name, total_stops, ar, normal, scanned)
            SELECT
                DATE(NOW()) AS day,
                area_name,
                total_stops,
                ar,
                normal,
                scanned
            FROM storage_quest_total_stats
            WHERE area_name IN ({area_names_str}) AND day = CURDATE();
        END;
        """
        return drop_procedure_sql, create_procedure_sql

    async def generate_update_total_quest_total_procedure(self, area_names, timezone_offset):
        procedure_name = f"update_total_quest_total_stats_{timezone_offset}"
        area_names_str = ', '.join([f"'{name}'" for name in area_names])

        drop_procedure_sql = f"DROP PROCEDURE IF EXISTS {procedure_name};"
        create_procedure_sql=f"""
        CREATE PROCEDURE {procedure_name}()
        BEGIN
            INSERT INTO quest_total_stats (area_name, total_stops, ar, normal, scanned)
            SELECT
                d.area_name,
                (
                    SELECT tp.total_stops
                    FROM total_pokestops tp
                    WHERE tp.area_name = d.area_name
                    LIMIT 1
                ) AS total_stops,
                d.ar,
                d.normal,
                d.scanned
            FROM storage_quest_total_stats d
            WHERE area_name IN ({area_names_str}) AND d.day = CURDATE()
            ON DUPLICATE KEY UPDATE
                total_stops = VALUES(total_stops),
                ar = quest_total_stats.ar + d.ar,
                normal = quest_total_stats.normal + d.normal;
        END;
        """
        return drop_procedure_sql, create_procedure_sql

    # Update Quest Grouped Procedures

    async def generate_update_daily_quest_grouped_procedure(self, area_names, timezone_offset):
        procedure_name = f"update_daily_quest_grouped_stats_{timezone_offset}"
        area_names_str = ', '.join([f"'{name}'" for name in area_names])

        drop_procedure_sql = f"DROP PROCEDURE IF EXISTS {procedure_name};"
        create_procedure_sql=f"""
        CREATE PROCEDURE {procedure_name}()
        BEGIN
            REPLACE INTO daily_quest_grouped_stats (day, area_name, ar_type, normal_type, reward_ar_type, reward_normal_type, reward_ar_item_id, reward_ar_item_amount, reward_normal_item_id, reward_normal_item_amount, reward_ar_poke_id, reward_ar_poke_form, reward_normal_poke_id, reward_normal_poke_form, total, scanned)
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
            WHERE area_name IN ({area_names_str}) AND day = CURDATE()
            GROUP BY ar_type, reward_ar_type, normal_type, reward_normal_type, reward_ar_item_id, reward_ar_item_amount, reward_normal_item_id, reward_normal_item_amount, reward_ar_poke_id, reward_ar_poke_form, reward_normal_poke_id, reward_normal_poke_form, area_name, total, scanned
            ORDER BY area_name, ar_type, normal_type, reward_ar_item_id, reward_normal_item_id, reward_ar_poke_id, reward_normal_poke_id ASC;
        END;
        """
        return drop_procedure_sql, create_procedure_sql

    async def generate_update_weekly_quest_grouped_procedure(self, area_names, timezone_offset):
        procedure_name = f"update_weekly_quest_grouped_stats_{timezone_offset}"
        area_names_str = ', '.join([f"'{name}'" for name in area_names])

        drop_procedure_sql = f"DROP PROCEDURE IF EXISTS {procedure_name};"
        create_procedure_sql=f"""
        CREATE PROCEDURE {procedure_name}()
        BEGIN
            REPLACE INTO weekly_quest_grouped_stats (day, area_name, ar_type, normal_type, reward_ar_type, reward_normal_type, reward_ar_item_id, reward_ar_item_amount, reward_normal_item_id, reward_normal_item_amount, reward_ar_poke_id, reward_ar_poke_form, reward_normal_poke_id, reward_normal_poke_form, total, scanned)
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
                SUM(total) AS total,
                scanned
            FROM storage_quest_grouped_stats
            WHERE area_name IN ({area_names_str}) AND day >= DATE_SUB(CURDATE(), INTERVAL DAYOFWEEK(CURDATE()) + 6 DAY) AND day < DATE_SUB(CURDATE(), INTERVAL DAYOFWEEK(CURDATE()) - 1 DAY)
            GROUP BY ar_type, reward_ar_type, normal_type, reward_normal_type, reward_ar_item_id, reward_ar_item_amount, reward_normal_item_id, reward_normal_item_amount, reward_ar_poke_id, reward_ar_poke_form, reward_normal_poke_id, reward_normal_poke_form, area_name, scanned
            ORDER BY area_name, ar_type, normal_type, reward_ar_item_id, reward_normal_item_id, reward_ar_poke_id, reward_normal_poke_id ASC;
        END;
        """
        return drop_procedure_sql, create_procedure_sql

    async def generate_update_monthly_quest_grouped_procedure(self, area_names, timezone_offset):
        procedure_name = f"update_monthly_quest_grouped_stats_{timezone_offset}"
        area_names_str = ', '.join([f"'{name}'" for name in area_names])

        drop_procedure_sql = f"DROP PROCEDURE IF EXISTS {procedure_name};"
        create_procedure_sql=f"""
        CREATE PROCEDURE {procedure_name}()
        BEGIN
            REPLACE INTO monthly_quest_grouped_stats (day, area_name, ar_type, normal_type, reward_ar_type, reward_normal_type, reward_ar_item_id, reward_ar_item_amount, reward_normal_item_id, reward_normal_item_amount, reward_ar_poke_id, reward_ar_poke_form, reward_normal_poke_id, reward_normal_poke_form, total, scanned)
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
                SUM(total) AS total,
                scanned
            FROM storage_quest_grouped_stats
            WHERE area_name IN ({area_names_str}) AND day >= CURDATE() - INTERVAL 1 MONTH
            GROUP BY ar_type, reward_ar_type, normal_type, reward_normal_type, reward_ar_item_id, reward_ar_item_amount, reward_normal_item_id, reward_normal_item_amount, reward_ar_poke_id, reward_ar_poke_form, reward_normal_poke_id, reward_normal_poke_form, area_name, scanned
            ORDER BY area_name, ar_type, normal_type, reward_ar_item_id, reward_normal_item_id, reward_ar_poke_id, reward_normal_poke_id ASC;
        END;
        """
        return drop_procedure_sql, create_procedure_sql

    # Update Raid Total Procedures

    async def generate_update_hourly_raid_total_procedure(self):
        procedure_name = f"update_hourly_raid_total_stats"

        drop_procedure_sql = f"DROP PROCEDURE IF EXISTS {procedure_name};"
        create_procedure_sql=f"""
        CREATE PROCEDURE {procedure_name}()
        BEGIN
            DROP TEMPORARY TABLE IF EXISTS temp_hourly_raid_total_stats;
            CREATE TEMPORARY TABLE IF NOT EXISTS temp_hourly_raid_total_stats AS
            SELECT rs.*
            FROM raid_sightings rs
            JOIN area_time_zones atz ON rs.area_name = atz.area_name
            WHERE rs.inserted_at >= UTC_TIMESTAMP() - INTERVAL 60 MINUTE + INTERVAL atz.time_zone_offset MINUTE
            AND rs.inserted_at < UTC_TIMESTAMP() + INTERVAL atz.time_zone_offset MINUTE;

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
        """
        return drop_procedure_sql, create_procedure_sql

    async def generate_update_daily_raid_total_procedure(self, area_names, timezone_offset):
        procedure_name = f"update_daily_raid_total_stats_{timezone_offset}"
        area_names_str = ', '.join([f"'{name}'" for name in area_names])

        drop_procedure_sql = f"DROP PROCEDURE IF EXISTS {procedure_name};"
        create_procedure_sql=f"""
        CREATE PROCEDURE {procedure_name}()
        BEGIN
            REPLACE INTO daily_raid_total_stats (day, area_name, total, total_ex_raid, total_exclusive)
            SELECT
                CURDATE() - INTERVAL 1 DAY AS day,
                area_name,
                total,
                total_ex_raid,
                total_exclusive
            FROM storage_raid_total_stats
            WHERE area_name IN ({area_names_str}) AND day = CURDATE() - INTERVAL 1 DAY;
        END;
        """
        return drop_procedure_sql, create_procedure_sql

    async def generate_update_total_raid_total_procedure(self, area_names, timezone_offset):
        procedure_name = f"update_total_raid_total_stats_{timezone_offset}"
        area_names_str = ', '.join([f"'{name}'" for name in area_names])

        drop_procedure_sql = f"DROP PROCEDURE IF EXISTS {procedure_name};"
        create_procedure_sql=f"""
        CREATE PROCEDURE {procedure_name}()
        BEGIN
            INSERT INTO raid_total_stats (area_name, total, total_ex_raid, total_exclusive)
            SELECT
                d.area_name,
                d.total,
                d.total_ex_raid,
                d.total_exclusive
            FROM storage_raid_total_stats d
            WHERE area_name IN ({area_names_str}) AND d.day = CURDATE() - INTERVAL 1 DAY
            ON DUPLICATE KEY UPDATE
                total = raid_total_stats.total + d.total,
                total_ex_raid = raid_total_stats.total_ex_raid + d.total_ex_raid,
                total_exclusive = raid_total_stats.total_exclusive + d.total_exclusive;
        END;
        """
        return drop_procedure_sql, create_procedure_sql


    # Update Raid Grouped Procedures

    async def generate_update_daily_raid_grouped_procedure(self, area_names, timezone_offset):
        procedure_name = f"update_daily_raid_grouped_stats_{timezone_offset}"
        area_names_str = ', '.join([f"'{name}'" for name in area_names])

        drop_procedure_sql = f"DROP PROCEDURE IF EXISTS {procedure_name};"
        create_procedure_sql=f"""
        CREATE PROCEDURE {procedure_name}()
        BEGIN
            REPLACE INTO daily_raid_grouped_stats (day, area_name, level, pokemon_id, form, costume, ex_raid_eligible, is_exclusive, total)
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
            WHERE area_name IN ({area_names_str}) AND day = CURDATE() - INTERVAL 1 DAY
            GROUP BY level, pokemon_id, form, costume, ex_raid_eligible, is_exclusive, area_name, total
            ORDER BY area_name, level, pokemon_id ASC;
        END;
        """
        return drop_procedure_sql, create_procedure_sql

    async def generate_update_weekly_raid_grouped_procedure(self, area_names, timezone_offset):
        procedure_name = f"update_weekly_raid_grouped_stats_{timezone_offset}"
        area_names_str = ', '.join([f"'{name}'" for name in area_names])

        drop_procedure_sql = f"DROP PROCEDURE IF EXISTS {procedure_name};"
        create_procedure_sql=f"""
        CREATE PROCEDURE {procedure_name}()
        BEGIN
            REPLACE INTO weekly_raid_grouped_stats (day, area_name, level, pokemon_id, form, costume, ex_raid_eligible, is_exclusive, total)
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
            WHERE area_name IN ({area_names_str}) AND day >= DATE_SUB(CURDATE(), INTERVAL DAYOFWEEK(CURDATE()) + 6 DAY) AND day < DATE_SUB(CURDATE(), INTERVAL DAYOFWEEK(CURDATE()) - 1 DAY)
            GROUP BY level, pokemon_id, form, costume, ex_raid_eligible, is_exclusive, area_name
            ORDER BY area_name, level, pokemon_id ASC;
        END;
        """
        return drop_procedure_sql, create_procedure_sql

    async def generate_update_monthly_raid_grouped_procedure(self, area_names, timezone_offset):
        procedure_name = f"update_monthly_raid_grouped_stats_{timezone_offset}"
        area_names_str = ', '.join([f"'{name}'" for name in area_names])

        drop_procedure_sql = f"DROP PROCEDURE IF EXISTS {procedure_name};"
        create_procedure_sql=f"""
        CREATE PROCEDURE {procedure_name}()
        BEGIN
            REPLACE INTO monthly_raid_grouped_stats (day, area_name, level, pokemon_id, form, costume, ex_raid_eligible, is_exclusive, total)
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
            WHERE area_name IN ({area_names_str}) AND day >= CURDATE() - INTERVAL 1 MONTH
            GROUP BY level, pokemon_id, form, costume, ex_raid_eligible, is_exclusive, area_name
            ORDER BY area_name, level, pokemon_id ASC;
        END;
        """
        return drop_procedure_sql, create_procedure_sql

    # Update Invasion Total Procedures

    async def generate_update_hourly_invasion_total_procedure(self):
        procedure_name = f"update_hourly_invasion_total_stats"

        drop_procedure_sql = f"DROP PROCEDURE IF EXISTS {procedure_name};"
        create_procedure_sql=f"""
        CREATE PROCEDURE {procedure_name}()
        BEGIN
            DROP TEMPORARY TABLE IF EXISTS temp_hourly_invasion_total_stats;
            CREATE TEMPORARY TABLE temp_hourly_invasion_total_stats AS
            SELECT inv.*
            FROM invasion_sightings inv
            JOIN area_time_zones atz ON inv.area_name = atz.area_name
            WHERE inv.inserted_at >= UTC_TIMESTAMP() - INTERVAL 60 MINUTE + INTERVAL atz.time_zone_offset MINUTE
            AND inv.inserted_at < UTC_TIMESTAMP() + INTERVAL atz.time_zone_offset MINUTE;

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
        """
        return drop_procedure_sql, create_procedure_sql

    async def generate_update_daily_invasion_total_procedure(self, area_names, timezone_offset):
        procedure_name = f"update_daily_invasion_total_stats_{timezone_offset}"
        area_names_str = ', '.join([f"'{name}'" for name in area_names])

        drop_procedure_sql = f"DROP PROCEDURE IF EXISTS {procedure_name};"
        create_procedure_sql=f"""
        CREATE PROCEDURE {procedure_name}()
        BEGIN
            REPLACE INTO daily_invasion_total_stats (day, area_name, total_grunts, total_confirmed, total_unconfirmed)
            SELECT
                CURDATE() - INTERVAL 1 DAY AS day,
                area_name,
                total_grunts,
                total_confirmed,
                total_unconfirmed
            FROM storage_invasion_total_stats
            WHERE area_name IN ({area_names_str}) AND day = CURDATE() - INTERVAL 1 DAY;
        END;
        """
        return drop_procedure_sql, create_procedure_sql

    async def generate_update_total_invasion_total_procedure(self, area_names, timezone_offset):
        procedure_name = f"update_total_invasion_total_stats_{timezone_offset}"
        area_names_str = ', '.join([f"'{name}'" for name in area_names])

        drop_procedure_sql = f"DROP PROCEDURE IF EXISTS {procedure_name};"
        create_procedure_sql=f"""
        CREATE PROCEDURE {procedure_name}()
        BEGIN
            INSERT INTO invasion_total_stats (area_name, total_grunts, total_confirmed, total_unconfirmed)
            SELECT
                d.area_name,
                d.total_grunts,
                d.total_confirmed,
                d.total_unconfirmed
            FROM storage_invasion_total_stats d
            WHERE area_name IN ({area_names_str}) AND d.day = CURDATE() - INTERVAL 1 DAY
            ON DUPLICATE KEY UPDATE
                total_grunts = invasion_total_stats.total_grunts + d.total_grunts,
                total_confirmed = invasion_total_stats.total_confirmed + d.total_confirmed,
                total_unconfirmed = invasion_total_stats.total_unconfirmed + d.total_unconfirmed;
        END;
        """
        return drop_procedure_sql, create_procedure_sql

    # Update Invasion Grouped Procedures

    async def generate_update_daily_invasion_grouped_procedure(self, area_names, timezone_offset):
        procedure_name = f"update_daily_invasion_grouped_stats_{timezone_offset}"
        area_names_str = ', '.join([f"'{name}'" for name in area_names])

        drop_procedure_sql = f"DROP PROCEDURE IF EXISTS {procedure_name};"
        create_procedure_sql=f"""
        CREATE PROCEDURE {procedure_name}()
        BEGIN
            REPLACE INTO daily_invasion_grouped_stats (day, area_name, display_type, grunt, total_grunts)
            SELECT
                CURDATE() - INTERVAL 1 DAY as day,
                area_name,
                display_type,
                grunt,
                total_grunts
            FROM storage_invasion_grouped_stats
            WHERE area_name IN ({area_names_str}) AND day = CURDATE() - INTERVAL 1 DAY
            GROUP BY display_type, grunt, area_name, total_grunts
            ORDER BY area_name, display_type, grunt ASC;
        END;
        """
        return drop_procedure_sql, create_procedure_sql

    async def generate_update_weekly_invasion_grouped_procedure(self, area_names, timezone_offset):
        procedure_name = f"update_weekly_invasion_grouped_stats_{timezone_offset}"
        area_names_str = ', '.join([f"'{name}'" for name in area_names])

        drop_procedure_sql = f"DROP PROCEDURE IF EXISTS {procedure_name};"
        create_procedure_sql=f"""
        CREATE PROCEDURE {procedure_name}()
        BEGIN
            REPLACE INTO weekly_invasion_grouped_stats (day, area_name, display_type, grunt, total_grunts)
            SELECT
                DATE_SUB(CURDATE(), INTERVAL DAYOFWEEK(CURDATE()) - 1 DAY) as day,
                area_name,
                display_type,
                grunt,
                SUM(total_grunts) as total_grunts
            FROM storage_invasion_grouped_stats
            WHERE area_name IN ({area_names_str}) AND day >= DATE_SUB(CURDATE(), INTERVAL DAYOFWEEK(CURDATE()) + 6 DAY) AND day < DATE_SUB(CURDATE(), INTERVAL DAYOFWEEK(CURDATE()) - 1 DAY)
            GROUP BY display_type, grunt, area_name
            ORDER BY area_name, display_type, grunt ASC;
        END;
        """
        return drop_procedure_sql, create_procedure_sql

    async def generate_update_monthly_invasion_grouped_procedure(self, area_names, timezone_offset):
        procedure_name = f"update_monthly_invasion_grouped_stats_{timezone_offset}"
        area_names_str = ', '.join([f"'{name}'" for name in area_names])

        drop_procedure_sql = f"DROP PROCEDURE IF EXISTS {procedure_name};"
        create_procedure_sql=f"""
        CREATE PROCEDURE {procedure_name}()
        BEGIN
            REPLACE INTO monthly_invasion_grouped_stats (day, area_name, display_type, grunt, total_grunts)
            SELECT
                LAST_DAY(CURDATE() - INTERVAL 1 MONTH) as day,
                area_name,
                display_type,
                grunt,
                SUM(total_grunts) as total_grunts
            FROM storage_invasion_grouped_stats
            WHERE area_name IN ({area_names_str}) AND day >= CURDATE() - INTERVAL 1 MONTH
            GROUP BY display_type, grunt, area_name
            ORDER BY area_name, display_type, grunt ASC;
        END;
        """
        return drop_procedure_sql, create_procedure_sql

    # Update Surge Procedures

    async def generate_update_hourly_surge_procedure(self):
        procedure_name = f"update_hourly_surge_stats"

        drop_procedure_sql = f"DROP PROCEDURE IF EXISTS {procedure_name};"
        create_procedure_sql=f"""
        CREATE PROCEDURE {procedure_name}()
        BEGIN
            DROP TEMPORARY TABLE IF EXISTS temp_hourly_surge_stats;
            CREATE TEMPORARY TABLE IF NOT EXISTS temp_hourly_surge_stats AS
            SELECT ps.*
            FROM pokemon_sightings ps
            JOIN area_time_zones atz ON ps.area_name = atz.area_name
            WHERE ps.inserted_at >= UTC_TIMESTAMP() - INTERVAL 60 MINUTE + INTERVAL atz.time_zone_offset MINUTE
            AND ps.inserted_at < UTC_TIMESTAMP() + INTERVAL atz.time_zone_offset MINUTE;

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
        END;
        """
        return drop_procedure_sql, create_procedure_sql

    async def generate_update_daily_surge_procedure(self):
        procedure_name = f"update_daily_surge_stats"

        drop_procedure_sql = f"DROP PROCEDURE IF EXISTS {procedure_name};"
        create_procedure_sql=f"""
        CREATE PROCEDURE {procedure_name}()
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

            REPLACE INTO daily_surge_pokemon_stats (hour, total_iv100, total_iv0, total_top1_little, total_top1_great, total_top1_ultra, total_shiny)
            SELECT
                hour_of_day,
                sum_total_iv100,
                sum_total_iv0,
                sum_total_top1_little,
                sum_total_top1_great,
                sum_total_top1_ultra,
                sum_total_shiny
            FROM temp_daily_surge;

            DROP TEMPORARY TABLE IF EXISTS temp_daily_surge_;
        END;
        """
        return drop_procedure_sql, create_procedure_sql

    async def generate_update_weekly_surge_procedure(self):
        procedure_name = f"update_weekly_surge_stats"

        drop_procedure_sql = f"DROP PROCEDURE IF EXISTS {procedure_name};"
        create_procedure_sql=f"""
        CREATE PROCEDURE {procedure_name}()
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

            REPLACE INTO weekly_surge_pokemon_stats (hour, total_iv100, total_iv0, total_top1_little, total_top1_great, total_top1_ultra, total_shiny)
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
        END;
        """
        return drop_procedure_sql, create_procedure_sql

    async def generate_update_monthly_surge_procedure(self):
        procedure_name = f"update_monthly_surge_stats"

        drop_procedure_sql = f"DROP PROCEDURE IF EXISTS {procedure_name};"
        create_procedure_sql=f"""
        CREATE PROCEDURE {procedure_name}()
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

            REPLACE INTO monthly_surge_pokemon_stats (hour, total_iv100, total_iv0, total_top1_little, total_top1_great, total_top1_ultra, total_shiny)
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
        """
        return drop_procedure_sql, create_procedure_sql

    # Update TTH Procedures

    async def generate_update_hourly_pokemon_tth_procedure(self):
        procedure_name = f"update_hourly_pokemon_tth_stats"

        drop_procedure_sql = f"DROP PROCEDURE IF EXISTS {procedure_name};"
        create_procedure_sql=f"""
        CREATE PROCEDURE {procedure_name}()
        BEGIN
            REPLACE INTO hourly_pokemon_tth_stats (area_name, tth_5, tth_10, tth_15, tth_20, tth_25, tth_30, tth_35, tth_40, tth_45, tth_50, tth_55, tth_55_plus)
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
        """
        return drop_procedure_sql, create_procedure_sql

    async def generate_update_daily_pokemon_tth_procedure(self, area_names, timezone_offset):
        procedure_name = f"update_daily_pokemon_tth_stats_{timezone_offset}"
        area_names_str = ', '.join([f"'{name}'" for name in area_names])

        drop_procedure_sql = f"DROP PROCEDURE IF EXISTS {procedure_name};"
        create_procedure_sql=f"""
        CREATE PROCEDURE {procedure_name}()
        BEGIN
            DROP TEMPORARY TABLE IF EXISTS temp_daily_pokemon_tth_{timezone_offset};
            CREATE TEMPORARY TABLE IF NOT EXISTS temp_daily_pokemon_tth_{timezone_offset} AS
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
            WHERE area_name IN ({area_names_str}) AND DATE(`day_hour`) = CURDATE() - INTERVAL 1 DAY
            GROUP BY hour_of_day, area_name;

            REPLACE INTO daily_pokemon_tth_stats (hour, area_name, total_tth_5, total_tth_10, total_tth_15, total_tth_20, total_tth_25, total_tth_30, total_tth_35, total_tth_40, total_tth_45, total_tth_50, total_tth_55, total_tth_55_plus)
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
            FROM temp_daily_pokemon_tth_{timezone_offset};
            DROP TEMPORARY TABLE IF EXISTS temp_daily_pokemon_tth_{timezone_offset};
        END;
        """
        return drop_procedure_sql, create_procedure_sql

    # Cleaning Procedures

    async def generate_clean_pokemon_sightings_procedure(self, area_names, timezone_offset):
        procedure_name = f"delete_pokemon_sightings_batches_{timezone_offset}"
        area_names_str = ', '.join([f"'{name}'" for name in area_names])

        drop_procedure_sql = f"DROP PROCEDURE IF EXISTS {procedure_name};"
        create_procedure_sql=f"""
        CREATE PROCEDURE {procedure_name}()
        BEGIN
            DECLARE done INT DEFAULT FALSE;
            DECLARE CONTINUE HANDLER FOR NOT FOUND SET done = TRUE;

            WHILE NOT done DO
                DELETE FROM pokemon_sightings
                WHERE area_name IN ({area_names_str}) AND inserted_at < CURDATE() - INTERVAL 1 DAY
                LIMIT 20000;

                IF (ROW_COUNT() = 0) THEN
                SET done = TRUE;
                END IF;
            END WHILE;
        END;
        """
        return drop_procedure_sql, create_procedure_sql

    async def generate_clean_quest_sightings_procedure(self, area_names, timezone_offset):
        procedure_name = f"delete_quest_sightings_batches_{timezone_offset}"
        area_names_str = ', '.join([f"'{name}'" for name in area_names])

        drop_procedure_sql = f"DROP PROCEDURE IF EXISTS {procedure_name};"
        create_procedure_sql=f"""
        CREATE PROCEDURE {procedure_name}()
        BEGIN
            DECLARE done INT DEFAULT FALSE;
            DECLARE CONTINUE HANDLER FOR NOT FOUND SET done = TRUE;

            WHILE NOT done DO
                DELETE FROM quest_sightings
                WHERE area_name IN ({area_names_str}) AND inserted_at < CURDATE() - INTERVAL 1 DAY
                LIMIT 20000;

                IF (ROW_COUNT() = 0) THEN
                SET done = TRUE;
                END IF;
            END WHILE;
        END;
        """
        return drop_procedure_sql, create_procedure_sql

    async def generate_clean_raid_sightings_procedure(self, area_names, timezone_offset):
        procedure_name = f"delete_raid_sightings_batches_{timezone_offset}"
        area_names_str = ', '.join([f"'{name}'" for name in area_names])

        drop_procedure_sql = f"DROP PROCEDURE IF EXISTS {procedure_name};"
        create_procedure_sql=f"""
        CREATE PROCEDURE {procedure_name}()
        BEGIN
            DECLARE done INT DEFAULT FALSE;
            DECLARE CONTINUE HANDLER FOR NOT FOUND SET done = TRUE;

            WHILE NOT done DO
                DELETE FROM raid_sightings
                WHERE area_name IN ({area_names_str}) AND inserted_at < CURDATE() - INTERVAL 1 DAY
                LIMIT 20000;

                IF (ROW_COUNT() = 0) THEN
                SET done = TRUE;
                END IF;
            END WHILE;
        END;
        """
        return drop_procedure_sql, create_procedure_sql

    async def generate_clean_invasion_sightigns_procedure(self, area_names, timezone_offset):
        procedure_name = f"delete_invasion_sightings_batches_{timezone_offset}"
        area_names_str = ', '.join([f"'{name}'" for name in area_names])

        drop_procedure_sql = f"DROP PROCEDURE IF EXISTS {procedure_name};"
        create_procedure_sql=f"""
        CREATE PROCEDURE {procedure_name}()
        BEGIN
            DECLARE done INT DEFAULT FALSE;
            DECLARE CONTINUE HANDLER FOR NOT FOUND SET done = TRUE;

            WHILE NOT done DO
                DELETE FROM invasion_sightings
                WHERE area_name IN ({area_names_str}) AND inserted_at < CURDATE() - INTERVAL 1 DAY
                LIMIT 20000;

                IF (ROW_COUNT() = 0) THEN
                SET done = TRUE;
                END IF;
            END WHILE;
        END;
        """
        return drop_procedure_sql, create_procedure_sql
