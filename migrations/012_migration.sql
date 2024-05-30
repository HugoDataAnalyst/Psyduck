-- Major rework for all procedures in order to have dynamic timezone offsets based in the areas.
DROP PROCEDURE IF EXISTS store_invasion_grouped_stats;
DROP PROCEDURE IF EXISTS store_invasion_total_stats;
DROP PROCEDURE IF EXISTS store_pokemon_grouped_stats;
DROP PROCEDURE IF EXISTS store_pokemon_total_stats;
DROP PROCEDURE IF EXISTS store_quest_grouped_stats;
DROP PROCEDURE IF EXISTS store_quest_total_stats;
DROP PROCEDURE IF EXISTS store_raid_grouped_stats;
DROP PROCEDURE IF EXISTS store_raid_total_stats;
DROP PROCEDURE IF EXISTS store_storage_hourly_pokemon_tth_stats;

DROP EVENT IF EXISTS store_storage_invasion_grouped;
DROP EVENT IF EXISTS store_storage_invasion_total;
DROP EVENT IF EXISTS store_storage_pokemon_grouped_stats;
DROP EVENT IF EXISTS store_storage_pokemon_total_stats;
DROP EVENT IF EXISTS store_storage_quest_grouped;
DROP EVENT IF EXISTS store_storage_quest_total;
DROP EVENT IF EXISTS store_storage_raid_grouped;
DROP EVENT IF EXISTS store_storage_raid_total;
DROP EVENT IF EXISTS event_store_hourly_pokemon_tth_stats;
