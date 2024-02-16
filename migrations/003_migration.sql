-- Raw quests data
CREATE TABLE IF NOT EXISTS quest_sightings (
	id INTEGER PRIMARY KEY AUTO_INCREMENT,
	pokestop_id VARCHAR(255),
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
	inserted_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
	area_name VARCHAR(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci,
	INDEX idx_inserted_at (inserted_at),
	INDEX idx_area_name (area_name),
	INDEX idx_reward_ar_type (reward_ar_type),
	INDEX idx_reward_normal_type (reward_normal_type),
	INDEX idx_reward_area_name_inserted (area_name, inserted_at, reward_ar_type, reward_normal_type),
	INDEX idx_reward_area_name (area_name, reward_ar_type, reward_normal_type),
	INDEX idx_reward_inserted (inserted_at, reward_ar_type, reward_normal_type),
	INDEX idx_rewards (reward_ar_type, reward_normal_type)
) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;

-- Raw raid data
CREATE TABLE IF NOT EXISTS raid_sightings (
	id INTEGER PRIMARY KEY AUTO_INCREMENT,
	gym_id VARCHAR(255),
	ex_raid_eligible TINYINT,
	is_exclusive TINYINT,
	level TINYINT,
	pokemon_id SMALLINT,
	form VARCHAR(15) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci,
	costume VARCHAR(50) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci,
	inserted_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
	area_name VARCHAR(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci,
	INDEX idx_inserted_at (inserted_at),
	INDEX idx_area_name (area_name),
	INDEX idx_level (level),
	INDEX idx_pokemon_id (pokemon_id),
	INDEX idx_all (area_name, inserted_at, pokemon_id, form, level)
) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;

-- Raw invasion data
CREATE TABLE IF NOT EXISTS invasion_sightings (
	id INTEGER PRIMARY KEY AUTO_INCREMENT,
	pokestop_id VARCHAR(255),
	display_type TINYINT,
	grunt TINYINT,
	confirmed BOOLEAN,
	inserted_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
	area_name VARCHAR(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci,
	INDEX idx_display (display_type),
	INDEX idx_grunt (grunt),
	INDEX idx_confirmed (confirmed),
	INDEX idx_all (area_name, inserted_at, grunt, confirmed),
	INDEX idx_area_insert_grunt (area_name, inserted_at, grunt),
	INDEX idx_area_inserted_confirmed (area_name, inserted_at, confirmed)
) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;