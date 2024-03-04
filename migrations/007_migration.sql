-- Improve cleaning procedure
-- Pokemon Section
DROP PROCEDURE IF EXISTS delete_pokemon_sightings_batches;
CREATE PROCEDURE delete_pokemon_sightings_batches()
BEGIN
  DECLARE done INT DEFAULT FALSE;
  DECLARE CONTINUE HANDLER FOR NOT FOUND SET done = TRUE;

  WHILE NOT done DO
    DELETE FROM pokemon_sightings
    WHERE inserted_at >= CONCAT(CURDATE() - INTERVAL 1 DAY, ' 21:00:00')
    LIMIT 20000;

    IF (ROW_COUNT() = 0) THEN
      SET done = TRUE;
    END IF;
  END WHILE;
END;

-- Raid Section
DROP PROCEDURE IF EXISTS delete_raid_sightings_batches;
CREATE PROCEDURE delete_raid_sightings_batches()
BEGIN
  DECLARE done INT DEFAULT FALSE;
  DECLARE CONTINUE HANDLER FOR NOT FOUND SET done = TRUE;

  WHILE NOT done DO
    DELETE FROM raid_sightings
    WHERE inserted_at >= CONCAT(CURDATE() - INTERVAL 1 DAY, ' 21:00:00')
    LIMIT 20000;

    IF (ROW_COUNT() = 0) THEN
      SET done = TRUE;
    END IF;
  END WHILE;
END;

-- Invasion Section
DROP PROCEDURE IF EXISTS delete_invasion_sightings_batches;
CREATE PROCEDURE delete_invasion_sightings_batches()
BEGIN
  DECLARE done INT DEFAULT FALSE;
  DECLARE CONTINUE HANDLER FOR NOT FOUND SET done = TRUE;

  WHILE NOT done DO
    DELETE FROM invasion_sightings
    WHERE inserted_at >= CONCAT(CURDATE() - INTERVAL 1 DAY, ' 21:00:00')
    LIMIT 20000;

    IF (ROW_COUNT() = 0) THEN
      SET done = TRUE;
    END IF;
  END WHILE;
END;