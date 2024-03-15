ALTER TABLE quest_total_stats
ADD COLUMN total_stops INTEGER DEFAULT 0 NOT NULL;

DROP PROCEDURE IF EXISTS update_total_quest_total_stats;
CREATE PROCEDURE update_total_quest_total_stats()
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
    WHERE d.day = CURDATE()
    ON DUPLICATE KEY UPDATE
        total_stops = VALUES(total_stops),
        ar = quest_total_stats.ar + d.ar,
        normal = quest_total_stats.normal + d.normal;
END;

