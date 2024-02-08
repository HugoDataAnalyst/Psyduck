-- Simple migration to call the dynamic hourly updates
CREATE EVENT IF NOT EXISTS call_CreateOrUpdateHourlySurgeEvent
ON SCHEDULE AT CURRENT_TIMESTAMP
ON COMPLETION PRESERVE
DO
BEGIN
  CALL CreateOrUpdateHourlySurgeEvent();
END;

CREATE EVENT IF NOT EXISTS call_CreateOrUpdateHourlyTotalEvent
ON SCHEDULE AT CURRENT_TIMESTAMP
ON COMPLETION PRESERVE
DO
BEGIN
  CALL CreateOrUpdateHourlyTotalEvent();
END;

DROP EVENT IF EXISTS call_CreateOrUpdateHourlySurgeEvent;
DROP EVENT IF EXISTS call_CreateOrUpdateHourlyTotalEvent;

DROP PROCEDURE IF EXISTS CreateOrUpdateHourlySurgeEvent;
DROP PROCEDURE IF EXISTS CreateOrUpdateHourlyTotalEvent;