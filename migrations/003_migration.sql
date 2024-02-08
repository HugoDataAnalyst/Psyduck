-- Simple migration to call the dynamic hourly updates
CALL CreateOrUpdateHourlySurgeEvent();

CALL CreateOrUpdateHourlyTotalEvent();