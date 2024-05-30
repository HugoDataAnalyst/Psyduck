from config.app_config import app_config
from datetime import datetime, timedelta
from orm.queries import DatabaseOperations

class EventGenerator:
    def __init__(self):
        self.db_ops = DatabaseOperations()
        self.db_timezone_offset = app_config.db_timezone_offset

    async def get_unique_timezones(self):
        timezones = await self.db_ops.get_unique_timezones()
        return [{'time_zone_offset': tz} for tz in timezones]

    def generate_event_daily_sql(self, procedure_name, timezone_offset):
        offset_diff = timezone_offset - self.db_timezone_offset
        event_name = f"event_{procedure_name}_{timezone_offset}"
        # Get the current UTC time and adjust it to the database's local time
        current_time_utc = datetime.utcnow()
        current_time_db = current_time_utc + timedelta(minutes=self.db_timezone_offset)


        # Set the event to start at 1 AM in the database's local time, adjusted by the timezone offset
        event_time_db = current_time_db.replace(hour=1, minute=0, second=0, microsecond=0) + timedelta(days=1)
        adjusted_event_time = event_time_db + timedelta(minutes=offset_diff)

        drop_event_sql = f"DROP EVENT IF EXISTS {event_name};"

        create_event_sql = f"""
        CREATE EVENT IF NOT EXISTS {event_name}
        ON SCHEDULE EVERY 1 DAY
        STARTS '{adjusted_event_time.strftime('%Y-%m-%d %H:%M:%S')}'
        DO
        BEGIN
        CALL {procedure_name};
        END;
        """
        return drop_event_sql, create_event_sql

    async def create_events(self):
        unique_timezones = await self.get_unique_timezones()
        procedure_names = [
            "store_pokemon_total",
            "store_pokemon_grouped",
            "store_quest_total",
            "store_quest_grouped",
            "store_raid_total",
            "store_raid_grouped",
            "store_invasion_total",
            "store_invasion_grouped",
            "store_hourly_pokemon_tth"
        ]
        for timezone in unique_timezones:
            offset = timezone['time_zone_offset']
            for procedure in procedure_names:
                drop_event_sql, create_event_sql = self.generate_event_daily_sql(f"{procedure}_{abs(offset)}", offset)
                await self.db_ops.execute_sql(drop_event_sql)
                await self.db_ops.execute_sql(create_event_sql)
                print(f"Event for {procedure} with offset {offset} created/updated.")

