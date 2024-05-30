from config.app_config import app_config
from datetime import datetime, timedelta
from tortoise import Tortoise, run_async
from orm.queries import DatabaseOperations

class EventGenerator:
    def __init__(self):
        self.db_ops = DatabaseOperations()
        self.db_timezone_offset = app_config.db_timezone_offset

    async def get_unique_timezones(self):
        return await self.db_ops.get_unique_timezones()

    def generate_event_daily_sql(self, timezone_offset):
        offset_diff = timezone_offset - self.db_timezone_offset
        event_name = f"run_procedures_{abs(offset_diff)}"
        procedure_names = [
            f"store_pokemon_total_{abs(offset_diff)}",
            f"store_pokemon_grouped_{abs(offset_diff)}",
            f"store_quest_total_{abs(offset_diff)}",
            f"store_quest_grouped_{abs(offset_diff)}",
            f"store_raid_total_{abs(offset_diff)}",
            f"store_raid_grouped_{abs(offset_diff)}",
            f"store_invasion_total_{abs(offset_diff)}",
            f"store_invasion_grouped_{abs(offset_diff)}",
            "store_hourly_pokemon_tth"
        ]
        # Get the current UTC time and adjust it to the database's local time
        current_time_utc = datetime.utcnow()
        current_time_db = current_time_utc + timedelta(minutes=self.db_timezone_offset)


        # Set the event to start at 1 AM in the database's local time, adjusted by the timezone offset
        event_time_db = current_time_db.replace(hour=1, minute=0, second=0, microsecond=0) + timedelta(days=1)
        adjusted_event_time = event_time_db + timedelta(minutes=offset_diff)

        sql = f"""
        CREATE OR REPLACE EVENT {event_name}
        ON SCHEDULE EVERY 1 DAY
        STARTS '{adjusted_event_time.strftime('%Y-%m-%d %H:%M:%S')}'
        DO
        BEGIN
        {"; ".join([f"CALL {procedure}" for procedure in procedure_names])};
        END;
        """
        return sql

    async def create_events(self):
        unique_timezones = await self.get_unique_timezones()
        async with self.db_ops.get_connection() as connection:
            async with connection.cursor() as cursor:
                for timezone in unique_timezones:
                    offset = timezone['time_zone_offset']
                    event_sql = self.generate_event_daily_sql(offset)
                    await cursor.execute(event_sql)
                    print(f"Event for offset {offset} created/updated.")
                await connection.commit()

