from config.app_config import app_config
from dateutil.relativedelta import relativedelta
from datetime import datetime, timedelta
from orm.queries import DatabaseOperations

class EventGenerator:
    def __init__(self):
        self.db_ops = DatabaseOperations()
        self.db_timezone_offset = app_config.db_timezone_offset

    async def get_unique_timezones(self):
        timezones = await self.db_ops.get_unique_timezones()
        return [{'time_zone_offset': tz} for tz in timezones]

    def generate_store_event_daily_sql(self, procedure_name, timezone_offset):
        # Determine offset_diff based on the sign of timezone_offset
        if timezone_offset >= 0:
            offset_diff = timezone_offset - self.db_timezone_offset
        else:
            offset_diff = self.db_timezone_offset - timezone_offset

        event_name = f"event_{procedure_name}_{timezone_offset}"
        adjusted_procedure_name = f"{procedure_name}_{timezone_offset}"
        # Get the current UTC time and adjust it to the database's local time
        current_time_utc = datetime.utcnow()
        current_time_db = current_time_utc + timedelta(minutes=self.db_timezone_offset)


        # Set the event to start at 00 in the database's local time, adjusted by the timezone offset
        event_time_db = current_time_db.replace(hour=0, minute=0, second=0, microsecond=0) + timedelta(days=1)
        # Adjust event time considering the offset difference
        adjusted_event_time = event_time_db - timedelta(minutes=offset_diff)

        drop_event_sql = f"DROP EVENT IF EXISTS {event_name};"

        create_event_sql = f"""
        CREATE EVENT IF NOT EXISTS {event_name}
        ON SCHEDULE EVERY 1 DAY
        STARTS '{adjusted_event_time.strftime('%Y-%m-%d %H:%M:%S')}'
        DO
        BEGIN
        CALL {adjusted_procedure_name};
        END;
        """
        return drop_event_sql, create_event_sql

    def generate_store_event_hourly_sql(self, procedure_name):
        event_name = f"event_{procedure_name}"
        drop_event_sql = f"DROP EVENT IF EXISTS {event_name}"

        create_event_sql = f"""
        CREATE EVENT IF NOT EXISTS {event_name}
        ON SCHEDULE EVERY 1 HOUR
        STARTS (CURRENT_TIMESTAMP + INTERVAL 1 HOUR - INTERVAL MINUTE(CURRENT_TIMESTAMP) MINUTE - INTERVAL SECOND(CURRENT_TIMESTAMP) SECOND)
        DO
        BEGIN
        CALL {procedure_name};
        END;
        """
        return drop_event_sql, create_event_sql

    def generate_store_quest_event_daily_sql(self, procedure_name, timezone_offset):
        # Determine offset_diff based on the sign of timezone_offset
        if timezone_offset >= 0:
            offset_diff = timezone_offset - self.db_timezone_offset
        else:
            offset_diff = self.db_timezone_offset - timezone_offset
        event_name = f"event_{procedure_name}_{timezone_offset}"
        adjusted_procedure_name = f"{procedure_name}_{timezone_offset}"
        # Get the current UTC time and adjust it to the database's local time
        current_time_utc = datetime.utcnow()
        current_time_db = current_time_utc + timedelta(minutes=self.db_timezone_offset)


        # Set the event to start at 1 AM in the database's local time, adjusted by the timezone offset
        event_time_db = current_time_db.replace(hour=15, minute=0, second=0, microsecond=0) + timedelta(days=1)
        # Adjust event time considering the offset difference
        adjusted_event_time = event_time_db - timedelta(minutes=offset_diff)

        drop_event_sql = f"DROP EVENT IF EXISTS {event_name};"

        create_event_sql = f"""
        CREATE EVENT IF NOT EXISTS {event_name}
        ON SCHEDULE EVERY 1 DAY
        STARTS '{adjusted_event_time.strftime('%Y-%m-%d %H:%M:%S')}'
        DO
        BEGIN
        CALL {adjusted_procedure_name};
        END;
        """
        return drop_event_sql, create_event_sql

    def generate_update_daily_event_sql(self, procedure_name, timezone_offset):
        # Determine offset_diff based on the sign of timezone_offset
        if timezone_offset >= 0:
            offset_diff = timezone_offset - self.db_timezone_offset
        else:
            offset_diff = self.db_timezone_offset - timezone_offset
        event_name = f"event_{procedure_name}_{timezone_offset}"
        adjusted_procedure_name = f"{procedure_name}_{timezone_offset}"
        # Get the current UTC time and adjust it to the database's local time
        current_time_utc = datetime.utcnow()
        current_time_db = current_time_utc + timedelta(minutes=self.db_timezone_offset)


        # Set the event to start at 1 AM in the database's local time, adjusted by the timezone offset
        event_time_db = current_time_db.replace(hour=1, minute=0, second=0, microsecond=0) + timedelta(days=1)
        # Adjust event time considering the offset difference
        adjusted_event_time = event_time_db - timedelta(minutes=offset_diff)

        drop_event_sql = f"DROP EVENT IF EXISTS {event_name};"

        create_event_sql = f"""
        CREATE EVENT IF NOT EXISTS {event_name}
        ON SCHEDULE EVERY 1 DAY
        STARTS '{adjusted_event_time.strftime('%Y-%m-%d %H:%M:%S')}'
        DO
        BEGIN
        CALL {adjusted_procedure_name};
        END;
        """
        return drop_event_sql, create_event_sql

    def generate_update_daily_quest_event_sql(self, procedure_name, timezone_offset):
        # Determine offset_diff based on the sign of timezone_offset
        if timezone_offset >= 0:
            offset_diff = timezone_offset - self.db_timezone_offset
        else:
            offset_diff = self.db_timezone_offset - timezone_offset
        event_name = f"event_{procedure_name}_{timezone_offset}"
        adjusted_procedure_name = f"{procedure_name}_{timezone_offset}"
        # Get the current UTC time and adjust it to the database's local time
        current_time_utc = datetime.utcnow()
        current_time_db = current_time_utc + timedelta(minutes=self.db_timezone_offset)


        # Set the event to start at 1 AM in the database's local time, adjusted by the timezone offset
        event_time_db = current_time_db.replace(hour=15, minute=10, second=0, microsecond=0) + timedelta(days=1)
        # Adjust event time considering the offset difference
        adjusted_event_time = event_time_db - timedelta(minutes=offset_diff)

        drop_event_sql = f"DROP EVENT IF EXISTS {event_name};"

        create_event_sql = f"""
        CREATE EVENT IF NOT EXISTS {event_name}
        ON SCHEDULE EVERY 1 DAY
        STARTS '{adjusted_event_time.strftime('%Y-%m-%d %H:%M:%S')}'
        DO
        BEGIN
        CALL {adjusted_procedure_name};
        END;
        """
        return drop_event_sql, create_event_sql

    def generate_update_weekly_event_sql(self, procedure_name, timezone_offset):
        # Determine offset_diff based on the sign of timezone_offset
        if timezone_offset >= 0:
            offset_diff = timezone_offset - self.db_timezone_offset
        else:
            offset_diff = self.db_timezone_offset - timezone_offset
        event_name = f"event_{procedure_name}_{timezone_offset}"
        adjusted_procedure_name = f"{procedure_name}_{timezone_offset}"
        # Get the current UTC time and adjust it to the database's local time
        current_time_utc = datetime.utcnow()
        current_time_db = current_time_utc + timedelta(minutes=self.db_timezone_offset)


        # Set the event to start at 1:20 AM in the database's local time, adjusted by the timezone offset
        event_time_db = current_time_db.replace(hour=1, minute=20, second=0, microsecond=0) + timedelta(days=1)
        # Adjust event time considering the offset difference
        adjusted_event_time = event_time_db - timedelta(minutes=offset_diff)

        # Calculate the next weekly occurrence (next week's day and time)
        next_weekly_time = adjusted_event_time + timedelta(days=(8 - adjusted_event_time.weekday()))

        drop_event_sql = f"DROP EVENT IF EXISTS {event_name};"

        create_event_sql = f"""
        CREATE EVENT IF NOT EXISTS {event_name}
        ON SCHEDULE EVERY 1 WEEK
        STARTS '{next_weekly_time.strftime('%Y-%m-%d %H:%M:%S')}'
        DO
        BEGIN
        CALL {adjusted_procedure_name};
        END;
        """
        return drop_event_sql, create_event_sql

    def generate_update_weekly_quest_event_sql(self, procedure_name, timezone_offset):
        # Determine offset_diff based on the sign of timezone_offset
        if timezone_offset >= 0:
            offset_diff = timezone_offset - self.db_timezone_offset
        else:
            offset_diff = self.db_timezone_offset - timezone_offset
        event_name = f"event_{procedure_name}_{timezone_offset}"
        adjusted_procedure_name = f"{procedure_name}_{timezone_offset}"
        # Get the current UTC time and adjust it to the database's local time
        current_time_utc = datetime.utcnow()
        current_time_db = current_time_utc + timedelta(minutes=self.db_timezone_offset)


        # Set the event to start at 15:20 PM in the database's local time, adjusted by the timezone offset
        event_time_db = current_time_db.replace(hour=15, minute=20, second=0, microsecond=0) + timedelta(days=1)
        # Adjust event time considering the offset difference
        adjusted_event_time = event_time_db - timedelta(minutes=offset_diff)

        # Calculate the next weekly occurrence (next week's day and time)
        next_weekly_time = adjusted_event_time + timedelta(days=(8 - adjusted_event_time.weekday()))

        drop_event_sql = f"DROP EVENT IF EXISTS {event_name};"

        create_event_sql = f"""
        CREATE EVENT IF NOT EXISTS {event_name}
        ON SCHEDULE EVERY 1 WEEK
        STARTS '{next_weekly_time.strftime('%Y-%m-%d %H:%M:%S')}'
        DO
        BEGIN
        CALL {adjusted_procedure_name};
        END;
        """
        return drop_event_sql, create_event_sql

    def generate_update_monthly_event_sql(self, procedure_name, timezone_offset):
        # Determine offset_diff based on the sign of timezone_offset
        if timezone_offset >= 0:
            offset_diff = timezone_offset - self.db_timezone_offset
        else:
            offset_diff = self.db_timezone_offset - timezone_offset
        event_name = f"event_{procedure_name}_{timezone_offset}"
        adjusted_procedure_name = f"{procedure_name}_{timezone_offset}"
        # Get the current UTC time and adjust it to the database's local time
        current_time_utc = datetime.utcnow()
        current_time_db = current_time_utc + timedelta(minutes=self.db_timezone_offset)


        # Set the event to start at 1:20 AM in the database's local time, adjusted by the timezone offset
        event_time_db = current_time_db.replace(hour=1, minute=40, second=0, microsecond=0) + timedelta(days=1)
        # Adjust event time considering the offset difference
        adjusted_event_time = event_time_db - timedelta(minutes=offset_diff)

        # Calculate the next occurrence on the first day of the next month at the same time
        next_monthly_time = adjusted_event_time.replace(day=1) + relativedelta(months=1)

        drop_event_sql = f"DROP EVENT IF EXISTS {event_name};"

        create_event_sql = f"""
        CREATE EVENT IF NOT EXISTS {event_name}
        ON SCHEDULE EVERY 1 MONTH
        STARTS '{next_monthly_time.strftime('%Y-%m-%d %H:%M:%S')}'
        DO
        BEGIN
        CALL {adjusted_procedure_name};
        END;
        """
        return drop_event_sql, create_event_sql

    def generate_update_monthly_quest_event_sql(self, procedure_name, timezone_offset):
        # Determine offset_diff based on the sign of timezone_offset
        if timezone_offset >= 0:
            offset_diff = timezone_offset - self.db_timezone_offset
        else:
            offset_diff = self.db_timezone_offset - timezone_offset
        event_name = f"event_{procedure_name}_{timezone_offset}"
        adjusted_procedure_name = f"{procedure_name}_{timezone_offset}"
        # Get the current UTC time and adjust it to the database's local time
        current_time_utc = datetime.utcnow()
        current_time_db = current_time_utc + timedelta(minutes=self.db_timezone_offset)


        # Set the event to start at 1:20 AM in the database's local time, adjusted by the timezone offset
        event_time_db = current_time_db.replace(hour=15, minute=40, second=0, microsecond=0) + timedelta(days=1)
        # Adjust event time considering the offset difference
        adjusted_event_time = event_time_db - timedelta(minutes=offset_diff)

        # Calculate the next occurrence on the first day of the next month at the same time
        next_monthly_time = adjusted_event_time.replace(day=1) + relativedelta(months=1)

        drop_event_sql = f"DROP EVENT IF EXISTS {event_name};"

        create_event_sql = f"""
        CREATE EVENT IF NOT EXISTS {event_name}
        ON SCHEDULE EVERY 1 MONTH
        STARTS '{next_monthly_time.strftime('%Y-%m-%d %H:%M:%S')}'
        DO
        BEGIN
        CALL {adjusted_procedure_name};
        END;
        """
        return drop_event_sql, create_event_sql

    def generate_update_hourly_event_sql(self, procedure_name):
        event_name = f"event_{procedure_name}"

        drop_event_sql = f"DROP EVENT IF EXISTS {event_name};"

        create_event_sql = f"""
        CREATE EVENT IF NOT EXISTS {event_name}
        ON SCHEDULE EVERY 1 HOUR
        STARTS (CURRENT_TIMESTAMP + INTERVAL 1 HOUR - INTERVAL MINUTE(CURRENT_TIMESTAMP) MINUTE - INTERVAL SECOND(CURRENT_TIMESTAMP) SECOND + INTERVAL 2 MINUTE)
        DO
        BEGIN
        CALL {procedure_name};
        END;
        """
        return drop_event_sql, create_event_sql

    def generate_daily_delete_event_sql(self, procedure_name, timezone_offset):
        # Determine offset_diff based on the sign of timezone_offset
        if timezone_offset >= 0:
            offset_diff = timezone_offset - self.db_timezone_offset
        else:
            offset_diff = self.db_timezone_offset - timezone_offset
        event_name = f"event_{procedure_name}_{timezone_offset}"
        adjusted_procedure_name = f"{procedure_name}_{timezone_offset}"
        # Get the current UTC time and adjust it to the database's local time
        current_time_utc = datetime.utcnow()
        current_time_db = current_time_utc + timedelta(minutes=self.db_timezone_offset)


        # Set the event to start at 1 AM in the database's local time, adjusted by the timezone offset
        event_time_db = current_time_db.replace(hour=4, minute=50, second=0, microsecond=0) + timedelta(days=1)
        # Adjust event time considering the offset difference
        adjusted_event_time = event_time_db - timedelta(minutes=offset_diff)

        drop_event_sql = f"DROP EVENT IF EXISTS {event_name};"

        create_event_sql = f"""
        CREATE EVENT IF NOT EXISTS {event_name}
        ON SCHEDULE EVERY 1 DAY
        STARTS '{adjusted_event_time.strftime('%Y-%m-%d %H:%M:%S')}'
        DO
        BEGIN
        CALL {adjusted_procedure_name};
        END;
        """
        return drop_event_sql, create_event_sql

    def generate_daily_delete_quest_event_sql(self, procedure_name, timezone_offset):
        # Determine offset_diff based on the sign of timezone_offset
        if timezone_offset >= 0:
            offset_diff = timezone_offset - self.db_timezone_offset
        else:
            offset_diff = self.db_timezone_offset - timezone_offset
        event_name = f"event_{procedure_name}_{timezone_offset}"
        adjusted_procedure_name = f"{procedure_name}_{timezone_offset}"
        # Get the current UTC time and adjust it to the database's local time
        current_time_utc = datetime.utcnow()
        current_time_db = current_time_utc + timedelta(minutes=self.db_timezone_offset)


        # Set the event to start at 1 AM in the database's local time, adjusted by the timezone offset
        event_time_db = current_time_db.replace(hour=16, minute=00, second=0, microsecond=0) + timedelta(days=1)
        # Adjust event time considering the offset difference
        adjusted_event_time = event_time_db - timedelta(minutes=offset_diff)

        drop_event_sql = f"DROP EVENT IF EXISTS {event_name};"

        create_event_sql = f"""
        CREATE EVENT IF NOT EXISTS {event_name}
        ON SCHEDULE EVERY 1 DAY
        STARTS '{adjusted_event_time.strftime('%Y-%m-%d %H:%M:%S')}'
        DO
        BEGIN
        CALL {adjusted_procedure_name};
        END;
        """
        return drop_event_sql, create_event_sql

    async def create_events(self):
        unique_timezones = await self.get_unique_timezones()

        # Daily Storage Events
        daily_procedure_names = [
            "store_pokemon_total",
            "store_pokemon_grouped",
            "store_raid_total",
            "store_raid_grouped",
            "store_invasion_total",
            "store_invasion_grouped"
        ]

        # Hourly Storage Events
        store_hourly_procedure = [
            "store_hourly_pokemon_tth"
        ]

        # Daily Quest Storage Events
        store_quest_daily_procedure = [
            "store_quest_total",
            "store_quest_grouped"
        ]

        # Daily Update Procedure Events
        daily_update_procedures_names = [
            "update_daily_pokemon_total_stats",
            "update_daily_pokemon_grouped_stats",
            "update_daily_raid_total_stats",
            "update_daily_raid_grouped_stats",
            "update_daily_invasion_total_stats",
            "update_daily_invasion_grouped_stats",
            "update_daily_surge_stats",
            "update_daily_pokemon_tth_stats"
        ]

        # Daily Update Quest Events
        daily_update_quest_procedures_names = [
            "update_daily_quest_total_stats",
            "update_daily_quest_grouped_stats"
        ]

        # Weekly Update Procedure Events
        weekly_update_procedures_names = [
            "update_weekly_pokemon_grouped_stats",
            "update_weekly_raid_grouped_stats",
            "update_weekly_invasion_grouped_stats",
            "update_weekly_surge_stats"
        ]

        # Weekly Update Quest Events
        weekly_update_quest_procedures_names = [
            "update_weekly_quest_grouped_stats"
        ]

        # Monthly Update Procedure Events
        monthly_update_procedures_names = [
            "update_monthly_pokemon_grouped_stats",
            "update_monthly_raid_grouped_stats",
            "update_monthly_invasion_grouped_stats",
            "update_monthly_surge_stats"
        ]

        # Monthly Update Quest Events
        monthly_update_quest_procedures_names = [
            "update_monthly_quest_grouped_stats"
        ]

        # Hourly Update Events
        hourly_update_procedures_names = [
            "update_hourly_raid_total_stats",
            "update_hourly_invasion_total_stats",
            "update_hourly_surge_stats",
            "update_hourly_pokemon_tth_stats"
        ]

        # Daily Delete Events
        daily_delete_procedures_names = [
            "delete_pokemon_sightings_batches",
            "delete_raid_sightings_batches",
            "delete_invasion_sightings_batches"
        ]

        # Daily Quest Delete Events
        daily_delete_quest_procedures_name = [
            "delete_quest_sightings_batches"
        ]

        for timezone in unique_timezones:
            offset = timezone['time_zone_offset']
            # Daily Storage Events
            for procedure in daily_procedure_names:
                drop_event_sql, create_event_sql = self.generate_store_event_daily_sql(procedure, offset)
                await self.db_ops.execute_sql(drop_event_sql)
                await self.db_ops.execute_sql(create_event_sql)
                print(f"Event for {procedure} with offset {offset} created/updated.")
            # Hourly Storage Events
            for hourly_procedure in store_hourly_procedure:
                drop_event_sql, create_event_sql = self.generate_store_event_hourly_sql(hourly_procedure)
                await self.db_ops.execute_sql(drop_event_sql)
                await self.db_ops.execute_sql(create_event_sql)
                print(f"Event for {hourly_procedure} with offset {offset} created/updated.")
            # Daily Quest Storage
            for quest_daily_procedure in store_quest_daily_procedure:
                drop_event_sql, create_event_sql = self.generate_store_quest_event_daily_sql(quest_daily_procedure, offset)
                await self.db_ops.execute_sql(drop_event_sql)
                await self.db_ops.execute_sql(create_event_sql)
                print(f"Event for {quest_daily_procedure} with offset {offset} created/updated.")
            # Daily Update Events
            for daily_update_procedure in daily_update_procedures_names:
                drop_event_sql, create_event_sql = self.generate_update_daily_event_sql(daily_update_procedure, offset)
                await self.db_ops.execute_sql(drop_event_sql)
                await self.db_ops.execute_sql(create_event_sql)
                print(f"Event for {daily_update_procedure} with offset {offset} created/updated.")
            # Daily Update Quest Events
            for daily_update_quest_procedure in daily_update_quest_procedures_names:
                drop_event_sql, create_event_sql = self.generate_update_daily_quest_event_sql(daily_update_quest_procedure, offset)
                await self.db_ops.execute_sql(drop_event_sql)
                await self.db_ops.execute_sql(create_event_sql)
                print(f"Event for {daily_update_quest_procedure} with offset {offset} created/updated.")
            # Weekly Update Events
            for weekly_update_procedure in weekly_update_procedures_names:
                drop_event_sql, create_event_sql = self.generate_update_weekly_event_sql(weekly_update_procedure, offset)
                await self.db_ops.execute_sql(drop_event_sql)
                await self.db_ops.execute_sql(create_event_sql)
                print(f"Event for {weekly_update_procedure} with offset {offset} created/updated.")
            # Weekly Update Quest Events
            for weekly_update_quest_procedure in weekly_update_quest_procedures_names:
                drop_event_sql, create_event_sql = self.generate_update_weekly_quest_event_sql(weekly_update_quest_procedure, offset)
                await self.db_ops.execute_sql(drop_event_sql)
                await self.db_ops.execute_sql(create_event_sql)
                print(f"Event for {weekly_update_procedure} with offset {offset} created/updated.")
            # Monthly Update Events
            for monthly_update_procedure in monthly_update_procedures_names:
                drop_event_sql, create_event_sql = self.generate_update_monthly_event_sql(monthly_update_procedure, offset)
                await self.db_ops.execute_sql(drop_event_sql)
                await self.db_ops.execute_sql(create_event_sql)
                print(f"Event for {monthly_update_procedure} with offset {offset} created/updated.")
            # Monthly Update Quest Events
            for monthly_update_quest_procedure in monthly_update_quest_procedures_names:
                drop_event_sql, create_event_sql = self.generate_update_monthly_quest_event_sql(monthly_update_quest_procedure, offset)
                await self.db_ops.execute_sql(drop_event_sql)
                await self.db_ops.execute_sql(create_event_sql)
                print(f"Event for {monthly_update_quest_procedure} with offset {offset} created/updated.")
            # Hourly Update Total/TTH/Surge Events
            for hourly_update_procedure in hourly_update_procedures_names:
                drop_event_sql, create_event_sql = self.generate_update_hourly_event_sql(hourly_update_procedure)
                await self.db_ops.execute_sql(drop_event_sql)
                await self.db_ops.execute_sql(create_event_sql)
                print(f"Event for {hourly_update_procedure} with offset {offset} created/updated.")
            # Daily Delete Events
            for daily_delete_procedure in daily_delete_procedures_names:
                drop_event_sql, create_event_sql = self.generate_daily_delete_event_sql(daily_delete_procedure, offset)
                await self.db_ops.execute_sql(drop_event_sql)
                await self.db_ops.execute_sql(create_event_sql)
                print(f"Event for {daily_delete_procedure} with offset {offset} created/updated.")
            # Daily Delete Quest Events
            for daily_delete_quest_procedure in daily_delete_quest_procedures_name:
                drop_event_sql, create_event_sql = self.generate_daily_delete_quest_event_sql(daily_delete_quest_procedure, offset)
                await self.db_ops.execute_sql(drop_event_sql)
                await self.db_ops.execute_sql(create_event_sql)
                print(f"Event for {daily_delete_quest_procedure} with offset {offset} created/updated.")
