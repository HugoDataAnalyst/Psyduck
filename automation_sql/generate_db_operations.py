from config.app_config import app_config
from tortoise import run_async
from orm.procedures import ProcedureGenerator
from orm.events import EventGenerator
from orm.queries import DatabaseOperations

async def create_procedures():
    db_ops = DatabaseOperations()
    procedure_generator = ProcedureGenerator()
    area_timezones = await db_ops.get_area_timezones()

    timezone_groups = {}
    for area in area_timezones:
        area_name, offset = area
        if offset not in timezone_groups:
            timezone_groups[offset] = []
        timezone_groups[offset].append(area_name)

    for offset, areas in timezone_groups.items():
        drop_store_pokemon_total_sql, store_pokemon_total_sql = await procedure_generator.generate_store_pokemon_total_sql(areas, offset)
        drop_store_pokemon_grouped_sql, store_pokemon_grouped_sql = await procedure_generator.generate_store_pokemon_grouped_sql(areas, offset)
        drop_store_quest_total_sql ,store_quest_total_sql = await procedure_generator.generate_store_quest_total_sql(areas, offset)
        drop_store_quest_grouped_sql ,store_quest_grouped_sql = await procedure_generator.generate_store_quest_grouped_sql(areas, offset)
        drop_store_raid_total_sql ,store_raid_total_sql = await procedure_generator.generate_store_raid_total_sql(areas, offset)
        drop_store_raid_grouped_sql ,store_raid_grouped_sql = await procedure_generator.generate_store_raid_grouped_sql(areas, offset)
        drop_store_invasion_total_sql ,store_invasion_total_sql = await procedure_generator.generate_store_invasion_total_sql(areas, offset)
        drop_store_invasion_grouped_sql ,store_invasion_grouped_sql = await procedure_generator.generate_store_invasion_grouped_sql(areas, offset)
        drop_store_hourly_pokemon_tth_sql ,store_hourly_pokemon_tth_sql = await procedure_generator.generate_store_hourly_pokemon_tth_procedure()

        await db_ops.execute_sql(drop_store_pokemon_total_sql)
        await db_ops.execute_sql(store_pokemon_total_sql)
        await db_ops.execute_sql(drop_store_pokemon_grouped_sql)
        await db_ops.execute_sql(store_pokemon_grouped_sql)
        await db_ops.execute_sql(drop_store_quest_total_sql)
        await db_ops.execute_sql(store_quest_total_sql)
        await db_ops.execute_sql(drop_store_quest_grouped_sql)
        await db_ops.execute_sql(store_quest_grouped_sql)
        await db_ops.execute_sql(drop_store_raid_total_sql)
        await db_ops.execute_sql(store_raid_total_sql)
        await db_ops.execute_sql(drop_store_raid_grouped_sql)
        await db_ops.execute_sql(store_raid_grouped_sql)
        await db_ops.execute_sql(drop_store_invasion_total_sql)
        await db_ops.execute_sql(store_invasion_total_sql)
        await db_ops.execute_sql(drop_store_invasion_grouped_sql)
        await db_ops.execute_sql(store_invasion_grouped_sql)
        await db_ops.execute_sql(drop_store_hourly_pokemon_tth_sql)
        await db_ops.execute_sql(store_hourly_pokemon_tth_sql)

async def main():
    # Create procedures
    await create_procedures()

    # Create events
    db_timezone_offset = app_config.db_timezone_offset
    event_generator = EventGenerator(db_timezone_offset)
    await event_generator.create_events()

if __name__ == "__main__":
    run_async(main())
