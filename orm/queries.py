from tortoise.transactions import in_transaction
from orm.models import (
    PokemonSightings,
    QuestSightings,
    RaidSightings,
    InvasionSightings,
    DailyPokemonGroupedStats,
    WeeklyPokemonGroupedStats,
    MonthlyPokemonGroupedStats,
    HourlyPokemonTotalStats,
    DailyPokemonTotalStats,
    PokemonTotalStats,
    DailySurgePokemonStats,
    WeeklySurgePokemonStats,
    MonthlySurgePokemonStats,
    DailyQuestGroupedStats,
    WeeklyQuestGroupedStats,
    MonthlyQuestGroupedStats,
    DailyQuestTotalStats,
    QuestTotalStats,
    DailyRaidGroupedStats,
    WeeklyRaidGroupedStats,
    MonthlyRaidGroupedStats,
    HourlyRaidTotalStats,
    DailyRaidTotalStats,
    RaidTotalStats,
    DailyInvasionGroupedStats,
    WeeklyInvasionGroupedStats,
    MonthlyInvasionGroupedStats,
    HourlyInvasionTotalStats,
    DailyInvasionTotalStats,
    InvasionTotalStats,
    HourlyPokemonTthStats,
    DailyPokemonTthStats
)

class DatabaseOperations:

    async def insert_pokemon_data(self, data_batch):
        async with in_transaction() as conn:
            await PokemonSightings.bulk_create([PokemonSightings(**data) for data in data_batch])

    async def insert_pokemon_data_raw(self, data_batch):
        # Direct insertion using raw SQL
        async with in_transaction() as conn:
            values = ", ".join(
                f"({data['pokemon_id']}, '{data['form']}', {data['latitude']}, {data['longitude']}, "
                f"{data['iv']}, {data['pvp_little_rank']}, {data['pvp_great_rank']}, {data['pvp_ultra_rank']}, "
                f"{data['shiny']}, '{data['area_name']}', {data['despawn_time']}, '{data['inserted_at'].isoformat()}')"
                for data in data_batch
            )
            query = (
                f"INSERT INTO pokemon_sightings (pokemon_id, form, latitude, longitude, iv, "
                f"pvp_little_rank, pvp_great_rank, pvp_ultra_rank, shiny, area_name, despawn_time, inserted_at) "
                f"VALUES {values}"
            )
            await conn.execute_query(query)

    async def insert_quest_data(self, data_batch):
        async with in_transaction() as conn:
            await QuestSightings.bulk_create([QuestSightings(**data) for data in data_batch])

    async def insert_raid_data(self, data_batch):
        async with in_transaction() as conn:
            await RaidSightings.bulk_create([RaidSightings(**data) for data in data_batch])

    async def insert_invasion_data(self, data_batch):
        async with in_transaction() as conn:
            await InvasionSightings.bulk_create([InvasionSightings(**data) for data in data_batch])

    async def fetch_all_records(self, model, order_by=None):
        if order_by:
            return await model.all().order_by(*order_by)
        return await model.all()

    async def fetch_daily_pokemon_grouped_stats(self):
        return await self.fetch_all_records(DailyPokemonGroupedStats, order_by=["area_name", "pokemon_id"])

    async def fetch_weekly_pokemon_grouped_stats(self):
        return await self.fetch_all_records(WeeklyPokemonGroupedStats, order_by=["area_name", "pokemon_id"])

    async def fetch_monthly_pokemon_grouped_stats(self):
        return await self.fetch_all_records(MonthlyPokemonGroupedStats, order_by=["area_name", "pokemon_id"])

    async def fetch_hourly_pokemon_total_stats(self):
        return await self.fetch_all_records(HourlyPokemonTotalStats, order_by=["area_name"])

    async def fetch_daily_pokemon_total_stats(self):
        return await self.fetch_all_records(DailyPokemonTotalStats, order_by=["area_name"])

    async def fetch_pokemon_total_stats(self):
        return await self.fetch_all_records(PokemonTotalStats, order_by=["area_name"])

    async def fetch_daily_surge_api_pokemon_stats(self):
        return await self.fetch_all_records(DailySurgePokemonStats)

    async def fetch_weekly_surge_api_pokemon_stats(self):
        return await self.fetch_all_records(WeeklySurgePokemonStats)

    async def fetch_monthly_surge_api_pokemon_stats(self):
        return await self.fetch_all_records(MonthlySurgePokemonStats)

    async def fetch_daily_quest_grouped_stats(self):
        return await self.fetch_all_records(DailyQuestGroupedStats)

    async def fetch_weekly_quest_grouped_stats(self):
        return await self.fetch_all_records(WeeklyQuestGroupedStats)

    async def fetch_monthly_quest_grouped_stats(self):
        return await self.fetch_all_records(MonthlyQuestGroupedStats)

    async def fetch_daily_quest_total_stats(self):
        return await self.fetch_all_records(DailyQuestTotalStats)

    async def fetch_total_quest_total_stats(self):
        return await self.fetch_all_records(QuestTotalStats)

    async def fetch_daily_raid_grouped_stats(self):
        return await self.fetch_all_records(DailyRaidGroupedStats)

    async def fetch_weekly_raid_grouped_stats(self):
        return await self.fetch_all_records(WeeklyRaidGroupedStats)

    async def fetch_monthly_raid_grouped_stats(self):
        return await self.fetch_all_records(MonthlyRaidGroupedStats)

    async def fetch_hourly_raid_total_stats(self):
        return await self.fetch_all_records(HourlyRaidTotalStats)

    async def fetch_daily_raid_total_stats(self):
        return await self.fetch_all_records(DailyRaidTotalStats)

    async def fetch_total_raid_total_stats(self):
        return await self.fetch_all_records(RaidTotalStats)

    async def fetch_daily_invasion_grouped_stats(self):
        return await self.fetch_all_records(DailyInvasionGroupedStats)

    async def fetch_weekly_invasion_grouped_stats(self):
        return await self.fetch_all_records(WeeklyInvasionGroupedStats)

    async def fetch_monthly_invasion_grouped_stats(self):
        return await self.fetch_all_records(MonthlyInvasionGroupedStats)

    async def fetch_hourly_invasion_total_stats(self):
        return await self.fetch_all_records(HourlyInvasionTotalStats)

    async def fetch_daily_invasion_total_stats(self):
        return await self.fetch_all_records(DailyInvasionTotalStats)

    async def fetch_total_invasion_total_stats(self):
        return await self.fetch_all_records(InvasionTotalStats)

    async def fetch_hourly_pokemon_tth_stats(self):
        return await self.fetch_all_records(HourlyPokemonTthStats)

    async def fetch_daily_pokemon_tth_stats(self):
        return await self.fetch_all_records(DailyPokemonTthStats)
