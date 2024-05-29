from tortoise.transactions import in_transaction
from datetime import datetime
from orm.models import (
    AreaTimeZones,
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
    DailyPokemonTthStats,
    AreaTimeZones
)

class DatabaseOperations:

    async def insert_pokemon_data(self, data_batch):
        async with in_transaction() as conn:
            # Convert ISO format datetime strings back to datetime objects
            for data in data_batch:
                if 'inserted_at' in data:
                    data['inserted_at'] = datetime.fromisoformat(data['inserted_at'])
            await PokemonSightings.bulk_create([PokemonSightings(**data) for data in data_batch])


    async def insert_quest_data(self, data_batch):
        async with in_transaction() as conn:
            # Convert ISO format datetime strings back to datetime objects
            for data in data_batch:
                if 'inserted_at' in data:
                    data['inserted_at'] = datetime.fromisoformat(data['inserted_at'])
            await QuestSightings.bulk_create([QuestSightings(**data) for data in data_batch])

    async def insert_raid_data(self, data_batch):
        async with in_transaction() as conn:
            # Convert ISO format datetime strings back to datetime objects
            for data in data_batch:
                if 'inserted_at' in data:
                    data['inserted_at'] = datetime.fromisoformat(data['inserted_at'])
            await RaidSightings.bulk_create([RaidSightings(**data) for data in data_batch])

    async def insert_invasion_data(self, data_batch):
        async with in_transaction() as conn:
            # Convert ISO format datetime strings back to datetime objects
            for data in data_batch:
                if 'inserted_at' in data:
                    data['inserted_at'] = datetime.fromisoformat(data['inserted_at'])
            await InvasionSightings.bulk_create([InvasionSightings(**data) for data in data_batch])

    async def fetch_all_records(self, model, order_by=None):
        if order_by:
            return await model.all().order_by(*order_by)
        return await model.all()

    async def fetch_daily_pokemon_grouped_stats(self):
        return await DailyPokemonGroupedStats.all().order_by("area_name", "pokemon_id").values(
            "day",
            "pokemon_id",
            "form",
            "avg_lat",
            "avg_lon",
            "total",
            "total_iv100",
            "total_iv0",
            "total_top1_little",
            "total_top1_great",
            "total_top1_ultra",
            "total_shiny",
            "area_name",
            "avg_despawn"
        )

    async def fetch_weekly_pokemon_grouped_stats(self):
        return await WeeklyPokemonGroupedStats.all().order_by("area_name", "pokemon_id").values(
            "day",
            "pokemon_id",
            "form",
            "avg_lat",
            "avg_lon",
            "total",
            "total_iv100",
            "total_iv0",
            "total_top1_little",
            "total_top1_great",
            "total_top1_ultra",
            "total_shiny",
            "area_name",
            "avg_despawn"
        )

    async def fetch_monthly_pokemon_grouped_stats(self):
        return await MonthlyPokemonGroupedStats.all().order_by("area_name", "pokemon_id").values(
            "day",
            "pokemon_id",
            "form",
            "avg_lat",
            "avg_lon",
            "total",
            "total_iv100",
            "total_iv0",
            "total_top1_little",
            "total_top1_great",
            "total_top1_ultra",
            "total_shiny",
            "area_name",
            "avg_despawn"
        )

    async def fetch_hourly_pokemon_total_stats(self):
        return await HourlyPokemonTotalStats.all().order_by("area_name").values(
            "area_name",
            "total",
            "total_iv100",
            "total_iv0",
            "total_top1_little",
            "total_top1_great",
            "total_top1_ultra",
            "total_shiny",
            "avg_despawn"
        )

    async def fetch_daily_pokemon_total_stats(self):
        return await DailyPokemonTotalStats.all().order_by("area_name").values(
            "day",
            "area_name",
            "total",
            "total_iv100",
            "total_iv0",
            "total_top1_little",
            "total_top1_great",
            "total_top1_ultra",
            "total_shiny",
            "avg_despawn"
        )

    async def fetch_pokemon_total_stats(self):
        return await PokemonTotalStats.all().order_by("area_name").values(
            "area_name",
            "total",
            "total_iv100",
            "total_iv0",
            "total_top1_little",
            "total_top1_great",
            "total_top1_ultra",
            "total_shiny",
            "avg_despawn"
        )

    async def fetch_daily_surge_api_pokemon_stats(self):
        return await DailySurgePokemonStats.all().values(
            "hour",
            "total_iv100",
            "total_iv0",
            "total_top1_little",
            "total_top1_great",
            "total_top1_ultra",
            "total_shiny"
        )

    async def fetch_weekly_surge_api_pokemon_stats(self):
        return await WeeklySurgePokemonStats.all().values(
            "hour",
            "total_iv100",
            "total_iv0",
            "total_top1_little",
            "total_top1_great",
            "total_top1_ultra",
            "total_shiny"
        )

    async def fetch_monthly_surge_api_pokemon_stats(self):
        return await MonthlySurgePokemonStats.all().values(
            "hour",
            "total_iv100",
            "total_iv0",
            "total_top1_little",
            "total_top1_great",
            "total_top1_ultra",
            "total_shiny"
        )

    async def fetch_daily_quest_grouped_stats(self):
        return await DailyQuestGroupedStats.all().values(
            "day",
            "area_name",
            "ar_type",
            "normal_type",
            "reward_ar_type",
            "reward_normal_type",
            "reward_ar_item_id",
            "reward_ar_item_amount",
            "reward_normal_item_id",
            "reward_normal_item_amount",
            "reward_ar_poke_id",
            "reward_ar_poke_form",
            "reward_normal_poke_id",
            "reward_normal_poke_form",
            "total"
        )

    async def fetch_weekly_quest_grouped_stats(self):
        return await WeeklyQuestGroupedStats.all().values(
            "day",
            "area_name",
            "ar_type",
            "normal_type",
            "reward_ar_type",
            "reward_normal_type",
            "reward_ar_item_id",
            "reward_ar_item_amount",
            "reward_normal_item_id",
            "reward_normal_item_amount",
            "reward_ar_poke_id",
            "reward_ar_poke_form",
            "reward_normal_poke_id",
            "reward_normal_poke_form",
            "total"
        )

    async def fetch_monthly_quest_grouped_stats(self):
        return await MonthlyQuestGroupedStats.all().values(
            "day",
            "area_name",
            "ar_type",
            "normal_type",
            "reward_ar_type",
            "reward_normal_type",
            "reward_ar_item_id",
            "reward_ar_item_amount",
            "reward_normal_item_id",
            "reward_normal_item_amount",
            "reward_ar_poke_id",
            "reward_ar_poke_form",
            "reward_normal_poke_id",
            "reward_normal_poke_form",
            "total"
        )

    async def fetch_daily_quest_total_stats(self):
        return await DailyQuestTotalStats.all().values(
            "day",
            "area_name",
            "total_stops",
            "ar",
            "normal"
        )

    async def fetch_total_quest_total_stats(self):
        return await QuestTotalStats.all().values(
            "area_name",
            "ar",
            "normal"
        )

    async def fetch_daily_raid_grouped_stats(self):
        return await DailyRaidGroupedStats.all().values(
            "day",
            "area_name",
            "level",
            "pokemon_id",
            "form",
            "costume",
            "ex_raid_eligible",
            "is_exclusive",
            "total"
        )

    async def fetch_weekly_raid_grouped_stats(self):
        return await WeeklyRaidGroupedStats.all().values(
            "day",
            "area_name",
            "level",
            "pokemon_id",
            "form",
            "costume",
            "ex_raid_eligible",
            "is_exclusive",
            "total"
        )

    async def fetch_monthly_raid_grouped_stats(self):
        return await MonthlyRaidGroupedStats.all().values(
            "day",
            "area_name",
            "level",
            "pokemon_id",
            "form",
            "costume",
            "ex_raid_eligible",
            "is_exclusive",
            "total"
        )

    async def fetch_hourly_raid_total_stats(self):
        return await HourlyRaidTotalStats.all().values(
            "area_name",
            "total",
            "total_ex_raid",
            "total_exclusive"
        )

    async def fetch_daily_raid_total_stats(self):
        return await DailyRaidTotalStats.all().values(
            "day",
            "area_name",
            "total",
            "total_ex_raid",
            "total_exclusive"
        )

    async def fetch_total_raid_total_stats(self):
        return await RaidTotalStats.all().values(
            "area_name",
            "total",
            "total_ex_raid",
            "total_exclusive"
        )

    async def fetch_daily_invasion_grouped_stats(self):
        return await DailyInvasionGroupedStats.all().values(
            "day",
            "area_name",
            "display_type",
            "grunt",
            "total_grunts"
        )

    async def fetch_weekly_invasion_grouped_stats(self):
        return await WeeklyInvasionGroupedStats.all().values(
            "day",
            "area_name",
            "display_type",
            "grunt",
            "total_grunts"
        )

    async def fetch_monthly_invasion_grouped_stats(self):
        return await MonthlyInvasionGroupedStats.all().values(
            "day",
            "area_name",
            "display_type",
            "grunt",
            "total_grunts"
        )

    async def fetch_hourly_invasion_total_stats(self):
        return await HourlyInvasionTotalStats.all().values(
            "area_name",
            "total_grunts",
            "total_confirmed",
            "total_unconfirmed"
        )

    async def fetch_daily_invasion_total_stats(self):
        return await DailyInvasionTotalStats.all().values(
            "day",
            "area_name",
            "total_grunts",
            "total_confirmed",
            "total_unconfirmed"
        )

    async def fetch_total_invasion_total_stats(self):
        return await InvasionTotalStats.all().values(
            "area_name",
            "total_grunts",
            "total_confirmed",
            "total_unconfirmed"
        )

    async def fetch_hourly_pokemon_tth_stats(self):
        return await HourlyPokemonTthStats.all().values(
            "area_name",
            "tth_5",
            "tth_10",
            "tth_15",
            "tth_20",
            "tth_25",
            "tth_30",
            "tth_35",
            "tth_40",
            "tth_45",
            "tth_50",
            "tth_55",
            "tth_55_plus"
        )

    async def fetch_daily_pokemon_tth_stats(self):
        return await DailyPokemonTthStats.all().values(
            "hour",
            "area_name",
            "total_tth_5",
            "total_tth_10",
            "total_tth_15",
            "total_tth_20",
            "total_tth_25",
            "total_tth_30",
            "total_tth_35",
            "total_tth_40",
            "total_tth_45",
            "total_tth_50",
            "total_tth_55",
            "total_tth_55_plus"
        )

    async def update_area_time_zones(self, data_batch):
        async with in_transaction() as conn:
            await AreaTimeZones.bulk_create(
                [AreaTimeZones(**data) for data in data_batch],
                update_on_duplicate=['timezone', 'time_zone_offset']
            )
