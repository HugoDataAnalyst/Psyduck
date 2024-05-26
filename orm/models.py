from tortoise import fields, models

class PokemonSightings(models.Model):
    id = fields.IntField(pk=True)
    pokemon_id = fields.IntField()
    form = fields.CharField(max_length=15, null=True)
    latitude = fields.FloatField(null=True)
    longitude = fields.FloatField(null=True)
    iv = fields.IntField(null=True)
    pvp_little_rank = fields.IntField(null=True)
    pvp_great_rank = fields.IntField(null=True)
    pvp_ultra_rank = fields.IntField(null=True)
    shiny = fields.BooleanField(default=False)
    area_name = fields.CharField(max_length=255, null=True)
    despawn_time = fields.IntField(null=True)
    inserted_at = fields.DatetimeField(auto_now_add=True)

    class Meta:
        table = "pokemon_sightings"
        indexes = [
            ('inserted_at',),
            ('pokemon_id', 'form', 'area_name'),
            ('area_name',)
        ]

class StoragePokemonGroupedStats(models.Model):
    day = fields.DateField()
    pokemon_id = fields.IntField()
    form = fields.CharField(max_length=15)
    avg_lat = fields.FloatField()
    avg_lon = fields.FloatField()
    total = fields.IntField()
    total_iv100 = fields.IntField()
    total_iv0 = fields.IntField()
    total_top1_little = fields.IntField()
    total_top1_great = fields.IntField()
    total_top1_ultra = fields.IntField()
    total_shiny = fields.IntField()
    area_name = fields.CharField(max_length=255)
    avg_despawn = fields.FloatField()

    class Meta:
        table = "storage_pokemon_grouped_stats"
        unique_together = ("day", "pokemon_id", "form", "area_name")

class StoragePokemonTotalStats(models.Model):
    day = fields.DateField()
    area_name = fields.CharField(max_length=255)
    total = fields.IntField()
    total_iv100 = fields.IntField()
    total_iv0 = fields.IntField()
    total_top1_little = fields.IntField()
    total_top1_great = fields.IntField()
    total_top1_ultra = fields.IntField()
    total_shiny = fields.IntField()
    avg_despawn = fields.FloatField()

    class Meta:
        table = "storage_pokemon_total_stats"
        unique_together = ("day", "area_name")

class DailyPokemonGroupedStats(models.Model):
    day = fields.DateField()
    pokemon_id = fields.IntField()
    form = fields.CharField(max_length=15)
    avg_lat = fields.FloatField()
    avg_lon = fields.FloatField()
    total = fields.IntField()
    total_iv100 = fields.IntField()
    total_iv0 = fields.IntField()
    total_top1_little = fields.IntField()
    total_top1_great = fields.IntField()
    total_top1_ultra = fields.IntField()
    total_shiny = fields.IntField()
    area_name = fields.CharField(max_length=255)
    avg_despawn = fields.FloatField()

    class Meta:
        table = "daily_pokemon_grouped_stats"
        unique_together = ("pokemon_id", "form", "area_name")

class WeeklyPokemonGroupedStats(models.Model):
    day = fields.DateField()
    pokemon_id = fields.IntField()
    form = fields.CharField(max_length=15)
    avg_lat = fields.FloatField()
    avg_lon = fields.FloatField()
    total = fields.IntField()
    total_iv100 = fields.IntField()
    total_iv0 = fields.IntField()
    total_top1_little = fields.IntField()
    total_top1_great = fields.IntField()
    total_top1_ultra = fields.IntField()
    total_shiny = fields.IntField()
    area_name = fields.CharField(max_length=255)
    avg_despawn = fields.FloatField()

    class Meta:
        table = "weekly_pokemon_grouped_stats"
        unique_together = ("pokemon_id", "form", "area_name")

class MonthlyPokemonGroupedStats(models.Model):
    day = fields.DateField()
    pokemon_id = fields.IntField()
    form = fields.CharField(max_length=15)
    avg_lat = fields.FloatField()
    avg_lon = fields.FloatField()
    total = fields.BigIntField()
    total_iv100 = fields.BigIntField()
    total_iv0 = fields.BigIntField()
    total_top1_little = fields.BigIntField()
    total_top1_great = fields.BigIntField()
    total_top1_ultra = fields.BigIntField()
    total_shiny = fields.BigIntField()
    area_name = fields.CharField(max_length=255)
    avg_despawn = fields.FloatField()

    class Meta:
        table = "monthly_pokemon_grouped_stats"
        unique_together = ("pokemon_id", "form", "area_name")

class HourlyPokemonTotalStats(models.Model):
    area_name = fields.CharField(max_length=255)
    total = fields.IntField()
    total_iv100 = fields.IntField()
    total_iv0 = fields.IntField()
    total_top1_little = fields.IntField()
    total_top1_great = fields.IntField()
    total_top1_ultra = fields.IntField()
    total_shiny = fields.IntField()
    avg_despawn = fields.FloatField()

    class Meta:
        table = "hourly_pokemon_total_stats"
        unique_together = ("area_name",)

class DailyPokemonTotalStats(models.Model):
    day = fields.DateField()
    area_name = fields.CharField(max_length=255)
    total = fields.IntField()
    total_iv100 = fields.IntField()
    total_iv0 = fields.IntField()
    total_top1_little = fields.IntField()
    total_top1_great = fields.IntField()
    total_top1_ultra = fields.IntField()
    total_shiny = fields.IntField()
    avg_despawn = fields.FloatField()

    class Meta:
        table = "daily_pokemon_total_stats"
        unique_together = ("day", "area_name")

class PokemonTotalStats(models.Model):
    area_name = fields.CharField(max_length=255)
    total = fields.BigIntField()
    total_iv100 = fields.BigIntField()
    total_iv0 = fields.BigIntField()
    total_top1_little = fields.BigIntField()
    total_top1_great = fields.BigIntField()
    total_top1_ultra = fields.BigIntField()
    total_shiny = fields.BigIntField()
    avg_despawn = fields.FloatField()

    class Meta:
        table = "pokemon_total_stats"
        unique_together = ("area_name",)

class QuestSightings(models.Model):
    id = fields.IntField(pk=True)
    pokestop_id = fields.CharField(max_length=255)
    ar_type = fields.IntField()
    normal_type = fields.IntField()
    reward_ar_type = fields.IntField()
    reward_normal_type = fields.IntField()
    reward_ar_item_id = fields.IntField()
    reward_ar_item_amount = fields.IntField()
    reward_normal_item_id = fields.IntField()
    reward_normal_item_amount = fields.IntField()
    reward_ar_poke_id = fields.IntField()
    reward_ar_poke_form = fields.CharField(max_length=15, null=True)
    reward_normal_poke_id = fields.IntField()
    reward_normal_poke_form = fields.CharField(max_length=15, null=True)
    inserted_at = fields.DatetimeField(auto_now_add=True)
    area_name = fields.CharField(max_length=255, null=True)

    class Meta:
        table = "quest_sightings"
        indexes = [
            ('inserted_at',),
            ('area_name',),
            ('reward_ar_type',),
            ('reward_normal_type',),
            ('area_name', 'inserted_at', 'reward_ar_type', 'reward_normal_type'),
            ('area_name', 'reward_ar_type', 'reward_normal_type'),
            ('inserted_at', 'reward_ar_type', 'reward_normal_type'),
            ('reward_ar_type', 'reward_normal_type'),
            ('ar_type', 'reward_ar_type', 'normal_type', 'reward_normal_type', 'reward_ar_item_id', 'reward_ar_item_amount', 'reward_normal_item_id', 'reward_normal_item_amount', 'reward_ar_poke_id', 'reward_ar_poke_form', 'reward_normal_poke_id', 'reward_normal_poke_form', 'area_name')
        ]

class RaidSightings(models.Model):
    id = fields.IntField(pk=True)
    gym_id = fields.CharField(max_length=255)
    ex_raid_eligible = fields.BooleanField(default=False)
    is_exclusive = fields.BooleanField(default=False)
    level = fields.IntField()
    pokemon_id = fields.IntField(null=True)
    form = fields.CharField(max_length=15, null=True)
    costume = fields.CharField(max_length=50, null=True)
    inserted_at = fields.DatetimeField(auto_now_add=True)
    area_name = fields.CharField(max_length=255, null=True)

    class Meta:
        table = "raid_sightings"
        indexes = [
            ('inserted_at',),
            ('area_name',),
            ('level',),
            ('pokemon_id',),
            ('area_name', 'inserted_at', 'pokemon_id', 'form', 'level'),
            ('level', 'pokemon_id', 'form', 'costume', 'ex_raid_eligible', 'is_exclusive', 'area_name')
        ]

class InvasionSightings(models.Model):
    id = fields.IntField(pk=True)
    pokestop_id = fields.CharField(max_length=255)
    display_type = fields.IntField()
    grunt = fields.IntField()
    confirmed = fields.BooleanField(default=False)
    inserted_at = fields.DatetimeField(auto_now_add=True)
    area_name = fields.CharField(max_length=255, null=True)

    class Meta:
        table = "invasion_sightings"
        indexes = [
            ('display_type',),
            ('grunt',),
            ('confirmed',),
            ('area_name', 'inserted_at', 'grunt', 'confirmed'),
            ('area_name', 'inserted_at', 'grunt'),
            ('area_name', 'inserted_at', 'confirmed'),
            ('display_type', 'grunt', 'area_name'),
            ('inserted_at',)
        ]

class HourlySurgeStoragePokemonStats(models.Model):
    hour = fields.DatetimeField()
    total_iv100 = fields.IntField()
    total_iv0 = fields.IntField()
    total_top1_little = fields.IntField()
    total_top1_great = fields.IntField()
    total_top1_ultra = fields.IntField()
    total_shiny = fields.IntField()

    class Meta:
        table = "hourly_surge_storage_pokemon_stats"
        unique_together = ("hour",)

class DailySurgePokemonStats(models.Model):
    hour = fields.IntField()
    total_iv100 = fields.IntField()
    total_iv0 = fields.IntField()
    total_top1_little = fields.IntField()
    total_top1_great = fields.IntField()
    total_top1_ultra = fields.IntField()
    total_shiny = fields.IntField()

    class Meta:
        table = "daily_surge_pokemon_stats"
        unique_together = ("hour",)

class WeeklySurgePokemonStats(models.Model):
    hour = fields.IntField()
    total_iv100 = fields.IntField()
    total_iv0 = fields.IntField()
    total_top1_little = fields.IntField()
    total_top1_great = fields.IntField()
    total_top1_ultra = fields.IntField()
    total_shiny = fields.IntField()

    class Meta:
        table = "weekly_surge_pokemon_stats"
        unique_together = ("hour",)

class MonthlySurgePokemonStats(models.Model):
    hour = fields.IntField()
    total_iv100 = fields.IntField()
    total_iv0 = fields.IntField()
    total_top1_little = fields.IntField()
    total_top1_great = fields.IntField()
    total_top1_ultra = fields.IntField()
    total_shiny = fields.IntField()

    class Meta:
        table = "monthly_surge_pokemon_stats"
        unique_together = ("hour",)

class TotalPokestops(models.Model):
    day = fields.DateField()
    area_name = fields.CharField(max_length=255)
    total_stops = fields.IntField()

    class Meta:
        table = "total_pokestops"
        unique_together = ("day", "area_name")

class StorageQuestGroupedStats(models.Model):
    day = fields.DateField()
    area_name = fields.CharField(max_length=255)
    ar_type = fields.IntField()
    normal_type = fields.IntField()
    reward_ar_type = fields.IntField()
    reward_normal_type = fields.IntField()
    reward_ar_item_id = fields.IntField()
    reward_ar_item_amount = fields.IntField()
    reward_normal_item_id = fields.IntField()
    reward_normal_item_amount = fields.IntField()
    reward_ar_poke_id = fields.IntField()
    reward_ar_poke_form = fields.CharField(max_length=15)
    reward_normal_poke_id = fields.IntField()
    reward_normal_poke_form = fields.CharField(max_length=15)
    total = fields.IntField()

    class Meta:
        table = "storage_quest_grouped_stats"
        unique_together = ("day", "area_name")

class StorageRaidGroupedStats(models.Model):
    day = fields.DateField()
    area_name = fields.CharField(max_length=255)
    level = fields.IntField()
    pokemon_id = fields.IntField()
    form = fields.CharField(max_length=15)
    costume = fields.CharField(max_length=50)
    ex_raid_eligible = fields.BooleanField(default=False)
    is_exclusive = fields.BooleanField(default=False)
    total = fields.IntField()

    class Meta:
        table = "storage_raid_grouped_stats"
        unique_together = ("day", "area_name")

class StorageInvasionGroupedStats(models.Model):
    day = fields.DateField()
    area_name = fields.CharField(max_length=255)
    display_type = fields.IntField()
    grunt = fields.IntField()
    total_grunts = fields.IntField()

    class Meta:
        table = "storage_invasion_grouped_stats"
        unique_together = ("day", "area_name")

class StorageQuestTotalStats(models.Model):
    day = fields.DateField()
    area_name = fields.CharField(max_length=255)
    total_stops = fields.IntField()
    ar = fields.IntField()
    normal = fields.IntField()

    class Meta:
        table = "storage_quest_total_stats"
        unique_together = ("day", "area_name")

class StorageRaidTotalStats(models.Model):
    day = fields.DateField()
    area_name = fields.CharField(max_length=255)
    total = fields.IntField()
    total_ex_raid = fields.IntField()
    total_exclusive = fields.IntField()

    class Meta:
        table = "storage_raid_total_stats"
        unique_together = ("day", "area_name")

class StorageInvasionTotalStats(models.Model):
    day = fields.DateField()
    area_name = fields.CharField(max_length=255)
    total_grunts = fields.IntField()
    total_confirmed = fields.IntField()
    total_unconfirmed = fields.IntField()

    class Meta:
        table = "storage_invasion_total_stats"
        unique_together = ("day", "area_name")

class DailyQuestGroupedStats(models.Model):
    day = fields.DateField()
    area_name = fields.CharField(max_length=255)
    ar_type = fields.IntField()
    normal_type = fields.IntField()
    reward_ar_type = fields.IntField()
    reward_normal_type = fields.IntField()
    reward_ar_item_id = fields.IntField()
    reward_ar_item_amount = fields.IntField()
    reward_normal_item_id = fields.IntField()
    reward_normal_item_amount = fields.IntField()
    reward_ar_poke_id = fields.IntField()
    reward_ar_poke_form = fields.CharField(max_length=15)
    reward_normal_poke_id = fields.IntField()
    reward_normal_poke_form = fields.CharField(max_length=15)
    total = fields.IntField()

    class Meta:
        table = "daily_quest_grouped_stats"
        unique_together = ("day", "area_name")

class WeeklyQuestGroupedStats(models.Model):
    day = fields.DateField()
    area_name = fields.CharField(max_length=255)
    ar_type = fields.IntField()
    normal_type = fields.IntField()
    reward_ar_type = fields.IntField()
    reward_normal_type = fields.IntField()
    reward_ar_item_id = fields.IntField()
    reward_ar_item_amount = fields.IntField()
    reward_normal_item_id = fields.IntField()
    reward_normal_item_amount = fields.IntField()
    reward_ar_poke_id = fields.IntField()
    reward_ar_poke_form = fields.CharField(max_length=15)
    reward_normal_poke_id = fields.IntField()
    reward_normal_poke_form = fields.CharField(max_length=15)
    total = fields.IntField()

    class Meta:
        table = "weekly_quest_grouped_stats"
        unique_together = ("day", "area_name")

class MonthlyQuestGroupedStats(models.Model):
    day = fields.DateField()
    area_name = fields.CharField(max_length=255)
    ar_type = fields.IntField()
    normal_type = fields.IntField()
    reward_ar_type = fields.IntField()
    reward_normal_type = fields.IntField()
    reward_ar_item_id = fields.IntField()
    reward_ar_item_amount = fields.IntField()
    reward_normal_item_id = fields.IntField()
    reward_normal_item_amount = fields.IntField()
    reward_ar_poke_id = fields.IntField()
    reward_ar_poke_form = fields.CharField(max_length=15)
    reward_normal_poke_id = fields.IntField()
    reward_normal_poke_form = fields.CharField(max_length=15)
    total = fields.IntField()

    class Meta:
        table = "monthly_quest_grouped_stats"
        unique_together = ("day", "area_name")

class DailyRaidGroupedStats(models.Model):
    day = fields.DateField()
    area_name = fields.CharField(max_length=255)
    level = fields.IntField()
    pokemon_id = fields.IntField()
    form = fields.CharField(max_length=15)
    costume = fields.CharField(max_length=50)
    ex_raid_eligible = fields.BooleanField(default=False)
    is_exclusive = fields.BooleanField(default=False)
    total = fields.IntField()

    class Meta:
        table = "daily_raid_grouped_stats"
        unique_together = ("day", "area_name")

class WeeklyRaidGroupedStats(models.Model):
    day = fields.DateField()
    area_name = fields.CharField(max_length=255)
    level = fields.IntField()
    pokemon_id = fields.IntField()
    form = fields.CharField(max_length=15)
    costume = fields.CharField(max_length=50)
    ex_raid_eligible = fields.BooleanField(default=False)
    is_exclusive = fields.BooleanField(default=False)
    total = fields.IntField()

    class Meta:
        table = "weekly_raid_grouped_stats"
        unique_together = ("day", "area_name")

class MonthlyRaidGroupedStats(models.Model):
    day = fields.DateField()
    area_name = fields.CharField(max_length=255)
    level = fields.IntField()
    pokemon_id = fields.IntField()
    form = fields.CharField(max_length=15)
    costume = fields.CharField(max_length=50)
    ex_raid_eligible = fields.BooleanField(default=False)
    is_exclusive = fields.BooleanField(default=False)
    total = fields.IntField()

    class Meta:
        table = "monthly_raid_grouped_stats"
        unique_together = ("day", "area_name")

class DailyInvasionGroupedStats(models.Model):
    day = fields.DateField()
    area_name = fields.CharField(max_length=255)
    display_type = fields.IntField()
    grunt = fields.IntField()
    total_grunts = fields.IntField()

    class Meta:
        table = "daily_invasion_grouped_stats"
        unique_together = ("day", "area_name")

class WeeklyInvasionGroupedStats(models.Model):
    day = fields.DateField()
    area_name = fields.CharField(max_length=255)
    display_type = fields.IntField()
    grunt = fields.IntField()
    total_grunts = fields.IntField()

    class Meta:
        table = "weekly_invasion_grouped_stats"
        unique_together = ("day", "area_name")

class MonthlyInvasionGroupedStats(models.Model):
    day = fields.DateField()
    area_name = fields.CharField(max_length=255)
    display_type = fields.IntField()
    grunt = fields.IntField()
    total_grunts = fields.IntField()

    class Meta:
        table = "monthly_invasion_grouped_stats"
        unique_together = ("day", "area_name")

class DailyQuestTotalStats(models.Model):
    day = fields.DateField()
    area_name = fields.CharField(max_length=255)
    total_stops = fields.IntField()
    ar = fields.IntField()
    normal = fields.IntField()

    class Meta:
        table = "daily_quest_total_stats"
        unique_together = ("day", "area_name")

class QuestTotalStats(models.Model):
    area_name = fields.CharField(max_length=255)
    ar = fields.BigIntField()
    normal = fields.BigIntField()

    class Meta:
        table = "quest_total_stats"
        unique_together = ("area_name",)

class HourlyRaidTotalStats(models.Model):
    area_name = fields.CharField(max_length=255)
    total = fields.IntField()
    total_ex_raid = fields.IntField()
    total_exclusive = fields.IntField()

    class Meta:
        table = "hourly_raid_total_stats"
        unique_together = ("area_name",)

class DailyRaidTotalStats(models.Model):
    day = fields.DateField()
    area_name = fields.CharField(max_length=255)
    total = fields.IntField()
    total_ex_raid = fields.IntField()
    total_exclusive = fields.IntField()

    class Meta:
        table = "daily_raid_total_stats"
        unique_together = ("day", "area_name")

class RaidTotalStats(models.Model):
    area_name = fields.CharField(max_length=255)
    total = fields.BigIntField()
    total_ex_raid = fields.BigIntField()
    total_exclusive = fields.BigIntField()

    class Meta:
        table = "raid_total_stats"
        unique_together = ("area_name",)

class HourlyInvasionTotalStats(models.Model):
    area_name = fields.CharField(max_length=255)
    total_grunts = fields.IntField()
    total_confirmed = fields.IntField()
    total_unconfirmed = fields.IntField()

    class Meta:
        table = "hourly_invasion_total_stats"
        unique_together = ("area_name",)

class DailyInvasionTotalStats(models.Model):
    day = fields.DateField()
    area_name = fields.CharField(max_length=255)
    total_grunts = fields.IntField()
    total_confirmed = fields.IntField()
    total_unconfirmed = fields.IntField()

    class Meta:
        table = "daily_invasion_total_stats"
        unique_together = ("day", "area_name")

class InvasionTotalStats(models.Model):
    area_name = fields.CharField(max_length=255)
    total_grunts = fields.BigIntField()
    total_confirmed = fields.BigIntField()
    total_unconfirmed = fields.BigIntField()

    class Meta:
        table = "invasion_total_stats"
        unique_together = ("area_name",)

class StorageHourlyPokemonTthStats(models.Model):
    day_hour = fields.DatetimeField()
    area_name = fields.CharField(max_length=255)
    tth_5 = fields.IntField()
    tth_10 = fields.IntField()
    tth_15 = fields.IntField()
    tth_20 = fields.IntField()
    tth_25 = fields.IntField()
    tth_30 = fields.IntField()
    tth_35 = fields.IntField()
    tth_40 = fields.IntField()
    tth_45 = fields.IntField()
    tth_50 = fields.IntField()
    tth_55 = fields.IntField()
    tth_55_plus = fields.IntField()

    class Meta:
        table = "storage_hourly_pokemon_tth_stats"
        unique_together = ("area_name", "day_hour")

class HourlyPokemonTthStats(models.Model):
    area_name = fields.CharField(max_length=255)
    tth_5 = fields.IntField()
    tth_10 = fields.IntField()
    tth_15 = fields.IntField()
    tth_20 = fields.IntField()
    tth_25 = fields.IntField()
    tth_30 = fields.IntField()
    tth_35 = fields.IntField()
    tth_40 = fields.IntField()
    tth_45 = fields.IntField()
    tth_50 = fields.IntField()
    tth_55 = fields.IntField()
    tth_55_plus = fields.IntField()

    class Meta:
        table = "hourly_pokemon_tth_stats"
        unique_together = ("area_name",)

class DailyPokemonTthStats(models.Model):
    hour = fields.IntField()
    area_name = fields.CharField(max_length=255)
    total_tth_5 = fields.IntField()
    total_tth_10 = fields.IntField()
    total_tth_15 = fields.IntField()
    total_tth_20 = fields.IntField()
    total_tth_25 = fields.IntField()
    total_tth_30 = fields.IntField()
    total_tth_35 = fields.IntField()
    total_tth_40 = fields.IntField()
    total_tth_45 = fields.IntField()
    total_tth_50 = fields.IntField()
    total_tth_55 = fields.IntField()
    total_tth_55_plus = fields.IntField()

    class Meta:
        table = "daily_pokemon_tth_stats"
        unique_together = ("area_name", "hour")
