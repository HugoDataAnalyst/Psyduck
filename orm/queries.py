from tortoise.transactions import in_transaction
from orm.models import PokemonSighting, QuestSighting, RaidSighting, InvasionSighting

class DatabaseOperations:

    async def insert_pokemon_data(self, data_batch):
        async with in_transaction() as conn:
            await PokemonSighting.bulk_create([PokemonSighting(**data) for data in data_batch])

    async def insert_quest_data(self, data_batch):
        async with in_transaction() as conn:
            await QuestSighting.bulk_create([QuestSighting(**data) for data in data_batch])

    async def insert_raid_data(self, data_batch):
        async with in_transaction() as conn:
            await RaidSighting.bulk_create([RaidSighting(**data) for data in data_batch])

    async def insert_invasion_data(self, data_batch):
        async with in_transaction() as conn:
            await InvasionSighting.bulk_create([InvasionSighting(**data) for data in data_batch])
