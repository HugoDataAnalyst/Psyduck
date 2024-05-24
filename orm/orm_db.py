from tortoise import Tortoise, run_async
from config.app_config import app_config

async def init():
    await Tortoise.init(
        db_url='mysql://app_config.db_host:app_config.db_password@app_config.host:app_config.port/app_config.db_name',
        modules={'models': ['orm.models']}
    )
    await Tortoise.generate_schemas()

async def close():
    await Tortoise.close_connections()

run_async(init())
