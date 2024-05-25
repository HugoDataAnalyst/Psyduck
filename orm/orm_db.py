from tortoise import Tortoise
from config.app_config import app_config

async def init():
    db_url = f"mysql://{app_config.db_user}:{app_config.db_password}@{app_config.db_host}:{app_config.db_port}/{app_config.db_name}"
    await Tortoise.init(
        db_url=db_url,
        modules={'models': ['orm.models']}
    )
    # Can explore the code below to fully generate the database and its full schema on another time. Since it doesn't deal with versions by nature.
    # await Tortoise.generate_schemas()

async def close():
    await Tortoise.close_connections()
