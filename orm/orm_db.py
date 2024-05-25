from tortoise import Tortoise
from tortoise.contrib.fastapi import register_tortoise
from config.app_config import app_config

db_config = {
    'connections': {
        'default': {
            'engine': 'tortoise.backends.mysql',
            'credentials': {
                'host': app_config.db_host,
                'port': app_config.db_port,
                'user': app_config.db_user,
                'password': app_config.db_password,
                'database': app_config.db_name,
            }
        }
    },
    'apps': {
        'models': {
            'models': ['orm.models'],
            'default_connection': 'default',
        }
    }
}

async def init():
    register_tortoise(
        None,  # You can pass an app instance here if you have one
        config=db_config,
        generate_schemas=False  # Set to True if you want to generate schemas automatically
    )
    # Can explore the code below to fully generate the database and its full schema on another time. Since it doesn't deal with versions by nature.
    # await Tortoise.generate_schemas()

async def close():
    await Tortoise.close_connections()
