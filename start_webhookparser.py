import uvicorn
import asyncio
from config.app_config import app_config
from orm.orm_db import init as orm_init, close as orm_close

if __name__ == "__main__":
    # Initialize the ORM
    loop = asyncio.get_event_loop()
    loop.run_until_complete(orm_init())

    # Start the webhook processor and ensure ORM connections are closed when the server stops
    try:
        uvicorn.run("receiver.webhookparser:webhook_processor", host=app_config.receiver_host, port=app_config.receiver_port, workers=app_config.receiver_workers)
    finally:
        loop.run_until_complete(orm_close())
