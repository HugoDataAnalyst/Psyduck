import asyncio
import signal
import uvicorn
from appscheduler.scheduler import run_scheduler
from config.app_config import app_config

async def start_api():
    config = uvicorn.Config("api.fastapi_server:fastapi", host=app_config.api_host, port=app_config.api_port, workers=app_config.api_workers)
    server = uvicorn.Server(config)
    await server.serve()

async def main():
    scheduler_task = asyncio.create_task(run_scheduler())
    api_task = asyncio.create_task(start_api())

    await asyncio.wait([scheduler_task, api_task], return_when=asyncio.FIRST_COMPLETED)

def handle_signal(sig, frame):
    print("Shutting down...")
    for task in asyncio.all_tasks():
        task.cancel()

if __name__ == "__main__":
    signal.signal(signal.SIGINT, handle_signal)
    signal.signal(signal.SIGTERM, handle_signal)

    try:
        asyncio.run(main())
    except asyncio.CancelledError:
        pass
