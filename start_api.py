from config.app_config import app_config
import uvicorn
import subprocess
import asyncio
import logging
import signal

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("start_api")

async def start_scheduler():
    try:
        process = await asyncio.create_subprocess_exec(
            app_config.schedule_python_command, app_config.schedule_script_path,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE
        )
        return process
    except Exception as e:
        logger.error(f"Failed to start scheduler: {e}")
        return None

async def shutdown(scheduler_process, server):
    logger.info("Shutting down gracefully...")
    if scheduler_process:
        logger.info("Terminating scheduler process...")
        scheduler_process.terminate()
        await scheduler_process.wait()
        logger.info("Scheduler process terminated.")

    logger.info("Stopping FastAPI server...")
    await server.shutdown()
    logger.info("FastAPI server stopped.")
    logger.info("Shutdown complete.")

async def main():
    # Start the APScheduler
    scheduler_process = await start_scheduler()

    if scheduler_process:
        logger.info("Scheduler started successfully")
    else:
        logger.warning("Scheduler failed to start. FastAPI server will still run.")

    # Start the FastAPI server
    config = uvicorn.Config("api.fastapi_server:fastapi", host=app_config.api_host, port=app_config.api_port, workers=app_config.api_workers)
    server = uvicorn.Server(config)

    loop = asyncio.get_event_loop()

    # Register signal handlers for graceful shutdown
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, lambda: asyncio.create_task(shutdown(scheduler_process, server)))

    if scheduler_process:
        # Run both the scheduler and the FastAPI server
        await asyncio.gather(
            server.serve(),  # This starts the FastAPI server
            scheduler_process.wait()  # This waits for the scheduler process to complete
        )
    else:
        # Run only the FastAPI server if scheduler failed to start
        await server.serve()  # This starts the FastAPI server

if __name__ == "__main__":
    asyncio.run(main())
