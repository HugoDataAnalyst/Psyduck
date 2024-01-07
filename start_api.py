import uvicorn
from config.app_config import app_config

if __name__ == "__main__":
    uvicorn.run("api.fastapi_server:fastapi", host=app_config.api_host, port=app_config.api_port, workers=app_config.api_workers)