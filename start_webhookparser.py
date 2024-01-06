import uvicorn
from app_config import app_config

if __name__ == "__main__":
    uvicorn.run("webhookparser:webhook_processor", host=app_config.receiver_host, port=app_config.receiver_port, reload=app_config.receiver_reload)