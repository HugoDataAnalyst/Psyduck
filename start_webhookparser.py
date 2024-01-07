import uvicorn
from config.app_config import app_config

if __name__ == "__main__":
    uvicorn.run("receiver.webhookparser:webhook_processor", host=app_config.receiver_host, port=app_config.receiver_port, workers=app_config.receiver_workers)