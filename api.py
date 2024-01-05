import uvicorn

if __name__ == "__main__":
    uvicorn.run("fastapi_server:fastapi", host="app_config.api_host", port=app_config.api_port, reload=app_config.api_reload)