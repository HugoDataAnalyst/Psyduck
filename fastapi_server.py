from fastapi import FastAPI, HTTPException, Depends, Header, Request
from starlette_cache.backends.memory import MemoryBackend
from starlette_cache.middleware import CacheMiddleware
from celery.result import AsyncResult
from app_config import app_config
from tasks import query_daily_pokemon_stats, query_weekly_pokemon_stats, query_monthly_pokemon_stats
import asyncio

app = FastAPI()

app.add_middleware(CacheMiddleware, backend=MemoryBackend(), prefix="api-cache", ttl=3600)

def validate_secret_header(secret: str = Header(None)):
    if not secret or secret != app_config.api_secret_header_key:
        raise HTTPException(status_code=403, detail="Unauthorized access")

def validate_secret(secret: str = None):
    if not secret or secret != app_config.api_secret_key:
        raise HTTPException(status_code=403, detail="Unauthorized access")

def validate_ip(request: Request):
    client_host = request.client.host
    if app_config.api_ip_restriction and client_host not in app_config.api_allowed_ips:
        raise HTTPException(status_code=403, detail="Access denied")


async def get_task_result(task_function, *args, **kwargs):
    result = task_function.delay(*args, **kwargs)
    return await result.get(timeout=10)

@app.get("/api/daily-area-pokemon-stats")
async def daily_pokemon_stats(request: Request, secret: str = Depends(validate_secret)):
    cached_response = await request.app.state.cache.get(request.url.path)
    if cached_response:
        return cached_response

    response_data = await get_task_result(query_daily_pokemon_stats)
    await request.app.state.cache.set(request.url.path, response_data, ttl=app_config.api_daily_cache)
    return response_data

@app.get("/api/weekly-area-pokemon-stats")
async def weekly_pokemon_stats(request: Request, secret: str = Depends(validate_secret)):
    cached_response = await request.app.state.cache.get(request.url.path)
    if cached_response:
        return cached_response

    response_data = await get_task_result(query_weekly_pokemon_stats)
    await request.app.state.cache.set(request.url.path, response_data, ttl=app_config.api_weekly_cache)
    return response_data

@app.get("/api/monthly-area-pokemon-stats")
async def monthly_pokemon_stats(request: Request, secret: str = Depends(validate_secret)):
    cached_response = await request.app.state.cache.get(request.url.path)
    if cached_response:
        return cached_response

    response_data = await get_task_result(query_monthly_pokemon_stats)
    await request.app.state.cache.set(request.url.path, response_data, ttl=app_config.api_monthly_cache)
    return response_data


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=app_config.api_port)