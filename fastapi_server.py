from fastapi import FastAPI, HTTPException, Depends, Header, Request
from fastapi_cache import FastAPICache
from fastapi_cache.backends.redis import RedisBackend
from fastapi_cache.decorator import cache
from redis import asyncio as aioredis
from celery.result import AsyncResult
from app_config import app_config
from tasks import query_daily_pokemon_stats, query_weekly_pokemon_stats, query_monthly_pokemon_stats


app = FastAPI()

@app.on_event("startup")
async def startup():
    redis = aioredis.from_url(app_config.redis_url, encoding="utf8", decode_responses=True)
    FastAPICache.init(RedisBackend(redis), prefix="fastapi-cache")

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
@cache(expire=app_config.api_daily_cache)
async def daily_pokemon_stats(secret: str = Depends(validate_secret)):
    return await get_task_result(query_daily_pokemon_stats)

@app.get("/api/weekly-area-pokemon-stats")
@cache(expire=app_config.api_weekly_cache)
async def weekly_pokemon_stats(secret: str = Depends(validate_secret)):
    return await get_task_result(query_weekly_pokemon_stats)

@app.get("/api/monthly-area-pokemon-stats")
@cache(expire=app_config.api_monthly_cache)
async def monthly_pokemon_stats(secret: str = Depends(validate_secret)):
    return await get_task_result(query_monthly_pokemon_stats)


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=app_config.api_port)