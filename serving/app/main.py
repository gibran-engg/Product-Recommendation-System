import os

import redis
from fastapi import FastAPI, Request
from fastapi.responses import FileResponse, JSONResponse
from fastapi.staticfiles import StaticFiles

from app.config import WEB_DIR
from app.routes.recommendations import router as recommendations_router
from app.routes.recommendations import redis_client
from app.routes.shop import router as shop_router


app = FastAPI(
    title="Product Recommendation System API",
    version="0.3.0",
)


@app.exception_handler(redis.exceptions.ConnectionError)
def redis_unavailable(request: Request, exc: redis.exceptions.ConnectionError):
    return JSONResponse(status_code=503, content={"detail": "Redis is unreachable — is the stack running?"})


@app.get("/api")
def root():
    return {
        "service": "recommendation-api",
        "status": "running",
    }


@app.get("/health")
def health():
    try:
        redis_ok = redis_client.ping()
    except redis.exceptions.RedisError:
        redis_ok = False
    features_loaded = redis_ok and redis_client.get_json("metrics:pipeline") is not None
    return {
        "status": "healthy" if redis_ok else "degraded",
        "redis": redis_ok,
        "features_loaded": features_loaded,
    }


app.include_router(recommendations_router)
app.include_router(shop_router)

# Demo storefront: a static single-page app served by the API itself, so it
# needs no separate build step or container
if os.path.isdir(WEB_DIR):
    app.mount("/static", StaticFiles(directory=WEB_DIR), name="static")

    @app.get("/", include_in_schema=False)
    def storefront():
        return FileResponse(os.path.join(WEB_DIR, "index.html"))
