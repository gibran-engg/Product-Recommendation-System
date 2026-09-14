from fastapi import FastAPI, HTTPException

from app.services.redis_client import RedisClient

app = FastAPI(
    title="Product Recommendation System API",
    version="0.2.0",
)

redis_client = RedisClient()


@app.get("/")
def root():
    return {
        "service": "recommendation-api",
        "status": "running",
    }


@app.get("/health")
def health():
    return {
        "status": "healthy",
    }


@app.get("/recommendations/{user_id}")
def get_recommendations(user_id: str):
    recommendations = redis_client.get_recommendations(user_id)

    if recommendations is None:
        raise HTTPException(
            status_code=404,
            detail=f"No recommendations found for user {user_id}",
        )

    return {
        "user_id": user_id,
        "recommendations": recommendations,
    }