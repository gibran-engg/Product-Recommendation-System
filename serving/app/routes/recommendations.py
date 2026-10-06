from fastapi import APIRouter, HTTPException

from app.services.redis_client import RedisClient


router = APIRouter(
    prefix="/recommendations",
    tags=["recommendations"],
)

redis_client = RedisClient()


@router.get("/{user_id}")
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