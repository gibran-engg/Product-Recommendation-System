"""JSON API behind the demo storefront (serving/web)."""

from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel

from app.config import RANKING_PROFILES
from app.services.catalog import CatalogService
from app.services.recommender import Recommender
from app.services.redis_client import RedisClient

router = APIRouter(prefix="/api", tags=["storefront"])

redis_client = RedisClient()
catalog = CatalogService(redis_client)
recommender = Recommender(redis_client, catalog)


class SessionEvent(BaseModel):
    user_id: str
    item_id: int
    event_type: str


@router.get("/catalog/categories")
def categories():
    return catalog.categories()


@router.get("/catalog/items")
def list_items(
    category: str | None = None,
    q: str | None = None,
    offset: int = Query(0, ge=0),
    limit: int = Query(24, ge=1, le=100),
):
    return catalog.list_items(category, q, offset, limit)


@router.get("/items/{item_id}")
def item_detail(item_id: int):
    item = recommender.item_detail(item_id)
    if item is None:
        raise HTTPException(status_code=404, detail=f"Item {item_id} not found")
    return item


@router.get("/users/demo")
def demo_users():
    return redis_client.get_json("demo:users") or []


@router.get("/users/{user_id}")
def user_detail(user_id: str):
    return recommender.user_detail(user_id)


@router.post("/events", status_code=204)
def record_event(event: SessionEvent):
    try:
        recommender.record_event(event.user_id, event.item_id, event.event_type)
    except ValueError as exc:
        raise HTTPException(status_code=422, detail=str(exc))


@router.delete("/session/{user_id}", status_code=204)
def clear_session(user_id: str):
    redis_client.clear_session(user_id)


@router.get("/recommendations/{user_id}")
def explained_recommendations(
    user_id: str,
    context_item: int | None = None,
    limit: int = Query(10, ge=1, le=50),
):
    return recommender.recommend(user_id, context_item, limit)


@router.get("/metrics")
def metrics():
    return {
        "pipeline": redis_client.get_json("metrics:pipeline"),
        "evaluation": redis_client.get_json("metrics:evaluation") or [],
        "ranking_profiles": {
            name: [{"key": k, "label": label, "weight": w} for k, label, w in factors]
            for name, factors in RANKING_PROFILES.items()
        },
    }
