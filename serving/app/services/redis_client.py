import json
import os
from typing import Any

import redis


class RedisClient:
    def __init__(self) -> None:
        host = os.getenv("REDIS_HOST", "localhost")
        port = int(os.getenv("REDIS_PORT", "6379"))

        self.client = redis.Redis(
            host=host,
            port=port,
            decode_responses=True,
        )

    def ping(self) -> bool:
        return bool(self.client.ping())

    def set_recommendations(
        self,
        user_id: str,
        recommendations: list[dict[str, Any]],
    ) -> None:
        key = f"recommendations:{user_id}"
        self.client.set(key, json.dumps(recommendations))

    def set_many_recommendations(
        self,
        recommendations_by_user: dict[str, list[dict[str, Any]]],
    ) -> None:
        """Bulk version of set_recommendations: one round trip per batch, not per user."""
        self.set_many_json({f"recommendations:{user_id}": recs for user_id, recs in recommendations_by_user.items()})

    def get_recommendations(
        self,
        user_id: str,
    ) -> list[dict[str, Any]] | None:
        return self.get_json(f"recommendations:{user_id}")

    # --- Generic JSON values (serving features written by export_serving_features.py)

    def set_many_json(self, values_by_key: dict[str, Any]) -> None:
        pipe = self.client.pipeline(transaction=False)
        for key, value in values_by_key.items():
            pipe.set(key, json.dumps(value))
        pipe.execute()

    def get_json(self, key: str) -> Any | None:
        data = self.client.get(key)
        return None if data is None else json.loads(data)

    def get_many_json(self, keys: list[str]) -> list[Any | None]:
        if not keys:
            return []
        return [None if data is None else json.loads(data) for data in self.client.mget(keys)]

    def replace_sorted_set(self, key: str, scores: dict[str, float]) -> None:
        pipe = self.client.pipeline(transaction=True)
        pipe.delete(key)
        if scores:
            pipe.zadd(key, scores)
        pipe.execute()

    def top_of_sorted_set(self, key: str, start: int, count: int) -> list[str]:
        return self.client.zrevrange(key, start, start + count - 1)

    # --- Live session (written by the storefront through POST /events)

    def push_session_event(self, user_id: str, event: dict[str, Any], max_len: int, ttl_seconds: int) -> None:
        key = f"session:{user_id}"
        pipe = self.client.pipeline(transaction=True)
        pipe.lpush(key, json.dumps(event))
        pipe.ltrim(key, 0, max_len - 1)
        pipe.expire(key, ttl_seconds)
        pipe.execute()

    def get_session_events(self, user_id: str) -> list[dict[str, Any]]:
        """Most recent first."""
        return [json.loads(e) for e in self.client.lrange(f"session:{user_id}", 0, -1)]

    def clear_session(self, user_id: str) -> None:
        self.client.delete(f"session:{user_id}")
