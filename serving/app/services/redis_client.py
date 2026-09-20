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
        pipe = self.client.pipeline(transaction=False)
        for user_id, recommendations in recommendations_by_user.items():
            pipe.set(f"recommendations:{user_id}", json.dumps(recommendations))
        pipe.execute()

    def get_recommendations(
        self,
        user_id: str,
    ) -> list[dict[str, Any]] | None:
        key = f"recommendations:{user_id}"
        data = self.client.get(key)

        if data is None:
            return None

        return json.loads(data)