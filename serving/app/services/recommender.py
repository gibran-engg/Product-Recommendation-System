"""
Candidate generation + hybrid re-ranking for the storefront.

Candidates (first source wins when an item comes from several):
    1. items co-occurring in sessions with the product being viewed  ("copurchase")
    2. nearest neighbours of that product in ALS embedding space     ("similar")
    3. popular items in the same category as that product           ("category")
       + popular items in its complementary categories               ("complement")
    4. the user's precomputed batch top-N from Redis                 ("als" / "content")
    5. neighbours of the items clicked in the live session           ("session")
    6. the most popular items                                        ("popular")
Then app.services.ranker scores all of them for this user + session.
"""

import time
from datetime import datetime, timezone
from typing import Any

from app.config import (
    COMPLEMENT_CATEGORIES_USED, DEPARTMENT_FALLBACK_SCORE, EVENT_WEIGHTS, ITEMS_PER_COMPLEMENT,
    SESSION_MAX_EVENTS, SESSION_TTL_SECONDS,
)
from app.services import ranker
from app.services.catalog import CatalogService, public_item
from app.services.redis_client import RedisClient

POPULAR_CANDIDATES = 30
CATEGORY_CANDIDATES = 20
SESSION_ANCHORS = 3
NEIGHBOURS_PER_ANCHOR = 6

STRATEGIES = {
    "als": "Warm user: ALS collaborative filtering candidates",
    "content": "Cold-start user (<= 3 events): TF-IDF content-based candidates",
    "none": "New visitor: popularity + live-session candidates",
}


class Recommender:
    def __init__(self, redis_client: RedisClient, catalog: CatalogService):
        self.redis = redis_client
        self.catalog = catalog

    # --- Session -----------------------------------------------------------

    def record_event(self, user_id: str, item_id: int, event_type: str) -> None:
        if event_type not in EVENT_WEIGHTS:
            raise ValueError(f"event_type must be one of {sorted(EVENT_WEIGHTS)}")
        event = {"item_id": item_id, "event_type": event_type,
                 "at": datetime.now(timezone.utc).isoformat(timespec="seconds")}
        self.redis.push_session_event(user_id, event, SESSION_MAX_EVENTS, SESSION_TTL_SECONDS)

    def session(self, user_id: str) -> list[tuple[dict[str, Any], ranker.Item]]:
        """(event, item) pairs, most recent first."""
        events = self.redis.get_session_events(user_id)
        items = self.catalog.get_items([e["item_id"] for e in events])
        return [(e, items[e["item_id"]]) for e in events if e["item_id"] in items]

    # --- Recommendations ----------------------------------------------------

    def recommend(self, user_id: str, context_item_id: int | None, limit: int) -> dict[str, Any]:
        started = time.perf_counter()
        user = self.redis.get_json(f"user:{user_id}")
        precomputed = self.redis.get_recommendations(user_id) or []
        session = self.session(user_id)
        session_items = [(item, e["event_type"]) for e, item in session]
        context = self.catalog.get_item(context_item_id) if context_item_id is not None else None
        complements = self.complements(context) if context else []

        sources: dict[int, str] = {}

        def add(item_ids, source):
            for item_id in item_ids:
                sources.setdefault(item_id, source)

        if context:
            add([n["item_id"] for n in context.get("also") or []], "copurchase")
            add([n["item_id"] for n in context.get("similar") or []], "similar")
            # Long-tail products have few or no neighbours in the event slice
            add(self.catalog.popular_in_category(context.get("category_code"), CATEGORY_CANDIDATES,
                                                 context.get("category_id")), "category")
            for c in complements[:COMPLEMENT_CATEGORIES_USED]:
                add(self.catalog.popular_in_category(c["category"], ITEMS_PER_COMPLEMENT), "complement")
        add([r["item_id"] for r in precomputed], precomputed[0].get("source", "als") if precomputed else "als")
        for item, _ in session_items[:SESSION_ANCHORS]:
            add([n["item_id"] for n in (item.get("also") or [])[:NEIGHBOURS_PER_ANCHOR]], "session")
            add([n["item_id"] for n in (item.get("similar") or [])[:NEIGHBOURS_PER_ANCHOR]], "session")
        add([int(i) for i in self.redis.top_of_sorted_set("catalog:popular", 0, POPULAR_CANDIDATES)], "popular")

        # Never recommend what's on screen, what was just clicked, or what the user already has
        exclude = {e["item_id"] for e, _ in session} | set((user or {}).get("seen") or [])
        if context:
            exclude.add(context["item_id"])
        candidate_items = self.catalog.get_items([i for i in sources if i not in exclude])

        ctx = ranker.RankingContext(user, session_items, context, {c["category"]: c["value"] for c in complements})
        ranked = ranker.rank(ctx, [(item, sources[i]) for i, item in candidate_items.items()], limit)

        strategy = precomputed[0].get("source", "als") if precomputed else "none"
        return {
            "user_id": user_id,
            "strategy": strategy,
            "strategy_label": STRATEGIES[strategy],
            "context_item": public_item(context) if context else None,
            "complements": complements,
            "session_events": len(session),
            "candidates_considered": len(candidate_items),
            "recommendations": [{**r, "item": public_item(r["item"])} for r in ranked],
            "latency_ms": round((time.perf_counter() - started) * 1000, 1),
        }

    def complements(self, item: ranker.Item) -> list[dict[str, Any]]:
        """Complementary categories with a 0..1 "value": learned ones scaled to the strongest, fallbacks fixed."""
        categories = self.catalog.complement_categories(item)
        best = max((c["score"] for c in categories if c["kind"] == "learned"), default=0.0)
        return [
            {**c, "value": round(c["score"] / best, 4) if c["kind"] == "learned" and best else DEPARTMENT_FALLBACK_SCORE}
            for c in categories
        ]

    # --- Item & user views ----------------------------------------------------

    def item_detail(self, item_id: int) -> dict[str, Any] | None:
        item = self.catalog.get_item(item_id)
        if not item:
            return None
        neighbours = (item.get("similar") or []) + (item.get("also") or [])
        others = self.catalog.get_items([n["item_id"] for n in neighbours])

        def expand(ns):
            return [{**n, "item": public_item(others[n["item_id"]])} for n in ns or [] if n["item_id"] in others]

        return {
            **public_item(item),
            "tokens": item.get("tokens") or {},
            "embedding_dims": len(item.get("factors") or []),
            "similar": expand(item.get("similar")),
            "also": expand(item.get("also")),
            "complements": self.complements(item),
        }

    def user_detail(self, user_id: str) -> dict[str, Any]:
        user = self.redis.get_json(f"user:{user_id}")
        session = self.session(user_id)
        precomputed = self.redis.get_recommendations(user_id) or []
        recent_ids = [r["item_id"] for r in (user or {}).get("recent") or []]
        recent_items = self.catalog.get_items(recent_ids)
        strategy = precomputed[0].get("source", "als") if precomputed else "none"

        profile = None
        if user:
            profile = {k: v for k, v in user.items() if k not in ("factors", "seen", "recent", "profile")}
            profile["recent"] = [
                {**r, "item": public_item(recent_items[r["item_id"]])}
                for r in user.get("recent") or [] if r["item_id"] in recent_items
            ]
            profile["profile_tokens"] = user.get("profile") or {}
            profile["embedding_dims"] = len(user.get("factors") or [])
        return {
            "user_id": user_id,
            "known": user is not None,
            "strategy": strategy,
            "strategy_label": STRATEGIES[strategy],
            "precomputed_count": len(precomputed),
            "profile": profile,
            "session": [{**e, "item": public_item(item)} for e, item in session],
        }
