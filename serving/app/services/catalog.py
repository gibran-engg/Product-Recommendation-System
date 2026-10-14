"""
Read side of the product catalog that export_serving_features.py writes to Redis.

The dataset (REES46 e-commerce events) has no product titles or images, so the
display name is derived from brand + the most specific category, e.g.
brand "sony" + "electronics.audio.headphone" -> "Sony Headphone".
"""

import time
from typing import Any

from app.config import MIN_COMPLEMENT_CATEGORIES, SEARCH_INDEX_SIZE, SEARCH_INDEX_TTL_SECONDS
from app.services.redis_client import RedisClient

Item = dict[str, Any]

# Heavy model fields stay server-side; the storefront only needs display data
PUBLIC_FIELDS = (
    "item_id", "brand", "category_code", "category_id", "price", "views", "carts",
    "purchases", "unique_users", "popularity_score", "conversion_rate",
)


# REES46 category tokens that read badly as product nouns
LABELS = {"clocks": "Watch", "tv": "TV", "acoustic": "Speaker", "hdd": "HDD", "ssd": "SSD", "cpu": "CPU"}


def humanize(token: str) -> str:
    return LABELS.get(token, token.replace("_", " ").title())


def display_name(item: Item) -> str:
    brand = humanize(item["brand"]) if item.get("brand") else "Unbranded"
    code = item.get("category_code")
    noun = humanize(code.split(".")[-1]) if code else "Product"
    return f"{brand} {noun}"


def public_item(item: Item) -> Item:
    out = {k: item.get(k) for k in PUBLIC_FIELDS}
    code = item.get("category_code")
    out["name"] = display_name(item)
    out["category_path"] = [humanize(p) for p in code.split(".")] if code else ["Other"]
    out["category_root"] = code.split(".")[0] if code else "other"
    out["has_embedding"] = bool(item.get("factors"))
    return out


class CatalogService:
    def __init__(self, redis_client: RedisClient):
        self.redis = redis_client
        self._index: list[Item] = []
        self._complements: dict[str, list[dict[str, Any]]] = {}
        self._index_loaded_at = 0.0

    def get_item(self, item_id: int) -> Item | None:
        return self.redis.get_json(f"item:{item_id}")

    def get_items(self, item_ids: list[int]) -> dict[int, Item]:
        unique = list(dict.fromkeys(item_ids))
        items = self.redis.get_many_json([f"item:{i}" for i in unique])
        return {i: item for i, item in zip(unique, items) if item}

    def categories(self) -> list[dict[str, Any]]:
        return [{**c, "label": humanize(c["key"])} for c in self.redis.get_json("catalog:categories") or []]

    def _search_index(self) -> list[Item]:
        """Lightweight public records of the most popular items, refreshed every few minutes."""
        if not self._index or time.time() - self._index_loaded_at > SEARCH_INDEX_TTL_SECONDS:
            ids = [int(i) for i in self.redis.top_of_sorted_set("catalog:popular", 0, SEARCH_INDEX_SIZE)]
            items = self.get_items(ids)
            self._index = [public_item(items[i]) for i in ids if i in items]
            self._complements = self.redis.get_json("catalog:complements") or {}
            self._index_loaded_at = time.time()
        return self._index

    def popular_in_category(self, category_code: str | None, count: int, category_id: str | None = None) -> list[int]:
        """Most popular items in one exact category (category_code, else category_id)."""
        same = [
            i["item_id"] for i in self._search_index()
            if (category_code and i["category_code"] == category_code)
            or (not category_code and i["category_id"] == category_id)
        ]
        return same[:count]

    def complement_categories(self, item: Item) -> list[dict[str, Any]]:
        """
        Categories that shoppers of this item's category also engage with, learned by
        export_serving_features.py. Too little signal (a niche category in a small
        slice) -> topped up with the most popular other categories of the same
        department, flagged kind="department" so the UI doesn't pass them off as learned.
        """
        code = item.get("category_code")
        if not code:
            return []
        self._search_index()  # also refreshes the complements cache
        result = [{**c, "kind": "learned"} for c in self._complements.get(code) or []]
        if len(result) < MIN_COMPLEMENT_CATEGORIES:
            root = code.split(".")[0]
            taken = {c["category"] for c in result} | {code}
            popularity: dict[str, float] = {}
            for i in self._search_index():
                if i["category_root"] == root and i["category_code"] and i["category_code"] not in taken:
                    popularity[i["category_code"]] = popularity.get(i["category_code"], 0.0) + (i["popularity_score"] or 0.0)
            for other in sorted(popularity, key=lambda c: -popularity[c])[: MIN_COMPLEMENT_CATEGORIES - len(result)]:
                result.append({"category": other, "score": None, "users": 0, "kind": "department"})
        return result

    def list_items(self, category: str | None, query: str | None, offset: int, limit: int) -> dict[str, Any]:
        """Popularity-ordered listing, optionally filtered by top-level category and/or a text query."""
        results = self._search_index()
        if category:
            results = [i for i in results if i["category_root"] == category]
        if query:
            terms = query.lower().split()
            results = [
                i for i in results
                if all(t in f"{i['name']} {i['category_code'] or ''} {i['item_id']}".lower() for t in terms)
            ]
        return {"total": len(results), "items": results[offset: offset + limit]}
