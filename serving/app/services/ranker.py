"""
Hybrid re-ranker: scores candidate items for a user with the features the
batch pipeline exported (export_serving_features.py) plus the live session.

Every factor is in [0, 1]; the final score is their weighted sum (weights in
config.RANKING_PROFILES sum to 1 per profile). It is a blended relevance score, not a
calibrated click probability. Pure functions — no Redis — so it is unit-testable.
"""

import math
from typing import Any

from app.config import DIVERSITY_DECAY, EVENT_WEIGHTS, RANKING_PROFILES, SESSION_DECAY

Item = dict[str, Any]


def cosine(a: dict[str, float] | None, b: dict[str, float] | None) -> float:
    if not a or not b:
        return 0.0
    num = sum(w * b[t] for t, w in a.items() if t in b)
    den = math.sqrt(sum(w * w for w in a.values())) * math.sqrt(sum(w * w for w in b.values()))
    return num / den if den else 0.0


def dot(a: list[float] | None, b: list[float] | None) -> float | None:
    if not a or not b:
        return None
    return sum(x * y for x, y in zip(a, b))


def category_root(item: Item) -> str | None:
    code = item.get("category_code")
    return code.split(".")[0] if code else None


def item_category(item: Item) -> str:
    """Same category_code -> category_id fallback as the batch job."""
    return item.get("category_code") or f"cid:{item.get('category_id')}"


def session_profile(session_items: list[tuple[Item, str]]) -> dict[str, float]:
    """TF-IDF profile of the live session, newest events weighted most. session_items: most recent first."""
    profile: dict[str, float] = {}
    for position, (item, event_type) in enumerate(session_items):
        w = EVENT_WEIGHTS.get(event_type, 1) * SESSION_DECAY ** position
        for token, weight in (item.get("tokens") or {}).items():
            profile[token] = profile.get(token, 0.0) + w * weight
    return profile


def shares_from_session(session_items: list[tuple[Item, str]], key) -> dict[str, float]:
    totals: dict[str, float] = {}
    for item, event_type in session_items:
        k = key(item)
        if k:
            totals[k] = totals.get(k, 0.0) + max(EVENT_WEIGHTS.get(event_type, 1), 1)
    s = sum(totals.values())
    return {k: v / s for k, v in totals.items()} if s else {}


class RankingContext:
    """Everything about the viewer that doesn't depend on the candidate — built once per request."""

    def __init__(self, user: Item | None, session_items: list[tuple[Item, str]], context_item: Item | None,
                 complements: dict[str, float] | None = None):
        self.user = user or {}
        # category -> 0..1: how strongly shoppers of the product on screen also engage with that category
        self.complements = complements or {}
        self.session_items = session_items
        self.context_item = context_item
        self.session_profile = session_profile(session_items)

        user_categories = {c["key"]: c["share"] for c in self.user.get("categories") or []}
        user_brands = {b["key"]: b["share"] for b in self.user.get("brands") or []}
        # No batch history (guest / brand-new user): the session stands in for it
        self.categories = user_categories or shares_from_session(session_items, item_category)
        self.brands = user_brands or shares_from_session(session_items, lambda i: i.get("brand"))
        self.category_roots: dict[str, float] = {}
        for code, share in self.categories.items():
            root = code.split(".")[0]
            self.category_roots[root] = self.category_roots.get(root, 0.0) + share

        prices = [i["price"] for i, _ in session_items if i.get("price")]
        if context_item and context_item.get("price"):
            self.reference_price = context_item["price"]
        elif self.user.get("avg_price"):
            self.reference_price = self.user["avg_price"]
        else:
            self.reference_price = sum(prices) / len(prices) if prices else None

        # Co-purchase anchors: the product being viewed, else the latest session / history items
        anchors: list[Item] = []
        if context_item:
            anchors.append(context_item)
        anchors += [i for i, _ in session_items[:5]]
        self.copurchase: dict[int, float] = {}
        for anchor in anchors:
            for n in anchor.get("also") or []:
                self.copurchase[n["item_id"]] = max(self.copurchase.get(n["item_id"], 0.0), n["score"])


def raw_factors(ctx: RankingContext, item: Item) -> dict[str, float | None]:
    """Un-normalised factor values; ALS and popularity are scaled across the candidate set afterwards."""
    price = item.get("price")
    price_fit = 0.0
    if price and ctx.reference_price:
        price_fit = math.exp(-abs(math.log(price / ctx.reference_price)))  # 1 at same price, 0.5 at 2x / half

    exact = ctx.categories.get(item_category(item), 0.0)
    root = ctx.category_roots.get(category_root(item) or "", 0.0)

    als = dot(ctx.user.get("factors"), item.get("factors"))
    context = 0.0
    if ctx.context_item:
        # TF-IDF over category path + brand: shared full category path scores highest
        context = cosine(ctx.context_item.get("tokens"), item.get("tokens"))
    return {
        "context": context,
        "complement": ctx.complements.get(item_category(item), 0.0),
        "als": max(als, 0.0) if als is not None else None,
        "copurchase": ctx.copurchase.get(item["item_id"], 0.0),
        "content": cosine(ctx.user.get("profile"), item.get("tokens")),
        "session": cosine(ctx.session_profile, item.get("tokens")),
        "category": min(1.0, exact + 0.5 * (root - exact)),  # half credit for the same top-level category
        "price": price_fit,
        "brand": ctx.brands.get(item.get("brand") or "", 0.0),
        "popularity": math.log1p(item.get("popularity_score") or 0.0),
    }


def diversify(results: list[dict[str, Any]], decay: float, limit: int) -> list[dict[str, Any]]:
    """
    Greedy variety pass: repeatedly pick the item with the best score x decay^(items
    already picked from its category). Records the multiplier so the UI can show
    why a higher-scoring item was placed below a lower-scoring one.
    """
    picked: list[dict[str, Any]] = []
    shown: dict[str, int] = {}
    remaining = list(results)
    while remaining and len(picked) < limit:
        def adjusted(r):
            return r["score"] * decay ** shown.get(item_category(r["item"]), 0)
        best = max(remaining, key=lambda r: (adjusted(r), -r["item"]["item_id"]))
        category = item_category(best["item"])
        best["variety"] = {"same_category_before": shown.get(category, 0),
                           "multiplier": round(decay ** shown.get(category, 0), 4),
                           "rank_score": round(adjusted(best), 4)}
        picked.append(best)
        remaining.remove(best)
        shown[category] = shown.get(category, 0) + 1
    return picked


def rank(ctx: RankingContext, candidates: list[tuple[Item, str]], limit: int) -> list[dict[str, Any]]:
    """candidates: (item, candidate source). Returns the top `limit` with a per-factor breakdown."""
    profile_name = "product" if ctx.context_item else "home"
    profile = RANKING_PROFILES[profile_name]
    scored = [(item, source, raw_factors(ctx, item)) for item, source in candidates]
    max_als = max((f["als"] or 0.0 for _, _, f in scored), default=0.0)
    max_pop = max((f["popularity"] for _, _, f in scored), default=0.0)

    results = []
    for item, source, f in scored:
        raw_als = f["als"]
        values = {**f, "als": (raw_als or 0.0) / max_als if max_als else 0.0,
                  "popularity": f["popularity"] / max_pop if max_pop else 0.0}
        final = sum(weight * values[key] for key, _, weight in profile)
        factors = [
            {
                "key": key,
                "label": label,
                "weight": weight,
                "value": round(values[key], 4),
                "contribution": round(weight * values[key] / final, 4) if final else 0.0,
            }
            for key, label, weight in profile
        ]
        results.append({
            "item": item,
            "source": source,
            "score": round(final, 4),
            "als_prediction": round(raw_als, 4) if raw_als is not None else None,
            "factors": factors,
        })

    results.sort(key=lambda r: (-r["score"], r["item"]["item_id"]))
    return diversify(results, DIVERSITY_DECAY[profile_name], limit)
