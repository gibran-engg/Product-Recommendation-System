"""Unit tests for the serving layer's hybrid re-ranker (pure functions, no Redis)."""

import math

import pytest

from app.config import RANKING_PROFILES
from app.services import ranker


def make_item(item_id, category_code="electronics.smartphone", brand="samsung", price=200.0, tokens=None,
              factors=None, popularity=10.0, also=None):
    return {
        "item_id": item_id,
        "category_code": category_code,
        "category_id": "1",
        "brand": brand,
        "price": price,
        "tokens": tokens if tokens is not None else {f"cat:{category_code}": 0.8, f"brand:{brand}": 0.6},
        "factors": factors,
        "popularity_score": popularity,
        "also": also or [],
    }


@pytest.mark.parametrize("profile", sorted(RANKING_PROFILES))
def test_weights_sum_to_one(profile):
    assert sum(w for _, _, w in RANKING_PROFILES[profile]) == pytest.approx(1.0)


def test_product_page_prefers_items_like_the_one_on_screen():
    """A phone buyer (strong ALS on a phone) opening a vacuum should be shown vacuums first."""
    user = {"factors": [1.0, 0.0]}
    vacuum = make_item(10, "appliances.environment.vacuum", "bosch", price=150.0)
    other_vacuum = make_item(1, "appliances.environment.vacuum", "samsung", price=140.0, factors=[0.1, 0.0])
    phone = make_item(2, "electronics.smartphone", "huawei", price=150.0, factors=[0.9, 0.0])

    results = ranker.rank(ranker.RankingContext(user, [], vacuum), [(phone, "als"), (other_vacuum, "category")], 2)
    assert results[0]["item"]["item_id"] == 1


def test_cosine():
    assert ranker.cosine({"a": 1.0}, {"a": 2.0}) == pytest.approx(1.0)
    assert ranker.cosine({"a": 1.0}, {"b": 1.0}) == 0.0
    assert ranker.cosine(None, {"a": 1.0}) == 0.0


def test_final_score_is_weighted_sum_and_contributions_add_up():
    user = {"factors": [1.0, 0.0], "profile": {"cat:electronics.smartphone": 1.0},
            "categories": [{"key": "electronics.smartphone", "share": 1.0}],
            "brands": [{"key": "samsung", "share": 1.0}], "avg_price": 200.0}
    ctx = ranker.RankingContext(user, [], None)
    results = ranker.rank(ctx, [(make_item(1, factors=[0.5, 0.0]), "als")], limit=5)

    r = results[0]
    expected = sum(f["weight"] * f["value"] for f in r["factors"])
    assert r["score"] == pytest.approx(expected, abs=1e-3)
    assert sum(f["contribution"] for f in r["factors"]) == pytest.approx(1.0, abs=1e-3)
    assert 0.0 <= r["score"] <= 1.0


def test_session_click_promotes_matching_category():
    phone = make_item(1, "electronics.smartphone", "apple")
    fridge = make_item(2, "appliances.kitchen.refrigerators", "lg")
    clicked = make_item(3, "electronics.smartphone", "xiaomi")

    without = ranker.rank(ranker.RankingContext(None, [], None), [(fridge, "popular"), (phone, "popular")], 2)
    with_session = ranker.rank(ranker.RankingContext(None, [(clicked, "cart")], None), [(fridge, "popular"), (phone, "popular")], 2)

    def session_value(results, item_id):
        r = next(r for r in results if r["item"]["item_id"] == item_id)
        return next(f["value"] for f in r["factors"] if f["key"] == "session")

    assert session_value(without, 1) == 0.0
    assert session_value(with_session, 1) > session_value(with_session, 2)
    assert with_session[0]["item"]["item_id"] == 1


def test_copurchase_uses_context_item_neighbours():
    context = make_item(10, also=[{"item_id": 1, "score": 0.9, "sessions": 4}])
    ctx = ranker.RankingContext(None, [], context)
    results = ranker.rank(ctx, [(make_item(1), "copurchase"), (make_item(2), "popular")], 2)
    values = {r["item"]["item_id"]: next(f["value"] for f in r["factors"] if f["key"] == "copurchase") for r in results}
    assert values == {1: 0.9, 2: 0.0}


def test_price_fit():
    ctx = ranker.RankingContext({"avg_price": 100.0}, [], None)
    same = ranker.raw_factors(ctx, make_item(1, price=100.0))["price"]
    double = ranker.raw_factors(ctx, make_item(2, price=200.0))["price"]
    assert same == pytest.approx(1.0)
    assert double == pytest.approx(0.5)


def test_als_is_clipped_not_scaled_to_best_candidate():
    user = {"factors": [1.0, 1.0]}
    items = [(make_item(1, factors=[0.2, 0.2]), "als"), (make_item(2, factors=[0.1, 0.1]), "als"),
             (make_item(3, factors=[0.8, 0.8]), "als")]
    results = ranker.rank(ranker.RankingContext(user, [], None), items, 3)
    als = {r["item"]["item_id"]: next(f["value"] for f in r["factors"] if f["key"] == "als") for r in results}
    # a weak best match keeps its raw value instead of being inflated to 1.0
    assert als[1] == pytest.approx(0.4)
    assert als[2] == pytest.approx(0.2)
    assert als[3] == pytest.approx(1.0)  # raw 1.6 is clipped
    assert results[0]["als_prediction"] == pytest.approx(1.6)


def test_guest_without_history_gets_zero_personal_factors():
    results = ranker.rank(ranker.RankingContext(None, [], None), [(make_item(1), "popular")], 1)
    values = {f["key"]: f["value"] for f in results[0]["factors"]}
    assert values["als"] == values["content"] == values["session"] == values["brand"] == 0.0
    assert values["popularity"] == pytest.approx(1.0)
    assert not math.isnan(results[0]["score"])


def test_complement_factor_comes_from_learned_categories():
    phone = make_item(10, "electronics.smartphone", "apple")
    headphone = make_item(1, "electronics.audio.headphone", "sony")
    ctx = ranker.RankingContext(None, [], phone, {"electronics.audio.headphone": 1.0})
    values = ranker.raw_factors(ctx, headphone)
    assert values["complement"] == 1.0
    assert ranker.raw_factors(ctx, make_item(2, "appliances.kitchen.kettle"))["complement"] == 0.0


def test_product_page_mixes_complements_between_substitutes():
    """Six phones outscore one pair of headphones, but variety brings the headphones into the top 3."""
    phone = make_item(10, "electronics.smartphone", "apple", price=500.0)
    phones = [(make_item(i, "electronics.smartphone", "samsung", price=500.0), "category") for i in range(1, 7)]
    headphone = make_item(99, "electronics.audio.headphone", "sony", price=100.0)
    ctx = ranker.RankingContext(None, [], phone, {"electronics.audio.headphone": 1.0})

    results = ranker.rank(ctx, phones + [(headphone, "complement")], limit=6)

    assert results[0]["item"]["category_code"] == "electronics.smartphone"
    assert 99 in [r["item"]["item_id"] for r in results[:3]]
    headphone_score = next(r["score"] for r in results if r["item"]["item_id"] == 99)
    assert headphone_score < results[0]["score"]  # Match % stays the raw score; only the order changes
    second_phone = next(r for r in results[1:] if r["item"]["category_code"] == "electronics.smartphone")
    assert second_phone["variety"]["same_category_before"] == 1
    assert second_phone["variety"]["multiplier"] < 1
