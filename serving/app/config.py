import os

# Hybrid re-ranker (features.md "Final Architecture"): ALS candidates re-scored
# with behavioural, session and content features. Weights sum to 1, so the final
# score stays in [0, 1] and each factor's share of it is directly readable.
# (key, label shown in the UI, weight)
#
# Two profiles: the home feed is about the *user*; a product page is about the
# product on screen, so similarity to it and session co-purchase lead there and
# the user's long-term taste (ALS) only breaks ties. Without this, a phone buyer
# opening a vacuum got their phone recommendations back.
RANKING_PROFILES = {
    "home": [
        ("als", "Base ALS Score", 0.30),
        ("copurchase", "Item Co-purchase Factor", 0.18),
        ("content", "User-Item Similarity", 0.14),
        ("session", "Session Intent Boost", 0.14),
        ("category", "Category Affinity", 0.10),
        ("price", "Price Range Fit", 0.06),
        ("brand", "Brand Affinity", 0.05),
        ("popularity", "Popularity", 0.03),
    ],
    # Substitutes (similar items) and complements (headphones for a phone) both
    # score here; price fit is light because accessories are naturally cheaper
    "product": [
        ("context", "Item-Item Similarity", 0.24),
        ("copurchase", "Item Co-purchase Factor", 0.20),
        ("complement", "Complementary Category", 0.14),
        ("als", "Base ALS Score", 0.12),
        ("session", "Session Intent Boost", 0.08),
        ("content", "User-Item Similarity", 0.06),
        ("category", "Category Affinity", 0.05),
        ("price", "Price Range Fit", 0.04),
        ("brand", "Brand Affinity", 0.04),
        ("popularity", "Popularity", 0.03),
    ],
}

# Variety: after scoring, items are picked greedily and each pick is ranked by
# score x DECAY^(items already shown from the same category). Product pages
# diversify harder so a phone page shows a few phones, then headphones,
# smartwatches... instead of twelve phones. The Match % shown stays the raw score.
DIVERSITY_DECAY = {"home": 0.75, "product": 0.6}

# Complementary categories learned by export_serving_features.py; when a category
# has fewer than this many, other categories from the same department fill in
MIN_COMPLEMENT_CATEGORIES = 3
DEPARTMENT_FALLBACK_SCORE = 0.3   # complement value for a same-department fallback
COMPLEMENT_CATEGORIES_USED = 4    # complementary categories that contribute candidates
ITEMS_PER_COMPLEMENT = 5

# Event weights mirror processing/utils/weighting.py so live session clicks
# count the same way batch history does
EVENT_WEIGHTS = {"purchase": 5, "cart": 3, "view": 1, "remove_from_cart": 0}

SESSION_MAX_EVENTS = 20
SESSION_TTL_SECONDS = 2 * 60 * 60
SESSION_DECAY = 0.8          # each older session event counts 0.8x the next one

SEARCH_INDEX_SIZE = 5000     # most popular items kept in memory for listing/search
SEARCH_INDEX_TTL_SECONDS = 300

WEB_DIR = os.getenv("WEB_DIR", os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "web"))
