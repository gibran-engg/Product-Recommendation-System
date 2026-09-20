"""
weighting.py

Event-type weighting scheme + time-decay config, finalized from Week 1
EDA findings (notebooks/eda.ipynb, Section 11).
"""

EVENT_WEIGHTS = {
    "purchase": 5,
    "cart": 3,
    "view": 1,
    "remove_from_cart": 0,
}

# Users at or below this many total interactions are routed to the
# content-based fallback instead of ALS at serving time.
COLD_START_THRESHOLD = 3

# Exponential decay half-life in days. Decay is computed relative to the
# MOST RECENT event timestamp in the dataset, never wall-clock "now" —
# this dataset is from Oct 2019, so decaying against today's date would
# collapse every weight to ~0.
TIME_DECAY_HALF_LIFE_DAYS = 14