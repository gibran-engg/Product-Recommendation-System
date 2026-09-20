### Feature Engineering Plan

The recommendation system will use five feature groups designed to improve personalization, temporal relevance, cold-start handling, and recommendation quality.

### 1. Weighted Implicit Feedback

Assign interaction weights based on user intent:

* `purchase = 5`
* `cart = 3`
* `view = 1`
* `remove_from_cart = 0`

These weighted interactions form the primary confidence signal for **implicit-feedback ALS**, preventing high-volume views from overpowering stronger purchase/cart signals.

### 2. Time-Decay Weighting

Apply exponential time decay with a **14-day half-life**:

`final_score = event_weight × time_decay`

Recent interactions therefore contribute more strongly than older behavior, improving temporal relevance.

### 3. User & Item Behavioral Features

Engineer behavioral statistics including:

* User interaction/event counts
* Purchase and cart frequency
* Item popularity
* Item purchase/view conversion rate
* Category diversity
* Average price interaction

These features capture user engagement and long-tail item behavior and will primarily support recommendation reranking.

### 4. Session-Based Intent

Use `user_session` to capture short-term interests through:

* Session event/item counts
* Recent items
* Recent categories
* Recent brands
* Session-level category/brand frequency

Session reranking activates after **≥3 meaningful interactions**, allowing recommendations to adapt to the user's current intent.

### 5. Content-Based Cold-Start

Users with **≤3 interactions** will receive a content-based/popularity fallback using:

`category_code → category_id → unknown`

alongside `brand` and item popularity. This addresses the **49.1% of users with ≤3 interactions** identified during EDA.

### Final Architecture

```text
Weighted Interactions + Time Decay
                ↓
              ALS
                ↓
       Candidate Recommendations
                ↓
 ┌──────────────┼──────────────┐
 │              │              │
Behavioral   Session        Content
 Features     Intent        Features
 └──────────────┼──────────────┘
                ↓
        Hybrid Reranking
                ↓
             Top-N
```

**Design principle:** ALS handles collaborative filtering, while behavioral, session, and content features are used for reranking and cold-start handling rather than being forced directly into the ALS model.
