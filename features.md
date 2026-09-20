Here are the 5 to build now — picked because each is cheap, has a clear mechanism for moving your accuracy number, and none of them require the heavier stuff (session tracking, real-time bandits) you haven't built yet.

1. Time-decayed interaction weight

final_score = event_weight × decay_factor — this modifies the actual training input, so it directly changes what ALS learns, not just reranking after the fact. Critical implementation detail: decay relative to the most recent timestamp in your dataset (Oct 2019), never datetime.now() — I flagged this earlier, it's the single easiest way to silently break this.

2. Item popularity (total weighted interactions per item)

Trivial groupby, but not throwaway work — you need this exact number for your popularity-baseline (A/B testing control group) and cold-start fallback anyway. Building it now means it's not duplicated effort later.

3. Item conversion rate (purchase+cart events ÷ view events, per item)

Distinguishes items that are genuinely good from items that are just seen a lot. A high-view, low-conversion item is a weak recommendation candidate even if ALS ranks it high on raw interaction volume — this catches that.

4. Recency of user's last interaction with a candidate item

Simple filter/penalty: don't recommend something the user just purchased or viewed 10 minutes ago. Cheap to compute (max(timestamp) per user-item pair), and it's the kind of thing that visibly makes recommendations look "smart" in a demo, not just accurate on paper.

5. Category-match score (does the candidate's category overlap the user's top-N historical categories)

Your first real content-based reranking signal. Binary or frequency-weighted, computed from category_code, cheap, and it's a direct, explainable personalization signal — this is also your literal "recommended because you viewed X" explainability hook for later.