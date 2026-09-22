CREATE TABLE IF NOT EXISTS experiment_assignments (
    user_id VARCHAR(255) PRIMARY KEY,
    variant VARCHAR(20) NOT NULL,
    assigned_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT valid_variant
        CHECK (variant IN ('control', 'treatment'))
);

CREATE INDEX IF NOT EXISTS idx_experiment_assignments_variant
    ON experiment_assignments (variant);


CREATE TABLE IF NOT EXISTS recommendation_events (
    event_id BIGSERIAL PRIMARY KEY,
    user_id VARCHAR(255) NOT NULL,
    item_id VARCHAR(255) NOT NULL,
    variant VARCHAR(20) NOT NULL,
    event_type VARCHAR(20) NOT NULL,
    event_timestamp TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,

    CONSTRAINT valid_event_variant
        CHECK (variant IN ('control', 'treatment')),

    CONSTRAINT valid_event_type
        CHECK (event_type IN ('impression', 'click', 'conversion'))
);

CREATE INDEX IF NOT EXISTS idx_recommendation_events_user
    ON recommendation_events (user_id);

CREATE INDEX IF NOT EXISTS idx_recommendation_events_variant
    ON recommendation_events (variant);

CREATE INDEX IF NOT EXISTS idx_recommendation_events_type
    ON recommendation_events (event_type);

CREATE INDEX IF NOT EXISTS idx_recommendation_events_timestamp
    ON recommendation_events (event_timestamp);