CREATE TABLE IF NOT EXISTS experiment_assignments (
    user_id VARCHAR(255) PRIMARY KEY,
    variant VARCHAR(20) NOT NULL,
    assigned_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT valid_variant CHECK (variant IN ('control', 'treatment'))
);

CREATE INDEX IF NOT EXISTS idx_experiment_assignments_variant
    ON experiment_assignments (variant);