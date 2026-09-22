import os

import psycopg


POSTGRES_DSN = os.getenv(
    "POSTGRES_DSN",
    "postgresql://admin:admin@localhost:5432/recsys",
)


def log_event(
    user_id: str,
    item_id: str,
    variant: str,
    event_type: str,
) -> None:
    """
    Log a recommendation event to PostgreSQL.

    event_type must be one of:
    - impression
    - click
    - conversion
    """

    query = """
        INSERT INTO recommendation_events (
            user_id,
            item_id,
            variant,
            event_type
        )
        VALUES (%s, %s, %s, %s)
    """

    with psycopg.connect(POSTGRES_DSN) as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                query,
                (user_id, item_id, variant, event_type),
            )