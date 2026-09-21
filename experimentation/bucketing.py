import hashlib


def assign_bucket(user_id: str) -> str:
    """
    Deterministically assign a user to control (A) or treatment (B).

    The same user_id will always receive the same bucket.
    """
    hash_value = hashlib.sha256(user_id.encode("utf-8")).hexdigest()
    bucket_number = int(hash_value[:8], 16) % 100

    return "control" if bucket_number < 50 else "treatment"


def get_experiment_assignment(user_id: str) -> dict[str, str]:
    """
    Return the experiment assignment for a user.
    """
    return {
        "user_id": user_id,
        "variant": assign_bucket(user_id),
    }