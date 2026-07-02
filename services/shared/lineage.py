import uuid
from datetime import datetime, timezone


def generate_job_id() -> str:
    """Generate a unique job ID. Called once at job creation."""
    return str(uuid.uuid4())


def now_utc() -> datetime:
    """Current time in UTC. Used for arrival_timestamp and processed_timestamp."""
    return datetime.now(timezone.utc)


def now_utc_iso() -> str:
    """ISO 8601 string for BigQuery TIMESTAMP fields."""
    return now_utc().isoformat()
