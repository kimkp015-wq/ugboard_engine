import json
from pathlib import Path

# -------------------------
# Paths
# -------------------------
DATA_DIR = Path("data")
ITEMS_FILE = DATA_DIR / "items.json"
INGESTION_LOG_FILE = DATA_DIR / "ingestion_log.json"


# -------------------------
# Items storage
# -------------------------
def load_items():
    """
    Returns a list of song items.
    """
    if not ITEMS_FILE.exists():
        return []

    try:
        return json.loads(ITEMS_FILE.read_text())
    except Exception:
        return []


def save_items(items):
    """
    Persists song items safely.
    """
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    ITEMS_FILE.write_text(json.dumps(items, indent=2))


# -------------------------
# Ingestion log (idempotency)
# -------------------------
def load_ingestion_log():
    """
    Returns a SET of processed ingestion keys.
    """
    if not INGESTION_LOG_FILE.exists():
        return set()

    try:
        data = json.loads(INGESTION_LOG_FILE.read_text())
        return set(data) if isinstance(data, list) else set()
    except Exception:
        return set()


def save_ingestion_log(log):
    """
    Saves ingestion log safely.
    """
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    INGESTION_LOG_FILE.write_text(
        json.dumps(sorted(list(log)), indent=2)
    )