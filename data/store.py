# /app/data/store.py
import json
import os
from pathlib import Path
from typing import List, Dict, Any
import logging

logger = logging.getLogger(__name__)

DATA_DIR = Path(__file__).parent
ITEMS_FILE = DATA_DIR / "items.json"

def load_items() -> List[Dict[str, Any]]:
    """Load all items from JSON file"""
    try:
        if ITEMS_FILE.exists():
            with open(ITEMS_FILE, 'r', encoding='utf-8') as f:
                items = json.load(f)
                if isinstance(items, list):
                    return items
                else:
                    logger.warning(f"{ITEMS_FILE} does not contain a list, returning empty")
                    return []
        else:
            logger.info(f"{ITEMS_FILE} does not exist, returning empty list")
            return []
    except json.JSONDecodeError as e:
        logger.error(f"Error decoding JSON from {ITEMS_FILE}: {e}")
        return []
    except Exception as e:
        logger.error(f"Error loading items: {e}")
        return []

def save_items(items: List[Dict[str, Any]]) -> bool:
    """Save items to JSON file"""
    try:
        # Ensure data directory exists
        DATA_DIR.mkdir(exist_ok=True)
        
        # Save items
        with open(ITEMS_FILE, 'w', encoding='utf-8') as f:
            json.dump(items, f, indent=2, default=str)
        
        logger.info(f"Saved {len(items)} items to {ITEMS_FILE}")
        return True
    except Exception as e:
        logger.error(f"Error saving items: {e}")
        return False

def upsert_item(new_item: Dict[str, Any]) -> bool:
    """Insert or update an item in the store"""
    try:
        items = load_items()
        
        # Find existing item by unique identifier
        item_id = new_item.get('id') or new_item.get('external_id')
        if not item_id:
            logger.warning("Item missing id or external_id, cannot upsert")
            return False
        
        # Check if item exists
        item_index = -1
        for i, item in enumerate(items):
            if item.get('id') == item_id or item.get('external_id') == item_id:
                item_index = i
                break
        
        if item_index >= 0:
            # Update existing item
            items[item_index].update(new_item)
            logger.debug(f"Updated item: {item_id}")
        else:
            # Add new item
            items.append(new_item)
            logger.debug(f"Added new item: {item_id}")
        
        # Save back
        return save_items(items)
        
    except Exception as e:
        logger.error(f"Error upserting item: {e}")
        return False

# For backward compatibility with old imports
def get_items() -> List[Dict[str, Any]]:
    """Alias for load_items for backward compatibility"""
    return load_items()

def store_items(items: List[Dict[str, Any]]) -> bool:
    """Alias for save_items for backward compatibility"""
    return save_items(items)
