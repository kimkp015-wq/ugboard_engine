from data.scoring import compute_score
def upsert_item(new_item: Dict) -> Dict:
    """
    Insert or update a song record using song_id as primary key.
    Auto-recalculates score on every write.
    """

    if not isinstance(new_item, dict):
        raise ValueError("item must be a dict")

    song_id = new_item.get("song_id")
    if not song_id or not isinstance(song_id, str):
        raise ValueError("Missing required field: song_id")

    items = load_items()
    updated = False

    for idx, item in enumerate(items):
        if item.get("song_id") == song_id:
            merged = {**item, **new_item}
            merged["score"] = compute_score(merged)
            items[idx] = merged
            updated = True
            break

    if not updated:
        new_item["score"] = compute_score(new_item)
        items.append(new_item)

    _safe_write_json(ITEMS_FILE, items)
    return new_item