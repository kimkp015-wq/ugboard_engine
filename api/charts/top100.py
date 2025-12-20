@router.post("/top100/recalculate")
def recalculate_top100():
    path = resolve_top100_path()

    if not path:
        raise HTTPException(
            status_code=500,
            detail="Top100 file not found"
        )

    try:
        with open(path, "r") as f:
            data = json.load(f)
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Failed to read Top100 file: {str(e)}"
        )

    items = data.get("items", [])

    if not isinstance(items, list):
        raise HTTPException(
            status_code=500,
            detail="Invalid Top100 format"
        )

    # Apply boosts if available
    if apply_boosts:
        try:
            items = apply_boosts(items)
        except Exception:
            pass

    # Sort by score
    def score_value(item):
        try:
            return float(item.get("score", 0))
        except Exception:
            return 0

    items = sorted(items, key=score_value, reverse=True)

    # Reassign positions
    for index, item in enumerate(items, start=1):
        item["position"] = index

    # Write back safely
    data["items"] = items

    try:
        with open(path, "w") as f:
            json.dump(data, f, indent=2)
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Failed to write Top100 file: {str(e)}"
        )

    return {
        "status": "recalculated",
        "count": len(items)
    }