from api.storage import db


def recalculate_top100():
    items = list(db.top100.find({}, {"_id": 0}))

    # simple ordering rule (you can improve later)
    items.sort(key=lambda x: x.get("score", 0), reverse=True)

    for index, item in enumerate(items, start=1):
        db.top100.update_one(
            {"title": item["title"], "artist": item["artist"]},
            {"$set": {"position": index}}
        )

    return len(items)