from api.db import SessionLocal
from api.models.song import Song

db = SessionLocal()

songs = [
    Song(title="Song A", artist="Artist 1", region="UG", score=120),
    Song(title="Song B", artist="Artist 2", region="KE", score=110),
    Song(title="Song C", artist="Artist 3", region="TZ", score=100),
]

db.add_all(songs)
db.commit()
db.close()

print("Seed data added")