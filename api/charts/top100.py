from fastapi import APIRouter, Depends
from sqlalchemy.orm import Session
from api.db import SessionLocal
from api.models.song import Song

router = APIRouter()

def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()

@router.get("/top100")
def top_100(db: Session = Depends(get_db)):
    songs = (
        db.query(Song)
        .order_by(Song.score.desc())
        .limit(100)
        .all()
    )

    return {
        "status": "ok",
        "count": len(songs),
        "songs": [
            {
                "id": s.id,
                "title": s.title,
                "artist": s.artist,
                "region": s.region,
                "score": s.score,
            }
            for s in songs
        ]
    }