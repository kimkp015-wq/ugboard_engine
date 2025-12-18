# api/charts/trending.py

TRENDING_SONGS = [
    {"rank": 1, "artist": "Ava Peace", "title": "Danger"},
    {"rank": 2, "artist": "Joshua Baraka", "title": "Wrong Places"},
    {"rank": 3, "artist": "Azawi", "title": "Slow Dancing"},
    {"rank": 4, "artist": "Sheebah", "title": "Karma"},
    {"rank": 5, "artist": "Eddy Kenzo", "title": "Balippola"},
]

def build_trending():
    return {
        "status": "ok",
        "chart": "Uganda Trending Songs",
        "total": len(TRENDING_SONGS),
        "data": TRENDING_SONGS
    }