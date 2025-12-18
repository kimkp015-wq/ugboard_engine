from api.charts.data import UG_TOP_SONGS
from api.admin.boost import BOOST_LOG
import random
from datetime import datetime, timedelta

BOOST_SCORE = 25  # score added per admin boost

def build_top_100():
    # Use EAT (UTC+3)
    eat_now = datetime.utcnow() + timedelta(hours=3)
    today = eat_now.date().isoformat()

    random.seed(today)  # lock daily scores

    boosts_today = BOOST_LOG.get(today, [])

    ranked = []

    for index, song in enumerate(UG_TOP_SONGS, start=1):
        youtube_signal = random.randint(20, 60)
        radio_signal = random.randint(10, 40)
        social_signal = random.randint(15, 50)

        base_score = youtube_signal + radio_signal + social_signal

        boost_count = boosts_today.count(song["id"])
        boost_score = boost_count * BOOST_SCORE

        total_score = base_score + boost_score

        ranked.append({
            "rank": index,
            "id": song["id"],
            "artist": song["artist"],
            "title": song["title"],
            "score": total_score,
            "boosts": boost_count,
            "signals": {
                "youtube": youtube_signal,
                "radio": radio_signal,
                "social": social_signal
            }
        })

    ranked.sort(key=lambda x: x["score"], reverse=True)

    for i, song in enumerate(ranked, start=1):
        song["rank"] = i

    return {
        "status": "ok",
        "chart": "Uganda Top 100",
        "locked_for": today,
        "timezone": "EAT (UTC+3)",
        "boost_score": BOOST_SCORE,
        "total": len(ranked),
        "data": ranked
    }