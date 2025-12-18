import requests
from datetime import datetime

YOUTUBE_API_KEY = "PUT_YOUR_API_KEY_HERE"

YOUTUBE_SEARCH_URL = "https://www.googleapis.com/youtube/v3/search"
YOUTUBE_VIDEO_URL = "https://www.googleapis.com/youtube/v3/videos"


def fetch_ugandan_music(max_results=25):
    params = {
        "part": "snippet",
        "q": "Ugandan music",
        "type": "video",
        "maxResults": max_results,
        "key": YOUTUBE_API_KEY
    }

    response = requests.get(YOUTUBE_SEARCH_URL, params=params)
    data = response.json()

    video_ids = [item["id"]["videoId"] for item in data["items"]]

    stats_params = {
        "part": "statistics,snippet",
        "id": ",".join(video_ids),
        "key": YOUTUBE_API_KEY
    }

    stats_response = requests.get(YOUTUBE_VIDEO_URL, params=stats_params)
    stats_data = stats_response.json()

    videos = []

    for item in stats_data["items"]:
        videos.append({
            "title": item["snippet"]["title"],
            "artist_guess": item["snippet"]["title"].split("-")[0],
            "views": int(item["statistics"].get("viewCount", 0)),
            "published_at": item["snippet"]["publishedAt"],
            "source": "YouTube",
            "fetched_at": datetime.utcnow().isoformat()
        })

    return videos