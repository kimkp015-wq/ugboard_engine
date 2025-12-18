import requests
import xml.etree.ElementTree as ET

def fetch_ugandan_music(max_results=10):
    feed_url = "https://www.youtube.com/feeds/videos.xml?topic_id=/m/04rlf"

    headers = {
        "User-Agent": "Mozilla/5.0"
    }

    try:
        response = requests.get(feed_url, headers=headers, timeout=10)

        if response.status_code != 200:
            return {
                "status": "error",
                "message": "YouTube feed unavailable",
                "code": response.status_code
            }

        root = ET.fromstring(response.text)

        ns = {
            "yt": "http://www.youtube.com/xml/schemas/2015",
            "atom": "http://www.w3.org/2005/Atom"
        }

        results = []

        for entry in root.findall("atom:entry", ns)[:max_results]:
            video_id = entry.find("yt:videoId", ns)
            title = entry.find("atom:title", ns)
            channel = entry.find("atom:author/atom:name", ns)

            if video_id is None or title is None:
                continue

            results.append({
                "title": title.text,
                "channel": channel.text if channel is not None else "Unknown",
                "video_id": video_id.text,
                "url": f"https://www.youtube.com/watch?v={video_id.text}"
            })

        return {
            "status": "ok",
            "source": "youtube_music_topic",
            "results": results
        }

    except Exception as e:
        return {
            "status": "error",
            "message": "Failed to parse YouTube feed",
            "detail": str(e)
        }