import requests
import xml.etree.ElementTree as ET

def fetch_ugandan_music(max_results=10):
    feed_url = "https://www.youtube.com/feeds/videos.xml?search_query=ugandan+music"

    try:
        response = requests.get(feed_url, timeout=10)

        if response.status_code != 200:
            return {
                "status": "error",
                "message": "YouTube feed unavailable"
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

            if video_id is None or title is None:
                continue

            results.append({
                "title": title.text,
                "video_id": video_id.text,
                "url": f"https://www.youtube.com/watch?v={video_id.text}"
            })

        return {
            "status": "ok",
            "source": "youtube_rss",
            "results": results
        }

    except Exception as e:
        return {
            "status": "error",
            "message": "Failed to parse YouTube feed",
            "detail": str(e)
        }