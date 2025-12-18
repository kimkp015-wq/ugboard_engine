import requests
import xml.etree.ElementTree as ET

def fetch_ugandan_music(max_results=10):
    feed_url = "https://www.youtube.com/feeds/videos.xml?search_query=ugandan+music"
    response = requests.get(feed_url, timeout=10)

    root = ET.fromstring(response.text)

    ns = {"yt": "http://www.youtube.com/xml/schemas/2015"}
    results = []

    for entry in root.findall("entry")[:max_results]:
        video_id = entry.find("yt:videoId", ns).text
        title = entry.find("title").text

        results.append({
            "title": title,
            "video_id": video_id,
            "url": f"https://www.youtube.com/watch?v={video_id}"
        })

    return {
        "status": "ok",
        "source": "youtube_rss",
        "results": results
    }