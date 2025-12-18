import feedparser

YOUTUBE_SEARCH_RSS = (
    "https://www.youtube.com/feeds/videos.xml?search_query="
)

UG_KEYWORDS = [
    "Ugandan music",
    "UG music",
    "Eddy Kenzo",
    "Sheebah",
    "Bebe Cool",
    "Azawi",
    "Jose Chameleone",
    "Wiz Kid Uganda"
]

def fetch_ugandan_music(max_results=10):
    results = []

    for keyword in UG_KEYWORDS:
        try:
            feed = feedparser.parse(
                YOUTUBE_SEARCH_RSS + keyword.replace(" ", "+")
            )

            for entry in feed.entries:
                if len(results) >= max_results:
                    break

                results.append({
                    "title": entry.title,
                    "link": entry.link,
                    "published": entry.get("published", ""),
                    "source": "YouTube"
                })

        except Exception:
            continue

    if not results:
        return {
            "status": "error",
            "message": "YouTube feed unavailable",
            "code": 400
        }

    return {
        "status": "ok",
        "count": len(results),
        "data": results
    }