from api.charts.data import UG_TOP_SONGS

def build_top_100():
    return {
        "status": "ok",
        "chart": "Uganda Top Songs",
        "total": len(UG_TOP_SONGS),
        "data": UG_TOP_SONGS
    }