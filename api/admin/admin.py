from fastapi import APIRouter, HTTPException
from datetime import date

router = APIRouter()

# simple in-memory counter (safe & fast)
BOOST_LIMIT = 10
boost_state = {
    "date": date.today(),
    "count": 0
}


def check_and_consume_boost():
    global boost_state

    today = date.today()

    # reset every new day
    if boost_state["date"] != today:
        boost_state = {
            "date": today,
            "count": 0
        }

    if boost_state["count"] >= BOOST_LIMIT:
        raise HTTPException(
            status_code=429,
            detail="Daily boost limit reached"
        )

    boost_state["count"] += 1
    return True
from api.charts.top100 import router as top100_router
from api.charts.trending import router as trending_router
from api.charts.regions import router as regions_router

app.include_router(top100_router, prefix="/charts")
app.include_router(trending_router, prefix="/charts")
app.include_router(regions_router, prefix="/charts")