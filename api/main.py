from fastapi import FastAPI
from .charts.top100 import router as top100_router
from .admin.publish import router as admin_router

app = FastAPI(title="UG Board Engine")

app.include_router(top100_router)
app.include_router(admin_router)

@app.get("/")
def root():
    return {"status": "ok", "engine": "ugboard"}