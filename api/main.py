from fastapi import FastAPI

app = FastAPI()

@app.get("/")
def root():
    return {
        "status": "ok",
        "engine": "ugboard",
        "message": "UG Board engine running"
    }