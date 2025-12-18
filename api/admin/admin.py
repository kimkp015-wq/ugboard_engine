# api/admin.py

def health():
    return {
        "status": "ok",
        "service": "admin"
    }