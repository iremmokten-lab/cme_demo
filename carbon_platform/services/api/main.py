from fastapi import FastAPI
from services.api.core.config import settings

app = FastAPI(
    title=settings.APP_NAME,
    version="1.0.0"
)


@app.get("/health")
def health():
    return {"status": "ok"}
