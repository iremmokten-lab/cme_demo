from fastapi import FastAPI
from services.api.core.config import settings
from services.api.db.session import init_db
from services.api.routers import (
    auth,
    tenants,
    facilities,
    factors,
    activity,
    calc,
    evidence,
    jobs,
)
from services.api.routers import mrv as mrv_router
from services.api.routers import documents as documents_router

app = FastAPI(title=settings.APP_NAME)

@app.on_event("startup")
async def on_startup():
    await init_db()

@app.get("/health")
async def health():
    return {"status": "ok", "app": settings.APP_NAME, "env": settings.ENV}

app.include_router(tenants.router, prefix="/tenants", tags=["Tenants"])
app.include_router(auth.router, prefix="/auth", tags=["Auth"])
app.include_router(facilities.router, prefix="/facilities", tags=["Facilities"])
app.include_router(factors.router, prefix="/factors", tags=["Factors"])
app.include_router(activity.router, prefix="/activity", tags=["Activity"])
app.include_router(calc.router, prefix="/calc", tags=["Calculation"])
app.include_router(evidence.router, prefix="/evidence", tags=["Evidence"])
app.include_router(jobs.router, prefix="/jobs", tags=["Jobs"])

app.include_router(mrv_router.router, prefix="/mrv", tags=["MRV"])
app.include_router(documents_router.router, prefix="/documents", tags=["Documents"])
