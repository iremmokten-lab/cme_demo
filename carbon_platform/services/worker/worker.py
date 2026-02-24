import asyncio
import json
from datetime import datetime
from sqlalchemy.ext.asyncio import create_async_engine, async_sessionmaker
from sqlalchemy import select, update

from services.api.core.config import settings
from services.api.db.models import Job

POLL_SECONDS = 3

async def main():
    engine = create_async_engine(settings.DATABASE_URL, pool_pre_ping=True)
    Session = async_sessionmaker(engine, expire_on_commit=False)

    while True:
        try:
            async with Session() as db:
                res = await db.execute(
                    select(Job)
                    .where(Job.status == "queued")
                    .order_by(Job.created_at.asc())
                    .limit(5)
                )
                jobs = res.scalars().all()
                for j in jobs:
                    await db.execute(update(Job).where(Job.id == j.id).values(status="running", updated_at=datetime.utcnow()))
                    await db.commit()

                    payload = json.loads(j.payload)
                    result = {"message": "Worker stub", "job_type": j.job_type, "payload": payload}

                    # TODO: optimizer_run burada gerçek optimizasyonla doldurulur
                    await db.execute(
                        update(Job).where(Job.id == j.id).values(
                            status="succeeded",
                            result=json.dumps(result, ensure_ascii=False),
                            updated_at=datetime.utcnow()
                        )
                    )
                    await db.commit()
        except Exception:
            # production: structured logging
            pass

        await asyncio.sleep(POLL_SECONDS)

if __name__ == "__main__":
    asyncio.run(main())
