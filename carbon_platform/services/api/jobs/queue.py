from redis import Redis
from rq import Queue
from services.api.core.config import settings


redis_conn = Redis.from_url(settings.REDIS_URL)

queue = Queue("carbon_jobs", connection=redis_conn)
