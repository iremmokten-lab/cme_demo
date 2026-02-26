import os
from pydantic import BaseSettings


class Settings(BaseSettings):

    ENV: str = "development"

    APP_NAME: str = "Carbon Compliance Platform"

    API_PORT: int = 8000

    DATABASE_URL: str = os.getenv(
        "DATABASE_URL",
        "postgresql://carbon:carbon@localhost:5432/carbon"
    )

    REDIS_URL: str = os.getenv(
        "REDIS_URL",
        "redis://localhost:6379"
    )

    S3_ENDPOINT: str = os.getenv(
        "S3_ENDPOINT",
        "http://localhost:9000"
    )

    S3_ACCESS_KEY: str = os.getenv(
        "S3_ACCESS_KEY",
        "minio"
    )

    S3_SECRET_KEY: str = os.getenv(
        "S3_SECRET_KEY",
        "minio123"
    )

    S3_BUCKET: str = os.getenv(
        "S3_BUCKET",
        "carbon-documents"
    )

    JWT_SECRET: str = os.getenv(
        "JWT_SECRET",
        "CHANGE_THIS_SECRET"
    )

    JWT_ALGORITHM: str = "HS256"


settings = Settings()
