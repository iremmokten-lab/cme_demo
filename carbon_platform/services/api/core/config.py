from pydantic_settings import BaseSettings

class Settings(BaseSettings):
    APP_NAME: str = "Carbon Compliance API"
    ENV: str = "dev"

    JWT_SECRET: str
    JWT_ISSUER: str = "carbon-platform"
    JWT_AUDIENCE: str = "carbon-platform-users"
    ACCESS_TOKEN_MINUTES: int = 480

    DATABASE_URL: str

    STORAGE_MODE: str = "local"  # local | s3
    LOCAL_S3_ROOT: str = "./data/s3"

    AWS_REGION: str | None = None
    AWS_S3_BUCKET: str | None = None
    AWS_ACCESS_KEY_ID: str | None = None
    AWS_SECRET_ACCESS_KEY: str | None = None
    AWS_S3_PREFIX: str = "carbon-platform"

    PRESIGN_EXPIRES_SECONDS: int = 900

    DEFAULT_ETS_PRICE_EUR_PER_TCO2: float = 75.0

    RLS_ENABLED: bool = True

    class Config:
        env_file = ".env"
        extra = "ignore"

settings = Settings()
