from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", extra="ignore")
    database_url: str = "postgresql+asyncpg://flagharbor:flagharbor@database:5432/flagharbor"
    jwt_secret: str = Field(
        default="local-demo-flagharbor-replace-before-deployment", min_length=32
    )
    token_minutes: int = 60


settings = Settings()
