from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_prefix="SMDG_",
        env_file=".env",
        extra="ignore",
    )

    app_name: str = "Smart Market Data Gateway"
    environment: str = "development"
    redis_url: str = Field(default="redis://localhost:6379/0")


settings = Settings()
