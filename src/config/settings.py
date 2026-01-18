# src/config/settings.py
from typing import Optional, List
from pydantic import AnyUrl, Field, validator
from pydantic_settings import BaseSettings, SettingsConfigDict
from enum import Enum


class Environment(str, Enum):
    LOCAL = "local"
    DEVELOPMENT = "development"
    STAGING = "staging"
    PRODUCTION = "production"


class DatabaseSettings(BaseSettings):
    host: str = "localhost"
    port: int = 5432
    database: str = "ugboard"
    username: str = "postgres"
    password: str = ""
    pool_size: int = 20
    pool_pre_ping: bool = True
    echo: bool = False
    
    @property
    def url(self) -> str:
        return f"postgresql+asyncpg://{self.username}:{self.password}@{self.host}:{self.port}/{self.database}"


class RedisSettings(BaseSettings):
    host: str = "localhost"
    port: int = 6379
    password: Optional[str] = None
    db: int = 0
    default_ttl: int = 3600  # 1 hour
    
    @property
    def url(self) -> str:
        if self.password:
            return f"redis://:{self.password}@{self.host}:{self.port}/{self.db}"
        return f"redis://{self.host}:{self.port}/{self.db}"


class SecuritySettings(BaseSettings):
    secret_key: str = Field(..., min_length=32)
    algorithm: str = "HS256"
    access_token_expire_minutes: int = 30
    refresh_token_expire_days: int = 7
    admin_token: str = Field(..., min_length=16)
    ingestion_token: str = Field(..., min_length=16)
    internal_token: str = Field(..., min_length=16)
    cors_origins: List[str] = ["http://localhost:3000", "https://*.ugboard.com"]
    
    @validator("cors_origins")
    def validate_cors_origins(cls, v):
        return [origin.rstrip("/") for origin in v]


class ScraperSettings(BaseSettings):
    max_concurrent_stations: int = 3
    request_timeout: int = 10
    user_agent: str = "UG-Board-Engine/2.0.0"
    retry_attempts: int = 3
    retry_delay: float = 1.0
    rate_limit_per_minute: int = 60
    station_config_path: str = "config/stations.json"


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        env_nested_delimiter="__",
        case_sensitive=False,
    )
    
    environment: Environment = Environment.LOCAL
    debug: bool = False
    project_name: str = "UG Board Engine"
    version: str = "2.0.0"
    api_prefix: str = "/api/v1"
    
    database: DatabaseSettings = DatabaseSettings()
    redis: RedisSettings = RedisSettings()
    security: SecuritySettings = SecuritySettings()
    scraper: ScraperSettings = ScraperSettings()
    
    log_level: str = "INFO"
    sentry_dsn: Optional[str] = None
    prometheus_port: int = 9090
    
    @property
    def is_production(self) -> bool:
        return self.environment == Environment.PRODUCTION
    
    @property
    def is_development(self) -> bool:
        return self.environment in [Environment.LOCAL, Environment.DEVELOPMENT]


settings = Settings()
