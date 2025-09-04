import logging

from functools import lru_cache
from typing import Annotated

from fastapi import Depends
from pydantic_settings import BaseSettings, SettingsConfigDict

logger = logging.getLogger(__name__)


class EnvSettings(BaseSettings):
    jwt_key: str = ''
    jwt_algorithm: str = ''
    jwt_expires_minutes: int = 0
    postgres_host: str = ''
    postgres_port: int = 5432
    postgres_user: str = 'postgres'
    postgres_password: str = ''
    postgres_db: str = ''
    environment: str = 'prod'  # ['dev'|'test'|'prod']
    log_level: str = 'WARNING'

    # Making the settings frozen means they are hashable.
    # https://github.com/fastapi/fastapi/issues/1985#issuecomment-1290899088
    model_config = SettingsConfigDict(env_file=".env", frozen=True)


@lru_cache
def get_env_settings():
    # A cached function keeping settings in memory.
    env = EnvSettings()
    print(f"Environment: {env}")

    return env


# Create an annotated dependency for brevity when defining an endpoint needing
# env settings.
EnvDependency = Annotated[EnvSettings, Depends(get_env_settings)]
