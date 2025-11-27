import logging

from functools import lru_cache
from typing import Annotated

from fastapi import Depends
from pydantic_settings import BaseSettings, SettingsConfigDict

logger = logging.getLogger(__name__)


class EnvSettings(BaseSettings):
    # Default values for environment variables.
    # The SAM template will override these values if deploying to AWS.
    postgres_secret_name: str = ''
    userone_secret_arn: str = ''
    jwt_secret_arn: str = ''
    environment: str = 'prod'  # ['dev'|'test'|'prod']
    log_level: str = 'warning'  # [debug|info|warning|error|critical]

    # Settings are obtained in order of preference from the following sources:
    # 1. Environment variables.
    # 2. .env file.
    # 3. Default values.
    # It is expected to use enviironment variables in production and .env file
    # in development.
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
