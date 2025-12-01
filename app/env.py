import boto3
import json
import logging

from botocore.exceptions import ClientError
from fastapi import Depends
from functools import lru_cache
from pydantic_settings import BaseSettings, SettingsConfigDict
from typing import Annotated

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
    env_settings = EnvSettings()
    logger.info(f"Environment settings: {env_settings}")
    return env_settings


class AllSettings(BaseSettings):
    # Intentionally not using pydantic-settings-aws module as I don't want it
    # to keep refreshing the secrets (should the lambda ever live that long).
    environment: str = 'prod'  # ['dev'|'test'|'prod']
    log_level: str = 'warning'  # [debug|info|warning|error|critical]
    postgres_host: str = ''
    postgres_port: int = 5432
    postgres_user: str = 'postgres'
    postgres_password: str = ''
    postgres_db: str = ''
    userone_name: str = 'userone'
    userone_pass: str = ''
    jwt_key: str = ''
    jwt_algorithm: str = ''
    jwt_expires_minutes: int = 0

    model_config = SettingsConfigDict(frozen=True)


@lru_cache
def get_all_settings():
    # A cached function keeping settings in memory.

    env_settings = get_env_settings()
    # Load secrets from AWS Secrets Manager.
    # Consider using batch_get_secrets to get multiple secrets at once.
    client = boto3.client("secretsmanager")

    try:
        postgres_secret = client.get_secret_value(
            SecretId=env_settings.postgres_secret_name
        )
        userone_secret = client.get_secret_value(
            SecretId=env_settings.userone_secret_arn
        )
        jwt_secret = client.get_secret_value(
            SecretId=env_settings.jwt_secret_arn
        )
    except ClientError as e:
        logger.error(e)
        raise e

    postgres_secret = postgres_secret["SecretString"]
    postgres_secret = json.loads(postgres_secret)
    postgres_host = postgres_secret["host"]
    postgres_port = postgres_secret["port"]
    postgres_user = postgres_secret["username"]
    postgres_password = postgres_secret["password"]
    postgres_db = postgres_secret["dbname"]

    userone_secret = userone_secret["SecretString"]
    userone_secret = json.loads(userone_secret)
    userone_name = userone_secret["username"]
    userone_pass = userone_secret["password"]

    jwt_secret = jwt_secret["SecretString"]
    jwt_secret = json.loads(jwt_secret)
    jwt_key = jwt_secret["key"]
    jwt_algorithm = jwt_secret["algorithm"]
    jwt_expires_minutes = jwt_secret["expires_minutes"]

    all_settings = AllSettings(
        environment=env_settings.environment,
        log_level=env_settings.log_level,
        postgres_host=postgres_host,
        postgres_port=postgres_port,
        postgres_user=postgres_user,
        postgres_password=postgres_password,
        postgres_db=postgres_db,
        userone_name=userone_name,
        userone_pass=userone_pass,
        jwt_key=jwt_key,
        jwt_algorithm=jwt_algorithm,
        jwt_expires_minutes=jwt_expires_minutes,
    )
    return all_settings


# Create an annotated dependency for brevity when defining an endpoint needing
# settings.
EnvDependency = Annotated[AllSettings, Depends(get_all_settings)]
