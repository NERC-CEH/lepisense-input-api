import boto3
import json
import logging

from botocore.exceptions import ClientError
from functools import lru_cache
from pydantic_settings import BaseSettings, SettingsConfigDict

from app.env import EnvSettings

logger = logging.getLogger(__name__)

# Intentionally not using pydantic-settings-aws module as I don't want it
# to keep refreshing the secrets (should the lambda ever live that long).


class AwsSecrets(BaseSettings):
    postgres_host: str = ''
    postgres_port: int = 5432
    postgres_user: str = 'postgres'
    postgres_password: str = ''
    postgres_db: str = ''
    userone_name: str = 'userone'
    userone_pass: str = ''
    userone_email: str = 'ami@ceh.ac.uk'
    jwt_key: str = ''
    jwt_algorithm: str = ''
    jwt_expires_minutes: int = 0

    def __init__(self, env: EnvSettings):

        # Load secrets from AWS Secrets Manager.
        # Consider using batch_get_secrets to get multiple secrets at once.
        client = boto3.client("secretsmanager")
        try:
            postgres_secret = client.get_secret_value(
                SecretId=env.postgres_secret_name
            )
            userone_secret = client.get_secret_value(
                SecretId=env.userone_secret_arn
            )
            jwt_secret = client.get_secret_value(
                SecretId=env.jwt_secret_arn
            )
        except ClientError as e:
            logger.error(e)
            raise e

        postgres_secret = postgres_secret["SecretString"]
        postgres_secret = json.loads(postgres_secret)

        # I expected to be able to set the properties directly, using
        # self.postgres_host = postgres_secret["host"], but it doesn't work.
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

        # Instead, it works if you submit the properties to the parent
        # constructor.
        super().__init__(
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

        logger.info("Secrets loaded.")


@lru_cache
def get_secrets(env: EnvSettings):
    # A cached function keeping settings in memory.
    secrets = AwsSecrets(env)
    return secrets
