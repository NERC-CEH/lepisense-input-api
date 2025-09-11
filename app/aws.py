import logging

from aioboto3 import Session
from botocore.client import BaseClient
from fastapi import Depends
from typing import Annotated

logger = logging.getLogger()


async def get_s3_client():
    """A function for injecting an s3 client as a dependency."""
    session = Session()
    async with session.client('s3') as s3:
        yield s3


# Create an annotated dependency for brevity when defining an endpoint needing
# an aws session.
S3Dependency = Annotated[BaseClient, Depends(get_s3_client)]
