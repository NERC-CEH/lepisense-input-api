import logging

from fastapi import Request, Depends
from sqlmodel import create_engine, SQLModel, Session
from typing import Annotated

from app.env import EnvSettings
# Importing sqlmodels ensures the tables are created in the database.
import app.sqlmodels  # noqa F401

logger = logging.getLogger()


def create_db(env: EnvSettings):

    pg_url = (f"postgresql://{env.postgres_user}:{env.postgres_password}@"
              f"{env.postgres_host}:{env.postgres_port}/{env.postgres_db}")

    engine = create_engine(pg_url, echo=True)
    return engine


def delete_db(engine):
    SQLModel.metadata.drop_all(engine)
    logger.info("Database deleted.")


def get_db_session(request: Request):
    """A function for injecting a session as a dependency."""
    with Session(request.app.state.engine, ) as session:
        yield session


# Create an annotated dependency for brevity when defining an endpoint needing
# a database session.
DbDependency = Annotated[Session, Depends(get_db_session)]
