import logging

from fastapi import Request, Depends
from sqlmodel import create_engine, SQLModel, Session
from typing import Annotated

from app.env import EnvSettings
# Importing sqlmodels ensures the tables are created in the database.
import app.sqlmodels  # noqa F401

logger = logging.getLogger()


def create_db(env: EnvSettings):

    pg_url = (
        f"postgresql://{env.postgres_user}:{env.postgres_password}@"
        f"{env.postgres_host}:{env.postgres_port}/{env.postgres_db}"
    )

    # Log SQL queries if log_level is debug or info.
    echo = True if env.log_level in ['debug', 'info'] else False
    engine = create_engine(pg_url, echo=echo)
    return engine

    # To initialise the database, call the database/reset API endpoint.


def init_db(engine):
    SQLModel.metadata.create_all(engine)
    logger.info("Database created.")


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
