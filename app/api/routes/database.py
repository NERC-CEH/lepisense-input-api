from alembic.config import Config
from alembic.migration import MigrationContext
from alembic import command
import logging

from fastapi import APIRouter, Request
from fastapi.responses import FileResponse
from sqlmodel import SQLModel

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/database", tags=["Database"])


@router.get("/current", summary="Get current database revision.")
async def current(request: Request):
    engine = request.app.state.engine
    conn = engine.connect()
    context = MigrationContext.configure(conn)
    value = context.get_current_revision()
    return {"revision": value}


@router.put("/upgrade", summary="Upgrade the database.")
async def upgrade(revision='head'):
    config = Config('alembic.ini')
    command.upgrade(config, revision)
    return {"ok": True}


@router.get("/revision", summary="Autogenerate database revision.")
async def revision():
    # The lambda section points to a writable file destination.
    config = Config('alembic.ini', ini_section='lambda')
    print("Autogenerating revision...")
    command.revision(config, autogenerate=True, version_path='/tmp/alembic')
    print("Finished alembic!")
    return FileResponse("/tmp/alembic/revision.py")


@router.put("/stamp", summary="Stamp the revision table with the given value.")
async def stamp(revision='heads'):
    config = Config('alembic.ini')
    command.stamp(config, revision)
    return {"ok": True}


@router.delete("/reset", summary="Reset the database, deleting all contents.")
async def reset(request: Request):
    engine = request.app.state.engine
    SQLModel.metadata.drop_all(engine)
    SQLModel.metadata.create_all(engine)
    return {"ok": True}
