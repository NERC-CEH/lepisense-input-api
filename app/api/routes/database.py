from alembic.config import Config
from alembic.migration import MigrationContext
from alembic.script import ScriptDirectory
from alembic import command
import logging

from fastapi import APIRouter, Request, Depends
from fastapi.responses import FileResponse
from sqlmodel import SQLModel

from app.auth import get_current_root_account

logger = logging.getLogger(__name__)

router = APIRouter(
    prefix="/database",
    tags=["Database"],
    dependencies=[Depends(get_current_root_account)]
)


@router.get("/current", summary="Get current database revision.")
async def current(request: Request):
    # We don't use command.current() as it does not return a value.
    # It only prints it to stdout.
    engine = request.app.state.engine
    conn = engine.connect()
    context = MigrationContext.configure(conn)
    value = context.get_current_revision()
    return {"revision": value}


@router.get("/history", summary="Get revision history.")
async def history(request: Request):
    # We don't use command.history() as it does not return a value.
    # It only prints it to stdout.
    config = Config('alembic.ini')
    script = ScriptDirectory.from_config(config)
    values = {}
    for rev in script.walk_revisions():
        values[rev.revision] = rev.doc
    return values


@router.put("/upgrade", summary="Upgrade the database.")
async def upgrade(revision='head'):
    config = Config('alembic.ini')
    command.upgrade(config, revision)
    return {"ok": True}


@router.put("/downgrade", summary="Downgrade the database.")
async def downgrade(revision='head'):
    """Downgrade the database to the given revision.

    Warning: Downgrading can delete data!
    Double Warning: Downgrading to 'base' will delete all data!
    """
    config = Config('alembic.ini')
    command.downgrade(config, revision)
    return {"ok": True}


@router.get("/revision", summary="Autogenerate database revision.")
async def revision():
    # The lambda section points to a writable file destination.
    config = Config('alembic.ini', ini_section='lambda')
    print("Autogenerating revision...")
    try:
        command.revision(config, autogenerate=True,
                         version_path='/tmp/alembic')
    except Exception as e:
        print(e)
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
