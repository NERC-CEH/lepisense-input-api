# Importing sqlmodels ensures the tables are created in the database on reset.
import app.sqlmodels  # noqa F401
from app.env import EnvDependency
from app.api.routes.organisation import router as organisation_router
from app.api.routes.country import router as country_router
from app.api.routes.network import router as network_router
from app.api.routes.devicetype import router as devicetype_router
from app.api.routes.deployment import router as deployment_router
from app.api.routes.device import router as device_router
from app.api.routes.deploymentdevice import router as deploymentdevice_router
from app.api.routes.file import router as file_router
from app.api.routes.database import router as database_router

from fastapi import APIRouter
from fastapi.responses import RedirectResponse

import logging

logger = logging.getLogger(__name__)

# Instantiate a router.
router = APIRouter()
router.include_router(organisation_router)
router.include_router(country_router)
router.include_router(network_router)
router.include_router(devicetype_router)
router.include_router(deployment_router)
router.include_router(device_router)
router.include_router(deploymentdevice_router)
router.include_router(file_router)
router.include_router(database_router)


@router.get("/", include_in_schema=False)
async def main(env: EnvDependency):
    return RedirectResponse(
        url="/" + env.environment + "/docs")
