import logging
import sys

from app.database import create_db
from app.env import get_all_settings
from app.api.main import router

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.middleware.gzip import GZipMiddleware
from mangum import Mangum


# Load settings.
env = get_all_settings()

# Configure logging
if logging.getLogger().hasHandlers():
    # Lambda pre-configures root logger.
    logging.getLogger().setLevel(env.log_level.upper())
else:
    # Local development.
    logging.basicConfig(level=env.log_level.upper())
logger = logging.getLogger()

# Initialise database.
try:
    engine = create_db(env)
except Exception:
    logger.error("Error creating database.", exc_info=True)
    sys.exit(1)
logger.info("Database initialised.")

# Instantiate the app.
app = FastAPI(
    title="LepiSense Input API",
    summary="API for submitting data to the LepiSense system",
    version="0.0.1",
    root_path=f"/{env.environment}",
    contact={
        "name": "AMI system team at UKCEH",
        "url": "https://www.ceh.ac.uk/solutions/equipment/automated-monitoring-insects-trap",
        "email": "ami-system@ceh.ac.uk",
    },
    license_info={
        "name": "Apache 2.0",
        "identifier": "MIT",
    }
)

app.add_middleware(GZipMiddleware, minimum_size=1000, compresslevel=5)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"]
)

# Store the engine in the app.state so it is available in requests.
app.state.engine = engine

# Attach all the routes we serve.
app.include_router(router)

handler = Mangum(app, lifespan="on")
