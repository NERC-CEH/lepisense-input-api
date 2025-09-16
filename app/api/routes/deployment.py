import logging

from fastapi import APIRouter, HTTPException, status
from pydantic import BaseModel
from sqlmodel import Session, select

from app.database import DbDependency
from app.sqlmodels import Deployment, Device, DeploymentDevice, Network
from app.api.routes.network import network_exists
from app.api.routes.devicetype import devicetype_exists

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/deployment", tags=["Deployment"])


class DeploymentBase(BaseModel):
    network_id: int
    devicetype_name: str
    name: str
    description: str | None
    latitude: float
    longitude: float
    active: bool


class DeploymentFull(DeploymentBase):
    id: int


@router.get(
    "/",
    summary="List deployments.",
    response_model=list[DeploymentFull]
)
async def get_deployments(
    db: DbDependency,
    organisation_name: str | None = None,
    country_code: str | None = None,
    network_name: str | None = None,
    deployment_name: str | None = None,
    devicetype_name: str | None = None,
    active: bool = True,
    deleted: bool = False,
    offset: int = 0,
    limit: int = 100
):
    sql = (select(Deployment).
           join(Network, Network.id == Deployment.network_id).
           where(Deployment.deleted == deleted).
           where(Deployment.active == active).
           limit(limit).
           offset(offset))
    if organisation_name:
        organisation_name = organisation_name.upper()
        sql = sql.where(Network.organisation_name == organisation_name)
    if country_code:
        country_code = country_code.upper()
        sql = sql.where(Network.country_code == country_code)
    if network_name:
        sql = sql.where(Network.name == network_name)
    if deployment_name:
        sql = sql.where(Deployment.name == deployment_name)
    if devicetype_name:
        devicetype_name = devicetype_name.lower()
        sql = sql.where(Deployment.devicetype_name == devicetype_name)

    deployments = db.exec(sql).all()
    return deployments


@router.get(
    "/{id}",
    summary="Deployment details.",
    response_model=DeploymentFull
)
async def get_deployment(db: DbDependency, id: int):
    return get_deployment_by_id(db, id)


@router.post(
    "/", summary="Create deployment.", response_model=DeploymentFull
)
async def create_deployment(
    db: DbDependency, body: DeploymentBase
):
    check_valid_deployment(db, body)
    try:
        body.devicetype_name = body.devicetype_name.lower()
        new_deployment = Deployment.model_validate(body)
        db.add(new_deployment)
        db.commit()
        db.refresh(new_deployment)
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Failed to create deployment: {e.args[0]}")
    return new_deployment


@router.put(
    "/{id}",
    summary="Update deployment.",
    response_model=DeploymentFull
)
async def update_deployment(
    db: DbDependency, id: int, body: DeploymentBase
):
    check_valid_deployment(db, body, id)
    current_deployment = get_deployment_by_id(db, id)
    try:
        body.devicetype_name = body.devicetype_name.lower()
        revised_deployment = body.model_dump(exclude_unset=True)
        current_deployment.sqlmodel_update(revised_deployment)
        db.add(current_deployment)
        db.commit()
        db.refresh(current_deployment)
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Failed to update deployment: {e.args[0]}")
    return current_deployment


@router.delete("/{id}", summary="Delete deployment.")
async def delete_deployment(db: DbDependency, id: int):
    if deployment_used(db, id):
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail=f"Deployment {id} is in use and cannot be deleted.")
    deployment = get_deployment_by_id(db, id)
    try:
        deployment.deleted = True
        db.add(deployment)
        db.commit()
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Failed to delete deployment: {e.args[0]}")
    return {"ok": True}


@router.put(
    "/undelete/{id}",
    summary="Undelete deployment.",
    response_model=DeploymentFull
)
async def undelete_deployment(db: DbDependency, id: int):
    deployment = get_deployment_by_id(db, id, True)
    check_valid_deployment(db, deployment)
    try:
        deployment.deleted = False
        db.add(deployment)
        db.commit()
        db.refresh(deployment)
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Failed to undelete deployment: {e.args[0]}")
    return deployment


def get_deployment_by_id(db: Session, id: int, deleted: bool = False):
    deployment = db.exec(
        select(Deployment).
        where(Deployment.id == id).
        where(Deployment.deleted == deleted)
    ).first()
    if not deployment:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"No deployment found with id {id}.")
    return deployment


def deployment_name_exists(
        db: Session, name: str, network_id: int, devicetype_name: str = None):
    sql = (select(Deployment).
           where(Deployment.name == name).
           where(Deployment.network_id == network_id))
    if devicetype_name:
        devicetype_name = devicetype_name.lower()
        # Query will resolve to a single deployment or none.
        sql = sql.where(Deployment.devicetype_name == devicetype_name)
        deployment = db.exec(sql).first()
        if deployment and deployment.deleted:
            raise HTTPException(
                status_code=status.HTTP_409_CONFLICT,
                detail=(f"Deployment {name} already exists for "
                        f"{devicetype_name} in network {network_id}."
                        " but is deleted."))
        elif deployment and not deployment.deleted:
            return True
        else:
            return False
    else:
        # Query may resolve to multiple deployments.
        all_deleted = False
        deployments = db.exec(sql)
        for deployment in deployments:
            if not deployment.deleted:
                return True
            else:
                all_deleted = True

        if all_deleted:
            raise HTTPException(
                status_code=status.HTTP_409_CONFLICT,
                detail=(f"Deployment {name} already exists in network "
                        f"{network_id} but is deleted."))

        return False


def deployment_exists(db: Session, id: int):
    deployment = db.exec(
        select(Deployment).
        where(Deployment.id == id)
    ).first()
    if deployment and deployment.deleted:
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail=f"Deployment {id} already exists but is deleted.")
    elif deployment and not deployment.deleted:
        return True
    else:
        return False


def deployment_used(db: Session, id: int):
    devices = db.exec(
        select(Device).
        where(Device.current_deployment_id == id).
        where(Device.deleted == False)
    ).first()
    deployment_devices = db.exec(
        select(DeploymentDevice).
        where(DeploymentDevice.deployment_id == id).
        where(DeploymentDevice.deleted == False)
    ).first()
    return True if devices or deployment_devices else False


def check_valid_deployment(
        db: Session, deployment: DeploymentBase, id: int = None):
    # Check foreign key validity.
    if not network_exists(db, deployment.network_id):
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Network {deployment.network_id} not found."
        )
    if not devicetype_exists(db, deployment.devicetype_name):
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Device type {deployment.devicetype_name} not found."
        )
    # Maintain unique deployment names for a network and devicetype.
    check_unique = True
    if id:
        current_deployment = get_deployment_by_id(db, id)
        check_unique = (
            current_deployment.network_id != deployment.network_id or
            current_deployment.devicetype_name != deployment.devicetype_name or
            current_deployment.name != deployment.name)

    if check_unique and deployment_name_exists(
        db,
        deployment.name,
        deployment.network_id,
        deployment.devicetype_name
    ):
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail=(f"Deployment {deployment.name} already exists for "
                    f"{deployment.devicetype_name} in network "
                    f"{deployment.network_id}."))
