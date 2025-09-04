from fastapi import APIRouter, HTTPException, status
from pydantic import BaseModel
from sqlmodel import Session, select

from app.database import DbDependency
from app.sqlmodels import Deployment, Device, DeploymentDevice
from app.api.routes.network import network_exists
from app.api.routes.devicetype import devicetype_exists

router = APIRouter(prefix="/deployment", tags=["Deployment"])


class DeploymentBase(BaseModel):
    network_id: int
    devicetype_name: str
    name: str
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
    deployment_id: int = None,
    offset: int = 0,
    limit: int = 100
):
    sql = (select(Deployment).
           where(Deployment.deleted == False).
           limit(limit).
           offset(offset))
    if deployment_id:
        sql = sql.where(Deployment.deployment_id == deployment_id)

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
    # Check foreign key validity
    if not network_exists(db, body.network_id):
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Network {body.network_id} not found."
        )
    if not devicetype_exists(db, body.devicetype_name):
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Device type {body.devicetype_name} not found."
        )
    # Maintain unique deployment names for a netwokr and devicetype.
    if deployment_name_exists(
            db, body.network_id, body.devicetype_name, body.name):
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail=(f"Deployment {body.name} already exists for "
                    f"{body.devicetype_name} in network {body.network_id}."))

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
    # Check foreign key validity
    if not network_exists(db, body.network_id):
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Network {body.network_id} not found."
        )
    if not devicetype_exists(db, body.devicetype_name):
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Device type {body.devicetype_name} not found."
        )
    current_deployment = get_deployment_by_id(db, id)
    # Maintain unique deployment names for a network and devicetype.
    if (current_deployment.network_id != body.network_id or
        current_deployment.devicetype_name != body.devicetype_name or
            current_deployment.name != body.name):
        if deployment_name_exists(
                db, body.network_id, body.devicetype_name, body.name):
            raise HTTPException(
                status_code=status.HTTP_409_CONFLICT,
                detail=(f"Deployment {body.name} already exists for "
                        f"{body.devicetype_name} in network {body.network_id}."))

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
    deployment.deleted = True
    db.add(deployment)
    db.commit()
    return {"ok": True}


@router.put(
    "/undelete/{id}",
    summary="Undelete deployment.",
    response_model=DeploymentFull
)
async def undelete_deployment(db: DbDependency, name: str):
    deployment = get_deployment_by_id(db, name, True)
    deployment.deleted = False
    db.add(deployment)
    db.commit()
    db.refresh(deployment)
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
        db: Session, network_id: int, devicetype_name: str, name: str):
    devicetype_name = devicetype_name.lower()
    deployment = db.exec(
        select(Deployment).
        where(Deployment.network_id == network_id).
        where(Deployment.devicetype_name == devicetype_name).
        where(Deployment.name == name)
    ).first()
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
