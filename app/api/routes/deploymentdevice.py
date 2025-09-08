from datetime import date
from fastapi import APIRouter, HTTPException, status
from pydantic import BaseModel
from sqlmodel import Session, select

from app.database import DbDependency
from app.sqlmodels import Device, DeploymentDevice
from app.api.routes.deployment import deployment_exists
from app.api.routes.device import device_exists

router = APIRouter(prefix="/deploymnent-device", tags=["Deployment Device"])


class DeploymentDeviceBase(BaseModel):
    device_id: int
    deployment_id: int
    start_date: date
    end_date: date


class DeploymentDeviceFull(DeploymentDeviceBase):
    id: int


@router.get(
    "/",
    summary="List deployment-devices.",
    response_model=list[DeploymentDeviceFull]
)
async def get_deploymentdevices(
    db: DbDependency,
    device_id: int = None,
    deployment_id: int = None,
    deleted: bool = False,
    offset: int = 0,
    limit: int = 100
):
    sql = (select(DeploymentDevice).
           where(DeploymentDevice.deleted == deleted).
           limit(limit).
           offset(offset))
    if device_id:
        sql = sql.where(DeploymentDevice.device_id == device_id)
    if deployment_id:
        sql = sql.where(DeploymentDevice.deployment_id == deployment_id)

    devices = db.exec(sql).all()
    return devices


@router.get(
    "/{id}",
    summary="Deploymnet-device details.",
    response_model=DeploymentDeviceFull
)
async def get_deploymentdevice(db: DbDependency, id: int):
    return get_deploymentdevice_by_id(db, id)


@router.post(
    "/",
    summary="Create deployment-device.",
    response_model=DeploymentDeviceFull
)
async def create_deploymentdevice(
    db: DbDependency, body: DeploymentDeviceBase
):
    check_valid_deploymentdevice(db, body)
    try:
        new_deploymentdevice = DeploymentDevice.model_validate(body)
        db.add(new_deploymentdevice)
        db.commit()
        db.refresh(new_deploymentdevice)
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Failed to create deploymentdevice: {e.args[0]}")
    return new_deploymentdevice


@router.put(
    "/{id}",
    summary="Update deployment-device.",
    response_model=DeploymentDeviceFull
)
async def update_deploymentdevice(
    db: DbDependency, id: int, body: DeploymentDeviceBase
):
    check_valid_deploymentdevice(db, body)
    current_device = get_deploymentdevice_by_id(db, id)
    try:
        revised_device = body.model_dump(exclude_unset=True)
        current_device.sqlmodel_update(revised_device)
        db.add(current_device)
        db.commit()
        db.refresh(current_device)
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Failed to update deployment-device: {e.args[0]}")
    return current_device


@router.delete("/{id}", summary="Delete deployment-device.")
async def delete_deploymentdevice(db: DbDependency, id: int):
    deploymentdevice = get_deploymentdevice_by_id(db, id)
    try:
        deploymentdevice.deleted = True
        db.add(deploymentdevice)
        db.commit()
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Failed to delete deploymentdevice: {e.args[0]}")
    return {"ok": True}


@router.put(
    "/undelete/{id}",
    summary="Undelete deployment-device.",
    response_model=DeploymentDeviceFull
)
async def undelete_deploymentdevice(db: DbDependency, name: str):
    deploymentdevice = get_deploymentdevice_by_id(db, name, True)
    check_valid_deploymentdevice(db, deploymentdevice)
    try:
        deploymentdevice.deleted = False
        db.add(deploymentdevice)
        db.commit()
        db.refresh(deploymentdevice)
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Failed to undelete deploymentdevice: {e.args[0]}")
    return deploymentdevice


def get_deploymentdevice_by_id(db: Session, id: int, deleted: bool = False):
    deploymentdevice = db.exec(
        select(DeploymentDevice).
        where(DeploymentDevice.id == id).
        where(DeploymentDevice.deleted == deleted)
    ).first()
    if not deploymentdevice:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"No deploymentdevice found with id {id}.")
    return deploymentdevice


def deploymentdevice_exists(db: Session, id: int):
    deploymentdevice = db.exec(
        select(DeploymentDevice).
        where(DeploymentDevice.id == id)
    ).first()
    if deploymentdevice and deploymentdevice.deleted:
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail=f"Deployment-device {id} already exists but is deleted.")
    elif deploymentdevice and not deploymentdevice.deleted:
        return True
    else:
        return False


def check_valid_deploymentdevice(
        db: Session, depdev: DeploymentDeviceBase):
    # Check foreign key validity.
    if not deployment_exists(db, depdev.deployment_id):
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Deployment {depdev.deployment_id} not found."
        )
    if not device_exists(db, depdev.device_id):
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Device {depdev.device_id} not found."
        )
    # Check start date is before end date.
    if depdev.start_date > depdev.end_date:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Start date must be before end date.")
    # Check device not already assigned in date range.
    overlapping = db.exec(
        select(DeploymentDevice).
        where(DeploymentDevice.device_id == depdev.device_id).
        where(DeploymentDevice.deleted == False).
        where(DeploymentDevice.start_date < depdev.end_date).
        where(DeploymentDevice.end_date >= depdev.start_date)
    ).first()
    if overlapping:
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail=f"Device {depdev.device_id} already assigned to deployment "
            f"{overlapping.deployment_id} between {overlapping.start_date} "
            f"and {overlapping.end_date}.")
