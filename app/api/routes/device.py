import logging

from fastapi import APIRouter, HTTPException, status
from pydantic import BaseModel, Field
from sqlmodel import Session, select

from app.database import DbDependency
from app.sqlmodels import Device, DeploymentDevice
from app.api.routes.deployment import deployment_exists
from app.api.routes.devicetype import devicetype_exists

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/device", tags=["Device"])


class DeviceBase(BaseModel):
    name: str | None = Field(description="An optional device name.")
    devicetype_name: str = Field(
        description="A name from the device type table")
    version: str
    current_deployment_id: int | None = None


class DeviceFull(DeviceBase):
    id: str = Field(description="Unique device id.")


@router.get(
    "/",
    summary="List devices.",
    response_model=list[DeviceFull]
)
async def get_devices(
    db: DbDependency,
    devicetype_name: str = None,
    deleted: bool = False,
    offset: int = 0,
    limit: int = 100
):
    sql = (select(Device).
           where(Device.deleted == deleted).
           limit(limit).
           offset(offset))
    if devicetype_name:
        sql = sql.where(Device.devicetype_name == devicetype_name)

    devices = db.exec(sql).all()
    return devices


@router.get(
    "/{id}",
    summary="Device details.",
    response_model=DeviceFull
)
async def get_device(db: DbDependency, id: int):
    return get_device_by_id(db, id)


@router.post(
    "/", summary="Create device.", response_model=DeviceFull
)
async def create_device(
    db: DbDependency, body: DeviceFull
):
    if device_exists(db, body.id):
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail=f"Device {body.id} already exists.")
    check_valid_device(db, body)
    try:
        body.devicetype_name = body.devicetype_name.lower()
        new_device = Device.model_validate(body)
        db.add(new_device)
        db.commit()
        db.refresh(new_device)
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Failed to create device: {e.args[0]}")
    return new_device


@router.put(
    "/{id}",
    summary="Update device.",
    response_model=DeviceFull
)
async def update_device(
    db: DbDependency, id: int, body: DeviceBase
):
    check_valid_device(db, body)
    current_device = get_device_by_id(db, id)
    try:
        body.devicetype_name = body.devicetype_name.lower()
        revised_device = body.model_dump(exclude_unset=True)
        current_device.sqlmodel_update(revised_device)
        db.add(current_device)
        db.commit()
        db.refresh(current_device)
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Failed to update device: {e.args[0]}")
    return current_device


@router.delete("/{id}", summary="Delete device.")
async def delete_device(db: DbDependency, id: int):
    if device_used(db, id):
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail=f"Device {id} is in use and cannot be deleted.")
    device = get_device_by_id(db, id)
    try:
        device.deleted = True
        db.add(device)
        db.commit()
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Failed to delete device: {e.args[0]}")
    return {"ok": True}


@router.put(
    "/undelete/{id}",
    summary="Undelete device.",
    response_model=DeviceFull
)
async def undelete_device(db: DbDependency, name: str):
    device = get_device_by_id(db, name, True)
    check_valid_device(db, device)
    try:
        device.deleted = False
        db.add(device)
        db.commit()
        db.refresh(device)
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Failed to undelete device: {e.args[0]}")
    return device


def get_device_by_id(db: Session, id: int, deleted: bool = False):
    device = db.exec(
        select(Device).
        where(Device.id == id).
        where(Device.deleted == deleted)
    ).first()
    if not device:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"No device found with id {id}.")
    return device


def device_exists(db: Session, id: int):
    device = db.exec(
        select(Device).
        where(Device.id == id)
    ).first()
    if device and device.deleted:
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail=f"Device {id} already exists but is deleted.")
    elif device and not device.deleted:
        return True
    else:
        return False


def device_used(db: Session, id: int):
    deployment_devices = db.exec(
        select(DeploymentDevice).
        where(DeploymentDevice.device_id == id).
        where(DeploymentDevice.deleted == False)
    ).first()
    return True if deployment_devices else False


def check_valid_device(db: Session, device: DeviceBase):
    # Check foreign key validity. Current deployment is optional.
    if (device.current_deployment_id and
            not deployment_exists(db, device.current_deployment_id)):
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Deployment {device.current_deployment_id} not found."
        )
    if not devicetype_exists(db, device.devicetype_name):
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Device type {device.devicetype_name} not found."
        )
