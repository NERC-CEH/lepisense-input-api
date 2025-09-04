from fastapi import APIRouter, HTTPException, status
from pydantic import BaseModel
from sqlmodel import Session, select

from app.database import DbDependency
from app.sqlmodels import Device, DeploymentDevice
from app.api.routes.network import network_exists
from app.api.routes.devicetype import devicetype_exists

router = APIRouter(prefix="/device", tags=["Device"])


class DeviceBase(BaseModel):
    network_id: int
    devicetype_name: str
    name: str
    latitude: float
    longitude: float
    active: bool


class DeviceFull(DeviceBase):
    id: int


@router.get(
    "/",
    summary="List devices.",
    response_model=list[DeviceFull]
)
async def get_devices(
    db: DbDependency,
    device_id: int = None,
    offset: int = 0,
    limit: int = 100
):
    sql = (select(Device).
           where(Device.deleted == False).
           limit(limit).
           offset(offset))
    if device_id:
        sql = sql.where(Device.device_id == device_id)

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
    db: DbDependency, body: DeviceBase
):
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
    if not device_exists(db, body):
        try:
            body.devicetype_name = body.organisation_name.lower()
            new_device = Device.model_validate(body)
            db.add(new_device)
            db.commit()
            db.refresh(new_device)
            return new_device
        except Exception as e:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=f"Failed to create device: {e.args[0]}")


@router.put(
    "/{id}",
    summary="Update device.",
    response_model=DeviceFull
)
async def update_device(
    db: DbDependency, id: int, body: DeviceBase
):
    if not devicetype_exists(db, body.devicetype_name):
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Device type {body.devicetype_name} not found."
        )
    current_device = get_device_by_id(db, id)
    try:
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
    device.deleted = True
    db.add(device)
    db.commit()
    return {"ok": True}


@router.put(
    "/undelete/{id}",
    summary="Undelete device.",
    response_model=DeviceFull
)
async def undelete_device(db: DbDependency, name: str):
    device = get_device_by_id(db, name, True)
    device.deleted = False
    db.add(device)
    db.commit()
    db.refresh(device)
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
