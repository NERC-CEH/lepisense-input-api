import logging

from fastapi import APIRouter, HTTPException, status
from pydantic import BaseModel
from sqlmodel import Session, select

from app.database import DbDependency
from app.sqlmodels import DeviceType, Device, Deployment

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/device-type", tags=["Device Type"])


class DeviceTypeBase(BaseModel):
    description: str


class DeviceTypeFull(DeviceTypeBase):
    name: str


@router.get(
    "/",
    summary="List device types.",
    response_model=list[DeviceTypeFull]
)
async def get_devicetypes(
    db: DbDependency, deleted: bool = False, offset: int = 0, limit: int = 100
):
    devicetypes = db.exec(
        select(DeviceType).
        where(DeviceType.deleted == deleted).
        limit(limit).
        offset(offset)
    ).all()
    return devicetypes


@router.get(
    "/{name}",
    summary="Device type details.",
    response_model=DeviceTypeFull
)
async def get_devicetype(db: DbDependency, name: str):
    return get_devicetype_by_name(db, name)


@router.post(
    "/", summary="Create device type.", response_model=DeviceTypeFull
)
async def create_devicetype(
    db: DbDependency, body: DeviceTypeFull
):
    if devicetype_exists(db, body.name):
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail=f"Device type {body.name} already exists.")

    body.name = body.name.lower()
    try:
        new_devicetype = DeviceType.model_validate(body)
        db.add(new_devicetype)
        db.commit()
        db.refresh(new_devicetype)
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Failed to create device type: {e.args[0]}")
    return new_devicetype


@router.put(
    "/{name}",
    summary="Update device type.",
    response_model=DeviceTypeFull
)
async def update_devicetype(
    db: DbDependency, name: str, body: DeviceTypeBase
):
    current_devicetype = get_devicetype_by_name(db, name)
    try:
        revised_devicetype = body.model_dump(exclude_unset=True)
        current_devicetype.sqlmodel_update(revised_devicetype)
        db.add(current_devicetype)
        db.commit()
        db.refresh(current_devicetype)
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Failed to update device type: {e.args[0]}")
    return current_devicetype


@router.delete("/{name}", summary="Delete device type.")
async def delete_devicetype(db: DbDependency, name: str):
    if devicetype_used(db, name):
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail=f"DeviceType {name} is in use and cannot be deleted.")
    devicetype = get_devicetype_by_name(db, name)
    try:
        devicetype.deleted = True
        db.add(devicetype)
        db.commit()
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Failed to delete device type: {e.args[0]}")
    return {"ok": True}


@router.put(
    "/undelete/{name}",
    summary="Undelete device type.",
    response_model=DeviceTypeFull
)
async def undelete_devicetype(db: DbDependency, name: str):
    devicetype = get_devicetype_by_name(db, name, True)
    try:
        devicetype.deleted = False
        db.add(devicetype)
        db.commit()
        db.refresh(devicetype)
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Failed to undelete device type: {e.args[0]}")
    return devicetype


def get_devicetype_by_name(db: Session, name: str, deleted: bool = False):
    name = name.lower()
    devicetype = db.exec(
        select(DeviceType).
        where(DeviceType.name == name).
        where(DeviceType.deleted == deleted)
    ).first()
    if not devicetype:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"No device type found with name {name}.")
    return devicetype


def devicetype_exists(db: Session, name: str):
    name = name.lower()
    devicetype = db.exec(
        select(DeviceType).
        where(DeviceType.name == name)
    ).first()
    if devicetype and devicetype.deleted:
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail=f"DeviceType {name} already exists but is deleted.")
    elif devicetype and not devicetype.deleted:
        return True
    else:
        return False


def devicetype_used(db: Session, name: str):
    name = name.lower()
    devices = db.exec(
        select(Device).
        where(Device.devicetype_name == name).
        where(Device.deleted == False)
    ).first()
    deployments = db.exec(
        select(Deployment).
        where(Deployment.devicetype_name == name).
        where(Deployment.deleted == False)
    ).first()
    return True if devices or deployments else False
