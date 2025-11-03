import logging

from fastapi import APIRouter, HTTPException, status
from pydantic import BaseModel, Field
from sqlmodel import Session, select

from app.database import DbDependency
from app.sqlmodels import Organisation, Network, Account

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/organisation", tags=["Organisation"])


class OrganisationBase(BaseModel):
    full_name: str = Field(description="The full name of the organisation.")


class OrganisationFull(OrganisationBase):
    name: str = Field(description="A short, unique, organisation name.")


@router.get(
    "/",
    summary="List organisations.",
    response_model=list[OrganisationFull]
)
async def get_organisations(
    db: DbDependency,
    deleted: bool = False,
    offset: int = 0,
    limit: int = 100
):
    organisations = db.exec(
        select(Organisation).
        where(Organisation.deleted == deleted).
        limit(limit).
        offset(offset)
    ).all()
    return organisations


@router.get(
    "/{name}",
    summary="Organisation details.",
    response_model=OrganisationFull
)
async def get_organisation(db: DbDependency, name: str):
    return get_organisation_by_name(db, name)


@router.post(
    "/", summary="Create organisation.", response_model=OrganisationFull
)
async def create_organisation(
    db: DbDependency, body: OrganisationFull
):
    if organisation_exists(db, body.name):
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail=f"Organisation {body.name} already exists.")

    try:
        body.name = body.name.upper()
        new_organisation = Organisation.model_validate(body)
        db.add(new_organisation)
        db.commit()
        db.refresh(new_organisation)
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Failed to create organisation: {e.args[0]}")
    return new_organisation


@router.put(
    "/{name}",
    summary="Update organisation.",
    response_model=OrganisationFull
)
async def update_organisation(
    db: DbDependency, name: str, body: OrganisationBase
):
    current_organisation = get_organisation_by_name(db, name)
    try:
        revised_organisation = body.model_dump(exclude_unset=True)
        current_organisation.sqlmodel_update(revised_organisation)
        db.add(current_organisation)
        db.commit()
        db.refresh(current_organisation)
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Failed to update organisation: {e.args[0]}")
    return current_organisation


@router.delete("/{name}", summary="Delete organisation.")
async def delete_organisation(db: DbDependency, name: str):
    if organisation_used(db, name):
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail=f"Organisation {name} is in use and cannot be deleted.")
    organisation = get_organisation_by_name(db, name)
    try:
        organisation.deleted = True
        db.add(organisation)
        db.commit()
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Failed to delete organisation: {e.args[0]}")
    return {"ok": True}


@router.put(
    "/undelete/{name}",
    summary="Undelete organisation.",
    response_model=OrganisationFull
)
async def undelete_organisation(db: DbDependency, name: str):
    organisation = get_organisation_by_name(db, name, True)
    try:
        organisation.deleted = False
        db.add(organisation)
        db.commit()
        db.refresh(organisation)
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Failed to undelete organisation: {e.args[0]}")
    return organisation


def get_organisation_by_name(db: Session, name: str, deleted: bool = False):
    name = name.upper()
    organisation = db.exec(
        select(Organisation).
        where(Organisation.name == name).
        where(Organisation.deleted == deleted)
    ).first()
    if not organisation:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"No organisation found with name {name}.")
    return organisation


def organisation_exists(db: Session, name: str):
    name = name.upper()
    organisation = db.exec(
        select(Organisation).
        where(Organisation.name == name)
    ).first()
    if organisation and organisation.deleted:
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail=f"Organisation {name} already exists but is deleted.")
    elif organisation and not organisation.deleted:
        return True
    else:
        return False


def organisation_used(db: Session, name: str):
    name = name.upper()
    networks = db.exec(
        select(Network).
        where(Network.organisation_name == name).
        where(Network.deleted == False)  # noqa
    ).first()
    accounts = db.exec(
        select(Account).
        where(Account.organisation_name == name).
        where(Account.deleted == False)  # noqa
    ).first()
    return True if networks or accounts else False
