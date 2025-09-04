from fastapi import APIRouter, HTTPException, status
from pydantic import BaseModel
from sqlmodel import Session, select

from app.database import DbDependency
from app.sqlmodels import Country, Network

router = APIRouter(prefix="/country", tags=["Country"])


class CountryBase(BaseModel):
    name: str


class CountryFull(CountryBase):
    code: str


@router.get(
    "/",
    summary="List countries.",
    response_model=list[CountryFull]
)
async def get_countries(
    db: DbDependency, offset: int = 0, limit: int = 100
):
    countries = db.exec(
        select(Country).
        where(Country.deleted == False).
        limit(limit).
        offset(offset)
    ).all()
    return countries


@router.get(
    "/{code}",
    summary="Country details.",
    response_model=CountryFull
)
async def get_country(db: DbDependency, code: str):
    return get_country_by_code(db, code)


@router.post(
    "/", summary="Create country.", response_model=CountryFull
)
async def create_country(
    db: DbDependency, body: CountryFull
):
    if country_exists(db, body.code):
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail=f"Country {body.code} already exists.")

    body.code = body.code.upper()
    try:
        new_country = Country.model_validate(body)
        db.add(new_country)
        db.commit()
        db.refresh(new_country)
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Failed to create country: {e.args[0]}")
    return new_country


@router.put(
    "/{code}",
    summary="Update country.",
    response_model=CountryFull
)
async def update_country(
    db: DbDependency, code: str, body: CountryBase
):
    current_country = get_country_by_code(db, code)
    try:
        revised_country = body.model_dump(exclude_unset=True)
        current_country.sqlmodel_update(revised_country)
        db.add(current_country)
        db.commit()
        db.refresh(current_country)
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Failed to update country: {e.args[0]}")
    return current_country


@router.delete("/{code}", summary="Delete country.")
async def delete_country(code: str, db: DbDependency):
    if country_used(db, code):
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail=f"Country {code} is in use and cannot be deleted.")
    country = get_country_by_code(db, code)
    country.deleted = True
    db.add(country)
    db.commit()
    return {"ok": True}


@router.put(
    "/undelete/{name}",
    summary="Undelete country.",
    response_model=CountryFull
)
async def undelete_organisation(db: DbDependency, name: str):
    country = get_country_by_code(db, name, True)
    country.deleted = False
    db.add(country)
    db.commit()
    db.refresh(country)
    return country


def get_country_by_code(db: Session, code: str, deleted: bool = False):
    code = code.upper()
    country = db.exec(
        select(Country).
        where(Country.code == code).
        where(Country.deleted == deleted)
    ).first()
    if not country:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"No country found with code {code}.")
    return country


def country_exists(db: Session, code: str):
    code = code.upper()
    country = db.exec(
        select(Country).
        where(Country.code == code)
    ).first()
    if country and country.deleted:
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail=f"Country {code} already exists but is deleted.")
    elif country and not country.deleted:
        return True
    else:
        return False


def country_used(db: Session, code: str):
    code = code.upper()
    networks = db.exec(
        select(Network).
        where(Network.country_code == code).
        where(Network.deleted == False)
    ).first()
    return True if networks else False
