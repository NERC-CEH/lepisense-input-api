from fastapi import APIRouter, HTTPException, status
from pydantic import BaseModel
from sqlmodel import Session, select

from app.database import DbDependency
from app.sqlmodels import Network, Deployment
from app.api.routes.organisation import organisation_exists
from app.api.routes.country import country_exists

router = APIRouter(prefix="/network", tags=["Network"])


class NetworkBase(BaseModel):
    name: str
    organisation_name: str
    country_code: str


class NetworkFull(NetworkBase):
    id: int


@router.get(
    "/",
    summary="List networks.",
    response_model=list[NetworkFull]
)
async def get_networks(
    db: DbDependency,
    organisation_name: str = None,
    country_code: str = None,
    offset: int = 0,
    limit: int = 100
):
    sql = (select(Network).
           where(Network.deleted == False).
           limit(limit).
           offset(offset))
    if organisation_name:
        sql = sql.where(Network.organisation_name == organisation_name)
    if country_code:
        sql = sql.where(Network.country_code == country_code)

    networks = db.exec(sql).all()
    return networks


@router.get(
    "/{id}",
    summary="Network details.",
    response_model=NetworkFull
)
async def get_network(db: DbDependency, id: int):
    return get_network_by_id(db, id)


@router.post(
    "/", summary="Create network.", response_model=NetworkFull
)
async def create_network(db: DbDependency, body: NetworkBase):
    # Check foreign key validity
    if not organisation_exists(db, body.organisation_name):
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Organisation {body.organisation_name} not found."
        )
    if not country_exists(db, body.country_code):
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Country {body.country_code} not found."
        )
    # Maintain unique network names for an organisation and country
    if network_name_exists(
            db, body.organisation_name, body.country_code, body.name):
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail=(f"Network {body.name} already exists for "
                    f"{body.organisation_name} in {body.country_code}."))

    try:
        body.organisation_name = body.organisation_name.upper()
        body.country_code = body.country_code.upper()
        new_network = Network.model_validate(body)
        db.add(new_network)
        db.commit()
        db.refresh(new_network)
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Failed to create network: {e.args[0]}")
    return new_network


@router.put(
    "/{id}",
    summary="Update network.",
    response_model=NetworkFull
)
async def update_network(
    db: DbDependency, id: int, body: NetworkBase
):
    # Check foreign key validity
    if not organisation_exists(db, body.organisation_name):
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Organisation {body.organisation_name} not found."
        )
    if not country_exists(db, body.country_code):
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Country {body.country_code} not found."
        )

    current_network = get_network_by_id(db, id)
    # Maintain unique network names for an organisation and country
    if (current_network.organisation_name != body.organisation_name or
        current_network.country_code != body.country_code or
            current_network.name != body.name):
        if network_name_exists(
            db, body.organisation_name, body.country_code, body.name
        ):
            raise HTTPException(
                status_code=status.HTTP_409_CONFLICT,
                detail=(f"Network {body.name} already exists for "
                        f"{body.organisation_name} in {body.country_code}."))

    try:
        body.organisation_name = body.organisation_name.upper()
        body.country_code = body.country_code.upper()
        revised_network = body.model_dump(exclude_unset=True)
        current_network.sqlmodel_update(revised_network)
        db.add(current_network)
        db.commit()
        db.refresh(current_network)
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Failed to update network: {e.args[0]}")
    return current_network


@router.delete("/{id}", summary="Delete network.")
async def delete_network(db: DbDependency, id: int):
    if network_used(db, id):
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail=f"Network {id} is in use and cannot be deleted.")
    network = get_network_by_id(db, id)
    network.deleted = True
    db.add(network)
    db.commit()
    return {"ok": True}


@router.put(
    "/undelete/{id}",
    summary="Undelete network.",
    response_model=NetworkFull
)
async def undelete_network(db: DbDependency, name: str):
    network = get_network_by_id(db, name, True)
    network.deleted = False
    db.add(network)
    db.commit()
    db.refresh(network)
    return network


def get_network_by_id(db: Session, id: int, deleted: bool = False):
    network = db.exec(
        select(Network).
        where(Network.id == id).
        where(Network.deleted == deleted)
    ).first()
    if not network:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"No network found with id {id}.")
    return network


def network_name_exists(
        db: Session, organisation_name: str, country_code: str, name: str):
    organisation_name = organisation_name.upper()
    country_code = country_code.upper()
    network = db.exec(
        select(Network).
        where(Network.name == name).
        where(Network.organisation_name == organisation_name).
        where(Network.country_code == country_code)
    ).first()
    if network and network.deleted:
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail=(f"Network {name} already exists for "
                    f"{organisation_name} in {country_code}"
                    " but is deleted."))
    elif network and not network.deleted:
        return True
    else:
        return False


def network_exists(db: Session, id: int):
    network = db.exec(
        select(Network).
        where(Network.id == id)
    ).first()
    if network and network.deleted:
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail=(f"Network id {id} already exists but is deleted."))
    elif network and not network.deleted:
        return True
    else:
        return False


def network_used(db: Session, id: int):
    deployments = db.exec(
        select(Deployment).
        where(Deployment.network_id == id).
        where(Deployment.deleted == False)
    ).first()
    return True if deployments else False
