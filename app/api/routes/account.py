import logging
from typing import Optional

from fastapi import APIRouter, HTTPException, status
from pydantic import BaseModel
from sqlmodel import Session, select

from app.auth import hash_password, AdminDependency
from app.constants import Role
from app.database import DbDependency
from app.env import EnvDependency
from app.sqlmodels import Account
from app.api.routes.organisation import organisation_exists

logger = logging.getLogger()

# Must be an admin account to access these routes.
router = APIRouter(
    prefix="/account",
    tags=["Account"]
)


class AccountGet(BaseModel):
    name: str
    organisation_name: str | None = None
    email: str
    role: str
    disabled: bool


class AccountPost(BaseModel):
    name: str
    organisation_name: str | None = None
    email: str
    password: str
    role: str
    disabled: bool


class AccountPatch(BaseModel):
    organisation_name: str | None = None
    email: Optional[str] = None
    password: Optional[str] = None
    role: Optional[str] = None
    disabled: Optional[bool] = None


@router.get('/', summary="List accounts.", response_model=list[AccountGet])
async def get_accounts(
    db: DbDependency,
    account: AdminDependency,
    role: Optional[Role] = None,
    disabled: bool = False,
    deleted: bool = False,
    offset: int = 0,
    limit: int = 100
):
    """Get all accounts."""
    query = (
        select(Account).
        where(Account.disabled == disabled).
        where(Account.deleted == deleted).
        limit(limit).
        offset(offset).
        order_by(Account.name)
    )
    if role:
        query = query.where(Account.role == role)
    if account.role != Role.ROOT.value:
        query = query.where(Account.organisation_name ==
                            account.organisation_name)
    try:
        accounts = db.exec(query).all()
    except Exception as e:
        logger.error(e)
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Failed to retrieve accounts: {e}")

    return accounts


@router.get('/{name}', summary="Get account.", response_model=AccountGet)
async def get_account(
    db: DbDependency,
    account: AdminDependency,
    name: str
):
    """Get a single account."""
    if account.role != Role.ROOT.value:
        organisation = account.organisation_name
    else:
        organisation = None
    return get_account_by_name(db, name, organisation)


@router.post('', summary="Create account.", response_model=AccountGet)
async def create_account(
    db: DbDependency,
    account: AdminDependency,
    body: AccountPost
):
    """Add an account for every consumer of this service.

    * **name** should identify the consumer.
    * **organisation** foreign key to the consumer's organisation.
    * **email** should be a valid email address for contacting the consumer.
    * **password** should be set for the consumer.
    * **role** should be set to one of [read|write|admin|root].
    * **disabled** should be set True to disable an account.

    There is a built-in administrative account that can be configured in the 
    host environment.
    """
    if account_exists(db, body.name):
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail=f"Account {body.name} already exists.")

    if body.organisation is not None:
        body.organisation = body.organisation.upper()

    if account.role != Role.ROOT.value:
        # Non-root admins can only create accounts for their own organisation.
        if body.organisation != account.organisation_name:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=f"The organisation must be {account.organisation_name}."
            )
        # Non-root admins cannot create root accounts.
        if body.role == Role.ROOT.value:
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail="You are not authorised to create root accounts."
            )
    elif body.organisation and not organisation_exists(db, body.organisation):
        # Root admins can create accounts for any organisation or None.
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Organisation {body.organisation} does not exist.")

    try:
        body.name = body.name.lower()
        hash = hash_password(body.password)
        extra_data = {"hash": hash}
        new_account = Account.model_validate(body, update=extra_data)
        db.add(new_account)
        db.commit()
        db.refresh(new_account)
    except Exception as e:
        logger.error(e)
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Failed to create account: {e}")
    return new_account


@router.patch("/{name}",  summary="Update account.", response_model=AccountGet)
async def update_account(
    db: DbDependency,
    env: EnvDependency,
    account: AdminDependency,
    name: str,
    body: AccountPatch
):
    """Update account with the given name."""
    if body.organisation is not None:
        body.organisation = body.organisation.upper()

    if account.role != Role.ROOT.value:
        # Non-root admins can only update accounts for their own organisation.
        if body.organisation != account.organisation_name:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=f"The organisation must be {account.organisation_name}."
            )
        # Non-root admins cannot create root accounts.
        if body.role == Role.ROOT.value:
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail="You are not authorised to create root accounts."
            )
    elif body.organisation and not organisation_exists(db, body.organisation):
        # Root admins can create accounts for any organisation or None.
        # TODO If org is None, role must be root.
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Organisation {body.organisation} does not exist.")

    current_account = get_account_by_name(db, name, body.organisation)
    if name == env.initial_account_name:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail=f"Account {env.initial_account_name} cannot be modified."
        )

    try:
        revised_account = body.model_dump(exclude_unset=True)
        extra_data = {}
        if body.password:
            hash = hash_password(body.password)
            extra_data = {"hash": hash}

        current_account.sqlmodel_update(revised_account, update=extra_data)
        db.add(current_account)
        db.commit()
        db.refresh(current_account)
    except Exception as e:
        logger.error(e)
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Failed to update account: {e}")
    return current_account


@router.delete("/{name}", summary="Delete account.")
async def delete_account(
    db: DbDependency,
    env: EnvDependency,
    account: AdminDependency,
    name: str
):
    """Delete account with the given name."""
    if account.role != Role.ROOT.value:
        organisation = account.organisation_name
    else:
        organisation = None
    account = get_account_by_name(db, name, organisation)

    if account_used(db, name):
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail=f"Account {name} is in use and cannot be deleted.")

    if name == env.initial_account_name:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail=f"Account {env.initial_account_name} cannot be deleted."
        )

    try:
        account.deleted = True
        db.add(account)
        db.commit()
    except Exception as e:
        logger.error(e)
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Failed to delete account: {e}")
    return {"ok": True}


@router.put(
    "/undelete/{name}",
    summary="Undelete account.",
    response_model=AccountGet
)
async def undelete_account(
    db: DbDependency,
    account: AdminDependency,
    name: str
):
    """Undelete account with the given name."""
    if account.role != Role.ROOT.value:
        organisation = account.organisation_name
    else:
        organisation = None

    account = get_account_by_name(db, name, organisation, True)
    try:
        account.deleted = False
        db.add(account)
        db.commit()
        db.refresh(account)
    except Exception as e:
        logger.error(e)
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Failed to undelete account: {e}")
    return account


def get_account_by_name(
        db: Session,
        name: str,
        organisation: str = None,
        deleted: bool = False
):
    name = name.lower()
    query = (
        select(Account).
        where(Account.name == name).
        where(Account.deleted == deleted)
    )
    if organisation:
        query = query.where(Account.organisation_name == organisation)

    try:
        account = db.exec(query).first()
    except Exception as e:
        logger.error(e)
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Failed to retrieve account: {e}")

    if not account:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"No account found with name {name}.")
    return account


def account_exists(db: Session, name: str):
    name = name.lower()
    account = db.exec(
        select(Account).
        where(Account.name == name)
    ).first()
    if account and account.deleted:
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail=f"Account {name} already exists but is deleted.")
    elif account and not account.deleted:
        return True
    else:
        return False


def account_used(db: Session, name: str):
    name = name.lower()
    # User is not a foreign key in any tables yet.
    return False
