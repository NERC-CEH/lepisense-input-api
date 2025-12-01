import bcrypt
from datetime import datetime, timedelta, timezone
from typing import Annotated, TypeAlias

from fastapi import APIRouter, Depends, HTTPException, status
from fastapi.security import OAuth2PasswordBearer, OAuth2PasswordRequestForm
from jose import JWTError, jwt
from sqlmodel import Session, select

from app.constants import Role
from app.database import DbDependency
from app.env import EnvDependency
from app.sqlmodels import Account


# Instantiate a router.
router = APIRouter()

# TODO: Add a refresh token
# See https://github.com/k4black/fastapi-jwt for a possible solution.


# Instantiate the security provider.
# Set /token as the path to log in.
oauth2_scheme = OAuth2PasswordBearer(tokenUrl="token")

authentication_exception = HTTPException(
    status_code=status.HTTP_401_UNAUTHORIZED,
    detail="Could not validate credentials.",
    headers={"WWW-Authenticate": "Bearer"}
)
authorization_exception = HTTPException(
    status_code=status.HTTP_403_FORBIDDEN,
    detail="You do not have enough permissions for this action.",
    headers={"WWW-Authenticate": "Bearer"}
)


def verify_password(plain_password: str, hashed_password: str | bytes):
    """Confirms a password matches its hashed version."""
    plain_password = plain_password.encode('utf-8')
    if type(hashed_password) is str:
        hashed_password = hashed_password.encode('utf-8')
    return bcrypt.checkpw(plain_password, hashed_password)


# Hash a password using bcrypt
def hash_password(password: str):
    """Creates a hashed password."""
    password_byte_enc = password.encode('utf-8')
    salt = bcrypt.gensalt()
    hashed_password = bcrypt.hashpw(password_byte_enc, salt)
    return hashed_password


def create_access_token(
        env, data: dict, expires_delta: timedelta | None = None
):
    """Creates an access token for an account."""
    to_encode = data.copy()
    if expires_delta:
        expire = datetime.now(timezone.utc) + expires_delta
    else:
        expire = datetime.now(timezone.utc) + timedelta(minutes=15)
    to_encode.update({"exp": expire})
    encoded_jwt = jwt.encode(to_encode, env.jwt_key,
                             algorithm=env.jwt_algorithm)
    return encoded_jwt


def authenticate_account(
        env: EnvDependency, db: Session, name: str, password: str
):
    """Confirms a name and password match."""
    if name == env.userone_name and password == env.userone_pass:
        return Account(name=name, role=Role.ROOT.value)

    account = db.exec(
        select(Account)
        .where(Account.name == name)
    ).one_or_none()
    if not account:
        return False
    if not verify_password(password, account.hash):
        return False
    return account


def get_current_account(
    env: EnvDependency,
    token:  Annotated[str, Depends(oauth2_scheme)],
    db: DbDependency
):
    """Confirms an access token is valid."""
    try:
        payload = jwt.decode(token, env.jwt_key,
                             algorithms=[env.jwt_algorithm])
        name: str = payload.get("sub")
        if name is None:
            raise authentication_exception
    except JWTError:
        raise authentication_exception

    if name == env.userone_name:
        # Userone account.
        return Account(name=name, role=Role.ROOT.value)

    account = db.exec(
        select(Account)
        .where(Account.name == name)
    ).one_or_none()

    if account is None or account.disabled or account.deleted:
        raise authentication_exception
    return account


def get_current_write_account(
    account:  Annotated[Account, Depends(get_current_account)]
):
    """Confirms an access token is valid for a write role."""
    if (
        not account.role == Role.WRITE.value and
        not account.role == Role.ADMIN.value and
        not account.role == Role.ROOT.value
    ):
        raise authorization_exception
    return account


def get_current_admin_account(
    account:  Annotated[Account, Depends(get_current_account)]
):
    """Confirms an access token is valid for an admin role."""
    if (
        not account.role == Role.ADMIN.value and
        not account.role == Role.ROOT.value
    ):
        raise authorization_exception
    return account


def get_current_root_account(
    account:  Annotated[Account, Depends(get_current_account)]
):
    """Confirms an access token is valid for a root role."""
    if (
        not account.role == Role.ROOT.value
    ):
        raise authorization_exception
    return account


# Create a type alias for brevity when defining an endpoint needing
# authentication.
Auth: TypeAlias = Annotated[Account, Depends(get_current_account)]
AdminDependency: TypeAlias = Annotated[Account, Depends(
    get_current_admin_account)]
RootDependency: TypeAlias = Annotated[Account, Depends(
    get_current_root_account)]


@router.post(
    "/token",
    tags=['Service'],
    summary="Login account.")
async def login(
    form_data: Annotated[OAuth2PasswordRequestForm, Depends()],
    db: DbDependency,
    env: EnvDependency
):
    """Submit a **name** and **password** to receive a token which will
    authenticate requests to protected endpoints.
    **grant_type** should be set to 'password' while **scope**, **client_id**,
    and **client_secret** are not required.

    To use the token, set the Authorization header to 'Bearer {token}' in
    your http request, where {token} is the token value returned by this
    endpoint.
    """
    # Automatic validation ensures name and password exist.
    account = authenticate_account(
        env, db, form_data.username, form_data.password
    )
    if not account:
        raise authentication_exception

    access_token_expires = timedelta(minutes=env.jwt_expires_minutes)
    access_token = create_access_token(
        env,
        data={"sub": account.name},
        expires_delta=access_token_expires
    )
    return {"access_token": access_token, "token_type": "bearer"}
