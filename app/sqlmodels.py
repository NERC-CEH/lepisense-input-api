from datetime import date
from sqlalchemy import false
from sqlmodel import SQLModel, Column, Field, LargeBinary


# Create a naming convention.
SQLModel.metadata.naming_convention = {
    "ix": "ix_%(column_0_label)s",
    "uq": "uq_%(table_name)s_%(column_0_name)s",
    "ck": "ck_%(table_name)s_%(constraint_name)s",
    "fk": "fk_%(table_name)s_%(column_0_name)s_%(referred_table_name)s",
    "pk": "pk_%(table_name)s",
}


class Organisation(SQLModel, table=True):
    name: str = Field(primary_key=True)
    full_name: str
    deleted: bool = Field(
        default=False,
        sa_column_kwargs={'server_default': false()}
    )


class Country(SQLModel, table=True):
    code: str = Field(primary_key=True)
    name: str
    deleted: bool = Field(
        default=False,
        sa_column_kwargs={'server_default': false()}
    )


class Network(SQLModel, table=True):
    id: int | None = Field(primary_key=True, default=None)
    organisation_name: str = Field(foreign_key='organisation.name', index=True)
    country_code: str = Field(foreign_key='country.code', index=True)
    name: str
    deleted: bool = Field(
        default=False,
        sa_column_kwargs={'server_default': false()}
    )


class Deployment(SQLModel, table=True):
    id: int | None = Field(primary_key=True, default=None)
    network_id: int = Field(foreign_key='network.id', index=True)
    devicetype_name: str = Field(foreign_key='devicetype.name', index=True)
    name: str
    description: str | None
    latitude: float
    longitude: float
    active: bool = Field(
        default=False,
        sa_column_kwargs={'server_default': false()}
    )
    deleted: bool = Field(
        default=False,
        sa_column_kwargs={'server_default': false()}
    )


class Device(SQLModel, table=True):
    id: str = Field(primary_key=True)
    name: str | None
    devicetype_name: str = Field(foreign_key='devicetype.name', index=True)
    version: str
    current_deployment_id: int | None = Field(
        foreign_key='deployment.id', index=True)
    deleted: bool = Field(
        default=False,
        sa_column_kwargs={'server_default': false()}
    )


class DeviceType(SQLModel, table=True):
    name: str = Field(primary_key=True)
    description: str
    deleted: bool = Field(
        default=False,
        sa_column_kwargs={'server_default': false()}
    )


class DeploymentDevice(SQLModel, table=True):
    id: int | None = Field(primary_key=True, default=None)
    device_id: str = Field(foreign_key='device.id', index=True)
    deployment_id: int = Field(foreign_key='deployment.id', index=True)
    start_date: date
    end_date: date | None
    deleted: bool = Field(
        default=False,
        sa_column_kwargs={'server_default': false()}
    )


class Account(SQLModel, table=True):
    name: str = Field(primary_key=True)
    organisation_name: str | None = Field(
        foreign_key='organisation.name', index=True)
    email: str
    hash: bytes = Field(sa_column=Column(LargeBinary, nullable=False))
    role: str
    disabled: bool = Field(
        default=False,
        sa_column_kwargs={'server_default': false()}
    )
    deleted: bool = Field(
        default=False,
        sa_column_kwargs={'server_default': false()}
    )


class Inference(SQLModel, table=True):
    id: int | None = Field(primary_key=True, default=None)
    device_id: str = Field(foreign_key='device.id', index=True)
    date: date
    task_arn: str | None
    completed: bool = Field(
        default=False,
        sa_column_kwargs={'server_default': false()}
    )
    deleted: bool = Field(
        default=False,
        sa_column_kwargs={'server_default': false()}
    )
