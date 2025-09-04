from datetime import date
from sqlmodel import SQLModel, Field
from typing import Optional


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
    deleted: bool = Field(default=False)


class Country(SQLModel, table=True):
    code: str = Field(primary_key=True)
    name: str
    deleted: bool = Field(default=False)


class Network(SQLModel, table=True):
    id: Optional[int] = Field(primary_key=True, default=None)
    organisation_name: str = Field(foreign_key='organisation.name', index=True)
    country_code: str = Field(foreign_key='country.code', index=True)
    name: str
    deleted: bool = Field(default=False)


class Deployment(SQLModel, table=True):
    id: Optional[int] = Field(primary_key=True, default=None)
    network_id: int = Field(foreign_key='network.id', index=True)
    devicetype_name: str = Field(foreign_key='devicetype.name', index=True)
    name: str
    latitude: float
    longitude: float
    active: bool
    deleted: bool = Field(default=False)


class Device(SQLModel, table=True):
    id: Optional[int] = Field(primary_key=True, default=None)
    uid: int
    name: str
    devicetype_name: str = Field(foreign_key='devicetype.name', index=True)
    version: str
    current_deployment_id: int = Field(foreign_key='deployment.id', index=True)
    deleted: bool = Field(default=False)


class DeviceType(SQLModel, table=True):
    name: str = Field(primary_key=True, default=None)
    description: str
    deleted: bool = Field(default=False)


class DeploymentDevice(SQLModel, table=True):
    id: Optional[int] = Field(primary_key=True, default=None)
    device_id: int = Field(foreign_key='device.id', index=True)
    deployment_id: int = Field(foreign_key='deployment.id', index=True)
    start_date: date
    end_date: date
    deleted: bool = Field(default=False)
