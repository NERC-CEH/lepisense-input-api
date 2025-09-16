import asyncio
import logging
import mimetypes

from datetime import date
from fastapi import APIRouter, HTTPException, status, UploadFile
from fastapi.responses import Response
from sqlmodel import Session, select
from typing import List

from app.aws import S3Dependency
from app.database import DbDependency
from app.env import EnvDependency
from app.sqlmodels import Organisation, Network, Deployment, Device
from app.api.routes.organisation import organisation_exists
from app.api.routes.country import country_exists
from app.api.routes.network import network_name_exists, get_network_by_name
from app.api.routes.deployment import deployment_name_exists
from app.api.routes.devicetype import devicetype_exists
from app.api.routes.device import get_device_by_id

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/file", tags=["File"])


@router.get("/", summary="List files.")
async def get_files(
    s3: S3Dependency,
    db: DbDependency,
    env: EnvDependency,
    organisation: str | None = None,
    country: str | None = None,
    network: str | None = None,
    deployment: str | None = None,
    devicetype: str | None = None,
    year: int | None = None,
    month: int | None = None,
    day: int | None = None,
):
    paginator = s3.get_paginator('list_objects_v2')
    bucket = 'lepisense-images-' + env.environment
    prefix = validate_prefix(
        db,
        organisation,
        country,
        network,
        deployment,
        devicetype,
        year,
        month,
        day
    )
    operation_parameters = {'Bucket': bucket, 'Prefix': prefix}
    files = []

    try:
        async for page in paginator.paginate(**operation_parameters):
            for obj in page.get('Contents', []):
                files.append(obj['Key'])
        return {"files": files}
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Failed to get files: {e.args[0]}")


@router.get("/count", summary="Count files.")
async def get_count(
    s3: S3Dependency,
    db: DbDependency,
    env: EnvDependency,
    organisation: str | None = None,
    country: str | None = None,
    network: str | None = None,
    deployment: str | None = None,
    devicetype: str | None = None,
    year: int | None = None,
    month: int | None = None,
    day: int | None = None,
):
    paginator = s3.get_paginator('list_objects_v2')
    bucket = 'lepisense-images-' + env.environment
    prefix = validate_prefix(
        db,
        organisation,
        country,
        network,
        deployment,
        devicetype,
        year,
        month,
        day
    )
    operation_parameters = {'Bucket': bucket, 'Prefix': prefix}
    count = 0

    try:
        async for page in paginator.paginate(**operation_parameters):
            count += page["KeyCount"]

        return {"count": count}
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Failed to count files: {e.args[0]}")


@router.get(
    "/presigned-url",
    summary="Get credentials to post file directly to S3 for device on date.")
async def generate_presigned_url(
    s3: S3Dependency,
    db: DbDependency,
    env: EnvDependency,
    id: str,
    date: date,
    filename: str
):
    bucket = 'lepisense-images-' + env.environment
    prefix = get_prefix(db, id, date)
    key = f"{prefix}/{filename}"

    try:
        # Generate a presigned URL for the S3
        presigned_url = await s3.generate_presigned_url(
            'put_object',
            Params={'Bucket': bucket, 'Key': key},
            ExpiresIn=3600)  # URL expires in 1 hour

        return presigned_url
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Failed to get presigned url: {e.args[0]}")


@router.get(
    "/{organisation}/{country}/{network}/{deployment}/{devicetype}/{year}/{month}/{day}/{filename}",
    summary="Get file",
    responses={
        200: {
            "content": {"image/jpeg": {}},
        }
    })
async def get_file(
    s3: S3Dependency,
    db: DbDependency,
    env: EnvDependency,
    organisation: str,
    country: str,
    network: str,
    deployment: str,
    devicetype: str,
    year: int,
    month: int,
    day: int,
    filename: str
):

    bucket = 'lepisense-images-' + env.environment
    prefix = validate_prefix(
        db,
        organisation,
        country,
        network,
        deployment,
        devicetype,
        year,
        month,
        day
    )
    key = f"{prefix}/{filename}"

    try:
        logger.debug(f"Requesting from S3 {key}")
        response = await s3.get_object(Bucket=bucket, Key=key)
        logger.debug(f"Response from S3: {response}")
        image = await response['Body'].read()
    except s3.exceptions.NoSuchKey:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="File not found.")
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Failed to get file: {e.args[0]}")

    # The Mangum adapter will base64 encode the binary content
    # for us and set isBase64Encoded to true in the response that is handed
    # to the API Gateway.
    return Response(
        content=image,
        status_code=200,
        media_type=response['ContentType'],
        headers={"Content-Type": response['ContentType']}
    )


@router.post("/", summary="Upload files.")
async def upload_files(
    db: DbDependency,
    env: EnvDependency,
    s3: S3Dependency,
    device_id: str,
    date: date,
    files: List[UploadFile]
):
    bucket = 'lepisense-images-' + env.environment
    prefix = get_prefix(db, device_id, date)

    try:
        tasks = [upload_file(s3, bucket, prefix, file) for file in files]
        await asyncio.gather(*tasks)
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Failed to get file: {e.args[0]}")

    logger.info(
        f"Device {id} uploaded {len(files)} to {prefix}.")
    return {"message": "All files uploaded successfully"}


async def upload_file(s3, bucket, prefix, file):
    # It is important to save the media type of the file in S3. If not,
    # when getting the object in future, the media type will be
    # application/octet-stream and, I suspect, base64 encoded.
    media_type = mimetypes.guess_type(file.filename)[0]
    if not media_type:
        media_type = "application/octet-stream"
    args = {'ContentType': media_type}

    try:
        await s3.upload_fileobj(
            file.file, bucket, f"{prefix}/{file.filename}", ExtraArgs=args)
    except Exception as e:
        logger.error(
            f"Error uploading {file.filename} to {bucket}/{prefix}. "
            f"Error: {e}")
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Error uploading {prefix}/{file.filename}: {e.args[0]}")


def validate_prefix(
        db: Session,
        organisation: str,
        country: str,
        network: str,
        deployment: str,
        devicetype: str,
        year: int,
        month: int,
        day: int):
    prefix = ""
    if organisation:
        if organisation_exists(db, organisation):
            prefix += f"{organisation.upper()}"
        else:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Organisation {organisation} does not exist.")
    else:
        return prefix

    if country:
        if country_exists(db, country):
            prefix += f"/{country.upper()}"
        else:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Country {country} does not exist.")
    else:
        return prefix

    if network:
        if network_name_exists(db, network, organisation, country):
            prefix += f"/{network}"
        else:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=(f"Network {network} does not exist for {organisation} "
                        f"in {country}."))
    else:
        return prefix

    if deployment:
        network_id = get_network_by_name(db, network, organisation, country).id
        if deployment_name_exists(db, deployment, network_id):
            prefix += f"/{deployment}"
        else:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=(f"Deployment {deployment} does not exist for "
                        f"{network}."))
    else:
        return prefix

    if devicetype:
        if devicetype_exists(db, devicetype):
            prefix += f"/{devicetype.lower()}"
        else:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"DeviceType {devicetype} does not exist.")
    else:
        return prefix

    if year:
        try:
            year = int(year)
            if year < 2000 or year > date.today().year:
                raise ValueError
        except ValueError:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=f"Year {year} is not a valid integer.")
        prefix += f"/{year}"
    else:
        return prefix

    if month:
        try:
            month = int(month)
            if month < 1 or month > 12:
                raise ValueError
        except ValueError:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=f"Month {month} is not a valid integer.")
        prefix += f"/{month}"
    else:
        return prefix

    if day:
        try:
            day = int(day)
            date(year, month, day)
        except ValueError:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=f"{year}-{month}-{day} is not a valid date.")
        prefix += f"/{day}"

    return prefix


def get_prefix(db: Session, id: str, date: date):

    device = get_device_by_id(db, id)
    if not device.current_deployment_id:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Device {id} is not on deployment.")

    # Foreign key constraints ensure that organisation and network
    # exist if the deployment exists.
    organisation, network, deployment, device = db.exec(
        select(Organisation, Network, Deployment, Device).
        select_from(Device).
        join(Deployment).
        join(Network).
        join(Organisation).
        where(Device.id == id)
    ).first()

    return (f"{organisation.name}/{network.country_code}/{network.name}/"
            f"{deployment.name}/{device.devicetype_name}/{date.year}/"
            f"{date.month}/{date.day}")
