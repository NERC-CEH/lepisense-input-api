import asyncio
import logging
import mimetypes

from datetime import date, timedelta
from fastapi import APIRouter, HTTPException, status, UploadFile
from fastapi.responses import Response
from sqlmodel import Session, select
from typing import List

from app.aws import S3Dependency
from app.database import DbDependency
from app.env import EnvDependency
from app.sqlmodels import (
    Organisation, Network, Deployment, Inference, Device, DeviceType
)
from app.api.routes.organisation import organisation_exists
from app.api.routes.country import country_exists
from app.api.routes.network import network_name_exists, get_network_by_name
from app.api.routes.deployment import deployment_name_exists
from app.api.routes.deploymentdevice import get_deployment_by_device_and_date
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
    metadata = get_metadata(db, device_id, date)
    prefix = get_prefix(metadata, date)

    try:
        tasks = [upload_file(s3, bucket, prefix, file) for file in files]
        await asyncio.gather(*tasks)
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Failed to upload files: {e}")

    deployment = metadata[2]
    deployment_id = deployment.id

    devicetype = metadata[4]
    night_session = devicetype.night_session
    filetime = files[0].filename.split(".")[0]
    # The file name is the form hhmmss.jpg.
    if night_session and filetime < "120000":
        # files from night session devices created before midday are
        # assigned to the previous day.
        session_date = date - timedelta(days=1)
    else:
        session_date = date

    create_inference(db, device_id, deployment_id, session_date)

    return {"message": "All files uploaded successfully"}


def get_metadata(db: Session, device_id: str, date: date):

    device, devicetype = db.exec(
        select(Device, DeviceType).
        select_from(Device).
        join(DeviceType).
        where(Device.id == device_id).
        where(Device.deleted == False)  # noqa
    ).first()
    if not device:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"No device found with id {device_id}.")

    # The device might not be deployed at the time the file is uploaded
    # if it is a manual rather than automatic submission. Therefore, we
    # cannot use device.current_deployment_id.
    deployment = get_deployment_by_device_and_date(db, device_id, date)

    # Foreign key constraints ensure that organisation and network
    # exist if the deployment exists.
    organisation, network = db.exec(
        select(Organisation, Network).
        select_from(Deployment).
        join(Network).
        join(Organisation).
        where(Deployment.id == deployment.id)
    ).first()

    return (organisation, network, deployment, device, devicetype)


def get_prefix(metadata: tuple, date: date):

    (organisation, network, deployment, device, devicetype) = metadata
    return (f"{organisation.name}/{network.country_code}/{network.name}/"
            f"{deployment.name}/{device.devicetype_name}/{date.year}/"
            f"{date.month:02d}/{date.day:02d}")


async def upload_file(s3, bucket, prefix, file):
    # It is important to save the media type of the file in S3. If not,
    # when getting the object in future, the media type will be
    # application/octet-stream and, I suspect, base64 encoded.
    logger.info(f"Uploading {file.filename} to {bucket}/{prefix}")
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
    logger.info(f"Uploaded {file.filename}")


def create_inference(
        db: Session, device_id: int, deployment_id: int, session_date: date):
    """
    Create a record for an inference job.
    """

    # Check if inference already exists for the device and date
    inference = db.exec(
        select(Inference).
        where(Inference.device_id == device_id).
        where(Inference.session_date == session_date).
        where(Inference.deleted == False)  # noqa
    ).first()

    if inference:
        if inference.completed:
            # Update to mark as incomplete.
            inference.completed = False
        else:
            # Do nothing.
            pass
    else:
        # Create new inference.
        inference = Inference(
            device_id=device_id,
            deployment_id=deployment_id,
            session_date=session_date,
            completed=False
        )

    # Save to database.
    db.add(inference)
    db.commit()
    db.refresh(inference)


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
