from app.api.routes.organisation import router as organisation_router
from app.api.routes.country import router as country_router
from app.api.routes.network import router as network_router
from app.api.routes.devicetype import router as devicetype_router
from app.api.routes.deployment import router as deployment_router

from time import perf_counter
import boto3
from fastapi import Form, File, UploadFile, Query, APIRouter, Request
from fastapi.responses import JSONResponse, PlainTextResponse, RedirectResponse
from pydantic import BaseModel, Field
import aioboto3
import csv
from typing import Annotated, List
import os
import logging
from sqlmodel import SQLModel
import asyncio

logger = logging.getLogger(__name__)

session = aioboto3.Session()


def load_deployments_info():
    deployments = []
    with open('deployments_info.csv', newline='', encoding='utf-8-sig') as csvfile:
        reader = csv.DictReader(csvfile)
        for row in reader:
            deployments.append(row)
    return deployments


deployments_info = load_deployments_info()

valid_countries_names = {d['country']
                         for d in deployments_info if d['status'] == 'active'}
valid_countries_location_names = {f"{d['country']} - {d['location_name']}" for d in deployments_info
                                  if d['status'] == 'active'}
valid_data_types = {"snapshot_images", "audible_recordings",
                    "ultrasound_recordings", "motion_images"}  # DC add "motion images"


class UploadResponse(BaseModel):
    uploaded_files: List[str]


class Deployment(BaseModel):
    country: str
    country_code: str
    location_name: str
    lat: str
    lon: str
    location_id: str
    camera_id: str
    system_id: str
    hardware_id: str
    deployment_id: str
    status: str


class NewDeployment(BaseModel):
    country: str
    country_code: str
    location_name: str
    lat: str
    lon: str
    camera_id: str
    hardware_id: str
    status: str = Field(default="inactive")


# Instantiate a router.
router = APIRouter()
router.include_router(organisation_router)
router.include_router(country_router)
router.include_router(network_router)
router.include_router(devicetype_router)
router.include_router(deployment_router)


@router.get("/", include_in_schema=False)
async def main():
    return RedirectResponse(
        url="/" + os.getenv("Environment", "") + "/docs")


@router.get("/reset")
async def reset(request: Request):
    engine = request.app.state.engine
    SQLModel.metadata.drop_all(engine)
    return {"ok": True}


@router.get("/get-deployments/", tags=["Deployments"])
async def get_deployments():
    return JSONResponse(content=deployments_info)


def increment_id(data, current_id):
    id_list = []
    for obj in data:
        if obj[current_id] != '':
            id_list.append(obj[current_id])
    # Extract the str part of each ID
    id_type = id_list[0][:3]
    # Extract the numeric part of each ID and convert it to an integer
    numeric_ids = [int(id[3:]) for id in id_list]
    # Find the maximum numeric ID
    max_numeric_id = max(numeric_ids)+1
    # Format back to the original ID format with leading zeros
    highest_id = f"{id_type}{max_numeric_id:06}"
    return highest_id


@router.post("/create-deployment/", tags=["Deployments"])
async def create_deployment(new_deployment: NewDeployment):
    try:
        global deployments_info
        # Append the new deployment to the CSV file
        with open('deployments_info.csv', 'a', newline='') as csvfile:
            # TODO: check is location already exists
            location_id = increment_id(deployments_info, 'location_id')
            # TODO: check is camera id already exists
            system_id = increment_id(deployments_info, 'system_id')
            deployment_id = increment_id(deployments_info, 'deployment_id')
            deployment = Deployment(country=new_deployment.country,
                                    country_code=new_deployment.country_code,
                                    location_name=new_deployment.location_name,
                                    lat=new_deployment.lat,
                                    lon=new_deployment.lon,
                                    location_id=location_id,
                                    camera_id=new_deployment.camera_id,
                                    system_id=system_id,
                                    hardware_id=new_deployment.hardware_id,
                                    deployment_id=deployment_id,
                                    status=new_deployment.status)
            fieldnames = ['country', 'country_code', 'location_name', "lat", "lon", "location_id", 'camera_id',
                          "system_id", 'hardware_id', 'deployment_id', 'status']
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            writer.writerow(deployment.dict())

        # Reload deployments info
        deployments_info = load_deployments_info()

        return JSONResponse(status_code=201, content={"message": "Deployment created successfully"})
    except Exception as e:
        return JSONResponse(status_code=500, content={"message": str(e)})


@router.put("/update-deployment/", tags=["Deployments"])
async def update_deployment(deployment: Deployment):
    try:
        # Reload deployments info
        global deployments_info

        # Find the deployment to update
        for i, existing_deployment in enumerate(deployments_info):
            if existing_deployment['deployment_id'] == deployment.deployment_id:
                deployments_info[i] = deployment.dict()
                break
            if i == len(deployments_info)-1:
                return JSONResponse(status_code=404, content={"message": "Deployment not found"})

        # Write the updated deployments back to the CSV file
        with open('deployments_info.csv', 'w', newline='') as csvfile:
            fieldnames = ['country', 'country_code', 'location_name', "lat", "lon", "location_id", 'camera_id',
                          "system_id", 'hardware_id', 'deployment_id', 'status']
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            writer.writeheader()
            for dep in deployments_info:
                writer.writerow(dep)

        deployments_info = load_deployments_info()
        return JSONResponse(status_code=200, content={"message": "Deployment updated successfully"})
    except Exception as e:
        return JSONResponse(status_code=500, content={"message": str(e)})


@router.get("/list-data/", tags=["Other"])
async def list_data(
        country_location_name: str = Query(
            "",
            enum=sorted(list(valid_countries_location_names)),
            description="Country and location names."
        ),
        data_type: str = Query("", enum=list(valid_data_types), description="")
):
    country, location_name = country_location_name.split(" - ")
    country_code = [d['country_code']
                    for d in deployments_info if d['country'] == country][0]
    s3_bucket_name = 'lepisense-images-' + os.getenv("Environment", "")
    deployment_id = [d['deployment_id'] for d in deployments_info if d['country'] == country
                     and d['location_name'] == location_name][0]
    prefix = deployment_id + "/" + data_type
    files = []
    try:
        async with session.client('s3') as s3_client:
            paginator = s3_client.get_paginator('list_objects_v2')
            operation_parameters = {'Bucket': s3_bucket_name, 'Prefix': prefix}
            async for page in paginator.paginate(**operation_parameters):
                for obj in page.get('Contents', []):
                    files.append(obj['Key'])
            return JSONResponse(status_code=200, content={"files": files})
    except Exception as e:
        return JSONResponse(
            status_code=500, content={"message": str(e)})


@router.get("/count-data/", tags=["Other"])
async def count_data(
        country_location_name: str = Query("", enum=sorted(list(valid_countries_location_names)),
                                           description="Country and location names."),
        data_type: str = Query("", enum=list(valid_data_types), description="")
):
    country, location_name = country_location_name.split(" - ")
    country_code = [d['country_code']
                    for d in deployments_info if d['country'] == country][0]
    s3_bucket_name = 'lepisense-images-' + os.getenv("Environment", "")
    deployment_id = [d['deployment_id'] for d in deployments_info if d['country'] == country
                     and d['location_name'] == location_name][0]
    prefix = deployment_id + "/" + data_type
    try:
        async with session.client('s3') as s3_client:
            paginator = s3_client.get_paginator('list_objects_v2')
            operation_parameters = {'Bucket': s3_bucket_name, 'Prefix': prefix}

            count = 0
            async for page in paginator.paginate(**operation_parameters):
                count += page["KeyCount"]
            return JSONResponse(status_code=200, content={"count": count})
    except Exception as e:
        return JSONResponse(status_code=500, content={"message": str(e)})


@router.get("/logs/", tags=["Other"])
async def get_logs():
    try:
        with open('upload_logs.log', 'r') as log_file:
            log_content = log_file.read()
        return PlainTextResponse(log_content)
    except Exception as e:
        return JSONResponse(status_code=500, content=f"Error reading log file: {e}")


@router.post("/generate-presigned-url/", tags=["Data"])
async def generate_presigned_url(
    name: str = Form(...),
    country: str = Form(...),
    deployment: str = Form(...),
    data_type: str = Form(...),
    filename: str = Form(...),
    file_type: str = Form(...)
):
    bucket_name = country.lower()
    key = f"{deployment}/{data_type}/{filename}"

    s3 = boto3.client('s3')
    try:
        # Generate a presigned URL for the S3
        presigned_url = s3.generate_presigned_url('put_object',
                                                  Params={"Bucket": bucket_name,
                                                          "Key": key,
                                                          "ContentType": file_type},
                                                  ExpiresIn=3600)  # URL expires in 1 hour

        return JSONResponse(status_code=200, content=presigned_url)
    except Exception as e:
        return JSONResponse(status_code=500, content={"message": str(e)})


@router.post("/upload/", tags=["Data"])
async def upload(
        name: Annotated[str, Form(description="User name")],
        organisation: Annotated[int, Form(description="Organisation id.")],
        deployment: Annotated[int, Form(description="Deployment id.")],
        data_type: str = Form(...),
        files: List[UploadFile] = File(...)
):
    start_time = perf_counter()
    s3_bucket_name = 'lepisense-images-' + os.getenv("Environment", "")
    key = f"{deployment}/{data_type}"

    try:
        tasks = [upload_file(s3_bucket_name, key, file, name)
                 for file in files]
        await asyncio.gather(*tasks)
    except Exception as e:
        print("Error:", e)
        return JSONResponse(status_code=500, content={str(e)})

    end_time = perf_counter()
    print(f"{end_time - start_time} seconds.")

    logger.info(
        f"User {name} from {country} uploaded {len(files)} {data_type} to deployment {deployment}.")
    return JSONResponse(status_code=200, content={"message": "All files uploaded and verified successfully"})


async def upload_file(s3_bucket_name, key, file, name):
    async with session.client('s3') as s3_client:
        try:
            # Upload updated file to S3
            await s3_client.upload_fileobj(file.file, s3_bucket_name, f"{key}/{file.filename}")
            # print(f"File {key}/{file.filename} uploaded successfully.")
        except Exception as e:
            logger.error(
                f"Error from User {name} when uploading {file.filename} to {s3_bucket_name}/{key}. Error: {e}")
            return JSONResponse(status_code=500, content={"message": f"Error uploading {key}/{file.filename}: {e}"})


@router.post("/check-file-exist/", tags=["Data"])
async def check_file_exist(
    name: str = Form(...),
    country: str = Form(...),
    deployment: str = Form(...),
    data_type: str = Form(...),
    filename: str = Form(...)
):
    bucket_name = country.lower()
    key = f"{deployment}/{data_type}/{filename}"

    s3 = boto3.client('s3')

    try:
        s3.head_object(Bucket=bucket_name, Key=key)
        message = {"exists": True}  # File exists
        return JSONResponse(status_code=200, content=message)
    except s3.exceptions.ClientError as e:
        if e.response['Error']['Code'] == '404':
            message = {"exists": False}  # File doesn't exist
            return JSONResponse(status_code=200, content=message)
        return JSONResponse(status_code=500, content={"message": f"{e}"})
    except Exception as e:
        return JSONResponse(status_code=500, content={"message": f"{e}"})
