from fastapi import FastAPI, Form, File, UploadFile, Request, Body, Query, HTTPException
from fastapi.responses import HTMLResponse, Response, JSONResponse
from fastapi.staticfiles import StaticFiles
from fastapi.middleware.cors import CORSMiddleware

from typing import List
from pydantic import BaseModel, Field
from datetime import datetime
from time import perf_counter
import csv
import json
import asyncio
from io import BytesIO
import zipfile
from botocore.exceptions import NoCredentialsError, PartialCredentialsError, ClientError
import aioboto3


tags_metadata = [
    {
        "name": "Data",
        "description": "Operations for data management in the server, uploading and downloading."
    },
    {
        "name": "Deployments",
        "description": "Operations with deployments."
    },
    {
        "name": "Other",
        "description": "Other operations."
    }
]

app = FastAPI(
    title="AMI Data Management API",
    version="1.0.1",
    contact={
        "name": "AMI system team at UKCEH",
        "url": "https://www.ceh.ac.uk/solutions/equipment/automated-monitoring-insects-trap",
        "email": "ami-system@ceh.ac.uk",
    },
    license_info={
        "name": "Apache 2.0",
        "identifier": "MIT",
    },
    openapi_tags=tags_metadata
)

# Set up CORS middleware
origins = [
    "http://localhost",
    "http://localhost:8080",
    "http://127.0.0.1",
    "http://127.0.0.1:8080",
    "https://connect-apps.ceh.ac.uk/ami-data-upload/"
]
app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)
# Mount the static directory to serve static files
app.mount("/static", StaticFiles(directory="static"), name="static")


# Load AWS credentials and S3 bucket name from config file
with open('credentials.json') as config_file:
    aws_credentials = json.load(config_file)

AWS_ACCESS_KEY_ID = aws_credentials['AWS_ACCESS_KEY_ID']
AWS_SECRET_ACCESS_KEY = aws_credentials['AWS_SECRET_ACCESS_KEY']
AWS_REGION = aws_credentials['AWS_REGION']
AWS_URL_ENDPOINT = aws_credentials['AWS_URL_ENDPOINT']

session = aioboto3.Session()


def load_deployments_info():
    deployments = []
    with open('deployments_info.csv', newline='') as csvfile:
        reader = csv.DictReader(csvfile)
        for row in reader:
            deployments.append(row)
    return deployments


deployments_info = load_deployments_info()
valid_countries_names = {d['country'] for d in deployments_info if d['status'] == 'active'}
valid_countries_location_names = {f"{d['country']} - {d['location_name']}" for d in deployments_info
                                  if d['status'] == 'active'}
valid_data_types = {"motion_images", "snapshot_images", "audible_recordings", "ultrasound_recordings"}


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
    data_type: str = Field(default="motion_images,snapshot_images,audible_recordings,ultrasound_recordings")
    s3_key: str = Field(default="country_code/deployment_id/data_type")
    status: str = Field(default="inactive")


@app.get("/", response_class=HTMLResponse)
async def main():
    with open("templates/upload.html") as f:
        html_content = f.read()
    return HTMLResponse(content=html_content)


@app.get("/get-deployments/", tags=["Deployments"])
async def get_deployments():
    return JSONResponse(content=deployments_info)


@app.post("/create-deployment/", tags=["Deployments"])
async def create_deployment(deployment: Deployment):
    try:
        # Append the new deployment to the CSV file
        with open('deployments_info.csv', 'a', newline='') as csvfile:
            fieldnames = ['country', 'country_code', 'location_name', "lat", "lon", "location_id", 'camera_id',
                          "system_id", 'hardware_id', 'deployment_id', 'data_type', "s3_key", 'status']
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            writer.writerow(deployment.dict())

        # Reload deployments info
        global deployments_info
        deployments_info = load_deployments_info()

        return JSONResponse(status_code=201, content={"message": "Deployment created successfully"})
    except Exception as e:
        return JSONResponse(status_code=500, content={"message": str(e)})


@app.put("/update-deployment/", tags=["Deployments"])
async def update_deployment(deployment: Deployment):
    try:
        # Load existing deployments
        deployments = load_deployments_info()

        # Find the deployment to update
        updated = False
        for i, existing_deployment in enumerate(deployments):
            if existing_deployment['deployment_id'] == deployment.deployment_id:
                deployments[i] = deployment.dict()
                updated = True
                break

        if not updated:
            return JSONResponse(status_code=404, content={"message": "Deployment not found"})

        # Write the updated deployments back to the CSV file
        with open('deployments_info.csv', 'w', newline='') as csvfile:
            fieldnames = ['country', 'country_code', 'location_name', "lat", "lon", "location_id", 'camera_id',
                          "system_id", 'hardware_id', 'deployment_id', 'data_type', "s3_key", 'status']
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            writer.writeheader()
            for dep in deployments:
                writer.writerow(dep)

        # Reload deployments info
        global deployments_info
        deployments_info = load_deployments_info()

        return JSONResponse(status_code=200, content={"message": "Deployment updated successfully"})
    except Exception as e:
        return JSONResponse(status_code=500, content={"message": str(e)})


@app.get("/list-data/", tags=["Other"])
async def list_data(
        country_location_name: str = Query("", enum=sorted(list(valid_countries_location_names)), description="Country and location names."),
        data_type: str = Query("", enum=list(valid_data_types), description="")
):
    country, location_name = country_location_name.split(" - ")
    country_code = [d['country_code'] for d in deployments_info if d['country'] == country][0]
    s3_bucket_name = country_code.lower()
    deployment_id = [d['deployment_id'] for d in deployments_info if d['country'] == country
                     and d['location_name'] == location_name][0]
    prefix = deployment_id + "/" + data_type
    files = []

    async with session.client('s3',
                              aws_access_key_id=AWS_ACCESS_KEY_ID,
                              aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
                              region_name=AWS_REGION,
                              endpoint_url=AWS_URL_ENDPOINT) as s3:
        try:
            paginator = s3.get_paginator('list_objects_v2')
            operation_parameters = {'Bucket': s3_bucket_name, 'Prefix': prefix}
            async for page in paginator.paginate(**operation_parameters):
                for obj in page.get('Contents', []):
                    files.append(obj['Key'])
            return JSONResponse(status_code=200, content={"files": files})
        except NoCredentialsError:
            raise HTTPException(status_code=403, detail="Credentials not available")
        except PartialCredentialsError:
            raise HTTPException(status_code=400, detail="Incomplete credentials")
        except ClientError:
            raise HTTPException(status_code=404, detail="The AWS Access Key Id does not exist in our records")
        except Exception as e:
            print('Failed pushing log file back to server')
            raise HTTPException(status_code=500, detail=str(e))
        except Exception as e:
            return JSONResponse(status_code=500, content={"message": str(e)})


@app.get("/get-logs/", tags=["Other"])
async def get_logs(country_name: str = Query("", enum=sorted(list(valid_countries_names)),
                                             description="Country names.")):
    country_code = [d['country_code'] for d in deployments_info if d['country'] == country_name][0]
    s3_bucket_name = country_code.lower()
    log_key = "logs/upload_summary.log"
    file_content = await download_file(s3_bucket_name, log_key)
    return Response(content=file_content, media_type="application/json5+text; charset=utf-8")


@app.post("/create-bucket/", tags=["Other"])
async def create_bucket(bucket_name: str = Query("", description="Bucket are named based on countries "
                                                                 "Alpha-3 code, check this link: "
                                                                 "https://www.iban.com/country-codes. "
                                                                 "E.g. The United Kingdom would be gbr")):
    async with session.client('s3',
                              aws_access_key_id=AWS_ACCESS_KEY_ID,
                              aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
                              region_name=AWS_REGION,
                              endpoint_url=AWS_URL_ENDPOINT) as s3:
        try:
            s3.create_bucket(Bucket=bucket_name)
            return JSONResponse(status_code=200, content={"message": f"Bucket '{bucket_name}' created successfully"})
        except s3.exceptions.BucketAlreadyExists:
            return JSONResponse(status_code=400, content={"message": "Bucket already exists"})
        except s3.exceptions.BucketAlreadyOwnedByYou:
            return JSONResponse(status_code=400, content={"message": "Bucket already owned by you"})
        except Exception as e:
            return JSONResponse(status_code=500, content={"message": str(e)})


@app.post("/upload/", tags=["Data"])
async def upload_zip(
        request: Request,
        name: str = Form(...),
        country: str = Form(...),
        deployment: str = Form(...),
        data_type: str = Form(...),
        file: UploadFile = File(...)
):
    start_time = perf_counter()
    s3_bucket_name = country.lower()
    key = deployment + "/" + data_type
    uploaded_files = []
    if not file.filename.endswith(".zip"):
        raise HTTPException(status_code=400, detail="File is not a zip file")

    try:
        # Read the zip file
        content = await file.read()
        zipfile_obj = zipfile.ZipFile(BytesIO(content))

        async with session.client('s3',
                                  aws_access_key_id=AWS_ACCESS_KEY_ID,
                                  aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
                                  region_name=AWS_REGION,
                                  endpoint_url=AWS_URL_ENDPOINT) as s3:
            # Upload each file in the zip to S3
            for file_info in zipfile_obj.infolist():
                # TODO: Check data type
                # Skip directories
                if file_info.is_dir():
                    continue

                file_data = zipfile_obj.read(file_info.filename)
                filename = file_info.filename.split('/')[-1]
                s3_key = key + "/" + filename

                # Create an upload task
                await asyncio.gather(*[s3.put_object(Bucket=s3_bucket_name, Key=s3_key, Body=file_data)],
                                     return_exceptions=True)

                uploaded_files.append(filename)
    except zipfile.BadZipFile:
        raise HTTPException(status_code=400, detail="Bad zip file")
    except Exception as e:
        print("Error:", e)
        raise HTTPException(status_code=500, detail=str(e))

    # Download log file from S3
    log_key = "logs/upload_summary.log"
    file_content = await download_file(s3_bucket_name, log_key)
    if not file_content:
        for filename in uploaded_files:
            # Append new log entry to the log file following CLF format
            ident = "-"
            authuser = "-"
            date_str = datetime.now().strftime("%d/%b/%Y:%H:%M:%S %z")
            request_line = f'POST /upload/ HTTP/1.1'
            status = 200
            bytes_sent = '-'  # You can replace this with the actual byte size of the response if needed
            file_content = file_content + (f'{request.client.host} {ident} {authuser} [{date_str}] "{request_line}" '
                                           f'{status} {bytes_sent} Name="{name}" Country="{s3_bucket_name}" '
                                           f'Deployment="{deployment}" DataType="{data_type}" '
                                           f'FileName="{filename}"\n').encode()
        # Upload updated file to S3
        await upload_file(s3_bucket_name, log_key, file_content)

    end_time = perf_counter()
    print(f"{end_time - start_time} seconds.")

    return JSONResponse(status_code=200, content={"message": "All files uploaded and verified successfully"})


async def download_file(bucket_name, file_key):
    async with session.client('s3',
                              aws_access_key_id=AWS_ACCESS_KEY_ID,
                              aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
                              region_name=AWS_REGION,
                              endpoint_url=AWS_URL_ENDPOINT) as s3:
        try:
            # Check if the file exists
            await s3.head_object(Bucket=bucket_name, Key=file_key)
            # If it exists, download the file content
            response = await s3.get_object(Bucket=bucket_name, Key=file_key)
            content = await response['Body'].read()
            return content
        except s3.exceptions.ClientError as e:
            error_code = e.response['Error']['Code']
            if error_code == '404':
                print(f"File {file_key} does not exist. Creating with initial content...")
                initial_content = b""
                await s3.put_object(Bucket=bucket_name, Key=file_key, Body=initial_content)
                return initial_content
            else:
                print(f"Error downloading file: {e}")
                return None
        except Exception as e:
            print(f"Error downloading file: {e}")
            return None


async def upload_file(bucket_name, file_key, content):
    async with session.client('s3',
                              aws_access_key_id=AWS_ACCESS_KEY_ID,
                              aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
                              region_name=AWS_REGION,
                              endpoint_url=AWS_URL_ENDPOINT) as s3:
        try:
            # Upload updated file to S3
            await s3.put_object(Bucket=bucket_name, Key=file_key, Body=content)
            print(f"File {file_key} updated and uploaded successfully.")
        except Exception as e:
            print(f"Error uploading file: {e}")


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8080)