from fastapi import FastAPI, Form, File, UploadFile, Request, Body, Query, HTTPException
from fastapi.responses import HTMLResponse, Response, JSONResponse
from fastapi.staticfiles import StaticFiles
from fastapi.middleware.cors import CORSMiddleware

from typing import List
from pydantic import BaseModel, Field
from datetime import datetime
import os
import csv
import json
import asyncio
from io import BytesIO
import zipfile
from itertools import islice
import boto3
import boto3.s3.transfer as s3transfer
from botocore.config import Config
from botocore.exceptions import NoCredentialsError, PartialCredentialsError, ClientError
from tempfile import NamedTemporaryFile
from concurrent.futures import ThreadPoolExecutor, as_completed


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

# Initialize S3 client
s3_client = boto3.client(
    's3',
    aws_access_key_id=AWS_ACCESS_KEY_ID,
    aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
    region_name=AWS_REGION,
    endpoint_url=AWS_URL_ENDPOINT,
    config=Config(max_pool_connections=25)
)

transfer_config = s3transfer.TransferConfig(
    max_concurrency=20,
    use_threads=True
)


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


@app.get("/", response_class=HTMLResponse, tags=["Data"])
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
    try:
        files = s3_list_objects(s3_bucket_name, prefix)
        return JSONResponse(status_code=200, content={"files": files})
    except Exception as e:
        return JSONResponse(status_code=500, content={"message": str(e)})


@app.get("/get-logs/", tags=["Other"])
async def get_logs(country_name: str = Query("", enum=sorted(list(valid_countries_names)),
                                             description="Country names.")):
    country_code = [d['country_code'] for d in deployments_info if d['country'] == country_name][0]
    s3_bucket_name = country_code.lower()
    print(s3_bucket_name)
    log_key = "logs/upload_summary.log"
    log_file = download_logs_str(s3_bucket_name, log_key)
    return Response(content=log_file, media_type="application/json5+text; charset=utf-8")


@app.post("/create-bucket/", tags=["Other"])
async def create_bucket(bucket_name: str = Query("", description="Bucket are named based on countries "
                                                                 "Alpha-3 code, check this link: "
                                                                 "https://www.iban.com/country-codes. "
                                                                 "E.g. The United Kingdom would be gbr")):
    try:
        s3_client.create_bucket(Bucket=bucket_name, CreateBucketConfiguration={'LocationConstraint': 'eu-west-1'})
        return JSONResponse(status_code=200, content={"message": f"Bucket '{bucket_name}' created successfully"})
    except s3_client.exceptions.BucketAlreadyExists:
        return JSONResponse(status_code=400, content={"message": "Bucket already exists"})
    except s3_client.exceptions.BucketAlreadyOwnedByYou:
        return JSONResponse(status_code=400, content={"message": "Bucket already owned by you"})
    except Exception as e:
        return JSONResponse(status_code=500, content={"message": str(e)})


@app.post("/upload/", tags=["Data"])
async def upload(
        request: Request,
        name: str = Form(...),
        country: str = Form(...),
        deployment: str = Form(...),
        data_type: str = Form(...),
        files: List[UploadFile] = File(...)
):
    now = datetime.now()
    start_time = now.strftime("%H:%M:%S")

    s3_bucket_name = country.lower()
    key = deployment + "/" + data_type

    all_uploaded = upload_files(files, s3_bucket_name, key)
    if all_uploaded:
        # TODO: test files where uploaded correctly and add uploaded files to the log file.
        host = request.client.host
        write_logs(files, s3_bucket_name, host, name, deployment, data_type)

        now = datetime.now()
        end_time = now.strftime("%H:%M:%S")
        print()
        print("Start Time =", start_time)
        print("End Time =", end_time)

        print("All files uploaded and verified successfully")
        return JSONResponse(status_code=200,
                            content={"message": "All files uploaded and verified successfully"})
    else:
        return JSONResponse(status_code=500, content={"message": "Upload failed."})


@app.post("/upload-zip/", tags=["Data"])
async def upload_zip(
        request: Request,
        name: str = Form(...),
        country: str = Form(...),
        deployment: str = Form(...),
        data_type: str = Form(...),
        file: UploadFile = File(...)
):
    now = datetime.now()
    start_time = now.strftime("%H:%M:%S")

    s3_bucket_name = country.lower()
    key = deployment + "/" + data_type

    if not file.filename.endswith(".zip"):
        raise HTTPException(status_code=400, detail="File is not a zip file")

    try:
        # Read the zip file
        content = await file.read()
        zipfile_obj = zipfile.ZipFile(BytesIO(content))

        # List to hold the tasks
        tasks = []
        uploaded_files = []

        # Upload each file in the zip to S3
        for file_info in zipfile_obj.infolist():
            # Skip directories
            if file_info.is_dir():
                continue

            file_data = zipfile_obj.read(file_info.filename)
            filename = file_info.filename.split('/')[-1]
            s3_key = key + "/" + filename

            # Create an upload task
            task = asyncio.ensure_future(upload_to_s3(file_data, s3_bucket_name, s3_key))
            tasks.append(task)
            uploaded_files.append(filename)

        # Wait for all tasks to complete
        await asyncio.gather(*tasks, return_exceptions=True)

        host = request.client.host
        write_logs(uploaded_files, s3_bucket_name, host, name, deployment, data_type)

        now = datetime.now()
        end_time = now.strftime("%H:%M:%S")
        print()
        print("Start Time =", start_time)
        print("End Time =", end_time)

        return JSONResponse(status_code=200,
                            content={"message": "All files uploaded and verified successfully"})

    except zipfile.BadZipFile:
        raise HTTPException(status_code=400, detail="Bad zip file")
    except Exception as e:
        print("Error:", e)
        raise HTTPException(status_code=500, detail=str(e))


async def upload_to_s3(file_data, s3_bucket_name, s3_key):
    await s3_client.put_object(Bucket=s3_bucket_name, Key=s3_key, Body=file_data)


def upload_file_s3(s3t, s3_bucket_name, key, file):
    try:
        dst = key + "/" + file.filename
        s3t.upload(
            file.file, s3_bucket_name, dst,
        )
    except Exception as e:
        return JSONResponse(status_code=500, content={"message": f"Error uploading {key}/{file.filename}: {e}"})


def upload_chunk(s3t, chunk, s3_bucket_name, key):
    for file in chunk:
        upload_file_s3(s3t, s3_bucket_name, key, file)


def chunks(iterable, size):
    iterator = iter(iterable)
    for first in iterator:
        yield list(islice(iterator, size))


def upload_files(files, s3_bucket_name, key, chunk_size=100, max_workers=10):
    s3t = s3transfer.create_transfer_manager(s3_client, config=transfer_config)
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = [
            executor.submit(upload_chunk, s3t, chunk, s3_bucket_name, key)
            for chunk in chunks(files, chunk_size)
        ]
        for future in as_completed(futures):
            try:
                future.result() # This will re-raise any exception that was raised during the execution
            except Exception as e:
                return JSONResponse(status_code=500, content={"message": f"Error uploading: {e}"})
                return False
    s3t.shutdown()  # wait for all the upload tasks to finish
    return True


def download_logs_tmp_file(s3_bucket_name, log_key):
    try:
        s3_client.head_object(Bucket=s3_bucket_name, Key=log_key)
        # If the log file exists, download it
        with NamedTemporaryFile(delete=False, mode='wb') as tmp_log:
            s3_client.download_fileobj(s3_bucket_name, log_key, tmp_log)
            tmp_log_name = tmp_log.name
        return tmp_log_name
    except ClientError as e:
        if e.response['Error']['Code'] == '404':
            # The log file does not exist, create a new one
            with NamedTemporaryFile(delete=False, mode='w') as tmp_log:
                tmp_log_name = tmp_log.name
            return tmp_log_name
        else:
            raise HTTPException(status_code=500, detail="Could not access S3 bucket")


def download_logs_str(s3_bucket_name, log_key):
    try:
        file = s3_client.get_object(Bucket=s3_bucket_name, Key=log_key)
        log_file = file['Body'].read().decode('utf-8')
        return log_file
    except UnicodeDecodeError:
        file = s3_client.get_object(Bucket=s3_bucket_name, Key=log_key)
        log_file = file['Body'].read().decode('latin1')
        return log_file
    except NoCredentialsError:
        raise HTTPException(status_code=401, detail="Credentials not available")
    except s3_client.exceptions.NoSuchKey:
        raise HTTPException(status_code=404, detail="JSON not found")
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"{e}")


def write_logs(uploaded_files, s3_bucket_name, host, name, deployment, data_type):
    # Create a temporary log file
    log_key = "logs/upload_summary.log"
    tmp_log_name = download_logs_tmp_file(s3_bucket_name, log_key)

    # Append new log entries to the temporary log file following CLF format
    ident = "-"
    authuser = "-"
    with open(tmp_log_name, "a") as tmp_log:
        for file_name in uploaded_files:
            date_str = datetime.now().strftime("%d/%b/%Y:%H:%M:%S %z")
            request_line = f'POST /upload/ HTTP/1.1'
            status = 200
            bytes_sent = '-'  # You can replace this with the actual byte size of the response if needed
            log_entry = (f'{host} {ident} {authuser} [{date_str}] "{request_line}" {status} {bytes_sent} Name="{name}" '
                         f'Country="{s3_bucket_name}" Deployment="{deployment}" DataType="{data_type}" FileName="{file_name}"\n')
            tmp_log.write(log_entry)

    # Upload the log file to S3
    try:
        with open(tmp_log_name, 'rb') as log_file:
            s3_client.upload_fileobj(log_file, s3_bucket_name, log_key)
    except NoCredentialsError:
        raise HTTPException(status_code=403, detail="Credentials not available")
    except PartialCredentialsError:
        raise HTTPException(status_code=400, detail="Incomplete credentials")
    except Exception as e:
        print('Failed pushing log file back to server')
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        os.remove(tmp_log_name)


def s3_list_objects(s3_bucket_name, prefix):
    try:
        # Create a paginator helper for list_objects_v2
        paginator = s3_client.get_paginator('list_objects_v2')
        operation_parameters = {'Bucket': s3_bucket_name, 'Prefix': prefix}
        files = []
        page_iterator = paginator.paginate(**operation_parameters)
        # Work through the response pages, add to the running total
        for page in page_iterator:
            if 'Contents' in page:
                for obj in page['Contents']:
                    files.append(obj['Key'])
        return files
    except NoCredentialsError:
        raise HTTPException(status_code=403, detail="Credentials not available")
    except PartialCredentialsError:
        raise HTTPException(status_code=400, detail="Incomplete credentials")
    except ClientError:
        raise HTTPException(status_code=404, detail="The AWS Access Key Id does not exist in our records")
    except Exception as e:
        print('Failed pushing log file back to server')
        raise HTTPException(status_code=500, detail=str(e))


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8080)