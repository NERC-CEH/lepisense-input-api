from fastapi import FastAPI, Form, File, UploadFile, HTTPException, Request, Body, Query
from fastapi.responses import HTMLResponse, JSONResponse, Response
from fastapi.staticfiles import StaticFiles
from fastapi.middleware.cors import CORSMiddleware
import boto3
from botocore.exceptions import NoCredentialsError, PartialCredentialsError, ClientError
import boto3.s3.transfer as s3transfer
from typing import List
import json
import os
import csv
from tempfile import NamedTemporaryFile
from pydantic import BaseModel, Field
from datetime import datetime


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
    endpoint_url=AWS_URL_ENDPOINT
)

workers = 20
transfer_config = s3transfer.TransferConfig(
    use_threads=True,
    max_concurrency=workers,
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
    prefix = f"{deployment_id}/{data_type}/"
    try:
        response = s3_client.list_objects_v2(Bucket=s3_bucket_name, Prefix=prefix)
        if 'Contents' not in response:
            return JSONResponse(status_code=404, content={"message": "No data found"})
        files = [content['Key'] for content in response['Contents']]
        return JSONResponse(status_code=200, content={"files": files})
    except NoCredentialsError:
        return JSONResponse(status_code=400, content={"message": "Credentials not available"})
    except PartialCredentialsError:
        return JSONResponse(status_code=400, content={"message": "Incomplete credentials"})
    except Exception as e:
        return JSONResponse(status_code=500, content={"message": str(e)})


@app.get("/get-logs/", tags=["Other"])
async def get_logs(
        country_name: str = Query("", enum=sorted(list(valid_countries_names)), description="Country names."),
):
    country_code = [d['country_code'] for d in deployments_info if d['country'] == country_name][0]
    s3_bucket_name = country_code.lower()
    try:
        file = s3_client.get_object(Bucket=s3_bucket_name, Key="logs/upload_summary.json")
        json_data = file['Body'].read().decode('utf-8')
        return Response(content=json_data, media_type="application/json")
    except s3_client.exceptions.NoSuchKey:
        raise HTTPException(status_code=404, detail="JSON not found")
    except NoCredentialsError:
        raise HTTPException(status_code=401, detail="Credentials not available")


def fast_upload(s3_bucket_name, files, key, uploaded_files):
    s3t = s3transfer.create_transfer_manager(s3_client, transfer_config)
    for file in files:
        try:
            dst = key + file.filename
            s3t.upload(
                file.file, s3_bucket_name, dst,
            )
            uploaded_files.append(file.filename)
        except Exception as e:
            return JSONResponse(status_code=400, content={"message": f"Error uploading {key}/{file.filename}: {e}"})

    s3t.shutdown()  # wait for all the upload tasks to finish
    return uploaded_files


@app.post("/upload/", tags=["Data"])
async def upload_file(
        request: Request,
        name: str = Form(...),
        country: str = Form(...),
        deployment: str = Form(...),
        data_type: str = Form(...),
        files: List[UploadFile] = File(...)
):
    s3_bucket_name = country.lower()
    # s3_bucket_name = "test-upload"
    uploaded_files = []
    key = deployment + "/" + data_type + "/"
    try:
        uploaded_files = fast_upload(s3_bucket_name, files, key, uploaded_files)
    except NoCredentialsError:
        return JSONResponse(status_code=400, content={"message": "Credentials not available"})
    except PartialCredentialsError:
        return JSONResponse(status_code=400, content={"message": "Incomplete credentials"})
    except Exception as e:
        return JSONResponse(status_code=500, content={"message": str(e)})

    # Create a temporary log file
    log_key = f"logs/upload_summary.log"
    tmp_log_name = None

    # Check if the log file exists in the bucket
    try:
        s3_client.head_object(Bucket=s3_bucket_name, Key=log_key)
        # If the log file exists, download it
        with NamedTemporaryFile(delete=False, mode='wb') as tmp_log:
            s3_client.download_fileobj(s3_bucket_name, log_key, tmp_log)
            tmp_log_name = tmp_log.name
    except ClientError as e:
        if e.response['Error']['Code'] == '404':
            # The log file does not exist, create a new one
            with NamedTemporaryFile(delete=False, mode='w') as tmp_log:
                tmp_log_name = tmp_log.name
        else:
            raise HTTPException(status_code=500, detail="Could not access S3 bucket")

    # Append new log entries to the temporary log file following CLF format
    host = request.client.host
    ident = "-"
    authuser = "-"
    with open(tmp_log_name, 'a') as tmp_log:
        for file_name in uploaded_files:
            date_str = datetime.now().strftime("%d/%b/%Y:%H:%M:%S %z")
            request_line = f'POST /upload/ HTTP/1.1'
            status = 200
            bytes_sent = '-'  # You can replace this with the actual byte size of the response if needed
            log_entry = (f'{host} {ident} {authuser} [{date_str}] "{request_line}" {status} {bytes_sent} Name="{name}" '
                         f'Country="{country}" Deployment="{deployment}" DataType="{data_type}" FileName="{file_name}"\n')
            tmp_log.write(log_entry)

    # Upload the log file to S3
    try:
        with open(tmp_log_name, 'rb') as f:
            s3_client.upload_fileobj(f, s3_bucket_name, log_key)
    except NoCredentialsError:
        raise HTTPException(status_code=403, detail="Credentials not available")
    except PartialCredentialsError:
        raise HTTPException(status_code=400, detail="Incomplete credentials")
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        # Clean up the temporary file
        os.remove(tmp_log_name)

    response = UploadResponse(uploaded_files=uploaded_files)
    return JSONResponse(content=response.dict())


@app.post("/create-bucket/", tags=["Other"])
async def create_bucket(bucket_name: str = Body(..., embed=True)):
    try:
        s3_client.create_bucket(Bucket=bucket_name, CreateBucketConfiguration={'LocationConstraint': 'eu-west-1'})
        return JSONResponse(status_code=200, content={"message": f"Bucket '{bucket_name}' created successfully"})
    except s3_client.exceptions.BucketAlreadyExists:
        return JSONResponse(status_code=400, content={"message": "Bucket already exists"})
    except s3_client.exceptions.BucketAlreadyOwnedByYou:
        return JSONResponse(status_code=400, content={"message": "Bucket already owned by you"})
    except NoCredentialsError:
        return JSONResponse(status_code=400, content={"message": "Credentials not available"})
    except PartialCredentialsError:
        return JSONResponse(status_code=400, content={"message": "Incomplete credentials"})
    except Exception as e:
        return JSONResponse(status_code=500, content={"message": str(e)})


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8080)
